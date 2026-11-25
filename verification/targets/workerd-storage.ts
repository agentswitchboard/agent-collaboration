// Runs a worker-build bundle under the pinned Miniflare and workerd, with real
// SQLite-backed Durable Objects persisted in a private temporary directory.
// Loopback only. Sign-in keys have a named service entry and a separate outbound trap.
// Rebuilt minimally from the loader
// deliverable A removed (ec7ba9dbf^:verification/targets/workerd-central.ts).
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Miniflare,
  Response,
  convertV4MiniflareOptions,
  type V4FetchHandler,
  type V4WorkerOptions,
} from "miniflare";
import { z } from "zod";
import {
  C2WitnessRecorder,
  c2PhaseSchema,
  c2HeldDiagnosticSchema,
  c2WitnessWrapperSource,
} from "../observations/central/c2-witness.ts";
import { ordinaryAcceptProbeSource } from "../observations/central/c2-ordinary-accept-probe.ts";

export interface WorkerdBundle {
  /** Directory holding worker-build's `index.js` and `index_bg.wasm`. */
  readonly bundleDirectory: string;
  /** The command that builds the bundle, named when it is missing. */
  readonly buildCommand: string;
  readonly compatibilityDate: string;
  /** Binding name to exported Durable Object class; each is SQLite-backed. */
  readonly durableObjects: Readonly<Record<string, string>>;
  readonly textBindings: Readonly<Record<string, string>>;
  readonly c2Witness?: C2WitnessRecorder;
  readonly c2OrdinaryAcceptProbe?: boolean;
}

const jwksMappingSchema = z
  .object({ fixtureUrl: z.url().optional(), publicTrapJwks: z.json().optional() })
  .strict()
  .superRefine((mapping, context) => {
    if (mapping.fixtureUrl === undefined) return;
    const fixture = new URL(mapping.fixtureUrl);
    if (
      fixture.protocol !== "http:" ||
      fixture.hostname !== "127.0.0.1" ||
      fixture.port === "" ||
      Number(fixture.port) === 0 ||
      fixture.username !== "" ||
      fixture.password !== "" ||
      fixture.hash !== "" ||
      fixture.href !== mapping.fixtureUrl
    )
      context.addIssue({
        code: "custom",
        message: "JWKS fixture must use an allocated 127.0.0.1 port",
      });
  });

export interface WorkerdSignInOptions {
  readonly linkBundle: WorkerdBundle;
  readonly jwks: z.infer<typeof jwksMappingSchema>;
}

interface JwksRequests {
  privateGets: number;
  unexpectedPrivate: number;
  publicRequests: number;
}

const signInKeysSource = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class SignInJwks extends WorkerEntrypoint {
  fetch(request) {
    return this.env.KEY_FIXTURE.fetch(new Request(request, { redirect: "manual" }));
  }
}
export default { fetch() { return new Response(null, { status: 404 }); } };
`;

function privateKeysFixture(fixtureUrl: string, requests: JwksRequests): V4FetchHandler {
  return async (request): Promise<Response> => {
    if (new URL(request.url).pathname !== "/api/auth/jwks") {
      requests.unexpectedPrivate++;
      return new Response(null, { status: 404 });
    }
    if (request.method !== "GET") {
      requests.unexpectedPrivate++;
      return new Response(null, { status: 405, headers: { allow: "GET" } });
    }
    requests.privateGets++;
    const response = await fetch(fixtureUrl, { method: "GET", redirect: "manual" });
    return new Response(response.body === null ? null : await response.arrayBuffer(), {
      status: response.status,
      headers: [...response.headers],
    });
  };
}

function signInClockService(readClock: (organization: string) => number): V4FetchHandler {
  return async (request: Parameters<V4FetchHandler>[0]): Promise<Response> => {
    const prefix = "https://signin-clock.test/clock/";
    const organization = request.url.startsWith(prefix) ? request.url.slice(prefix.length) : "";
    const parsed = z.uuidv7().safeParse(organization);
    if (request.method !== "GET" || !parsed.success)
      return new Response("invalid clock request", { status: 400 });
    const milliseconds = workerdLauncherTimeSchema.parse(readClock(parsed.data));
    return Response.json({ milliseconds });
  };
}

/** Configuration passed by the Rust fixture; contains no signer or assertion logic. */
export const workerdSignInPlanSchema = z
  .object({
    textBindings: z.record(z.string(), z.string()),
    jwksFixtureUrl: z.string().optional(),
    publicTrapJwks: z.json().optional(),
  })
  .strict();

// Match central_mail_time::MAX_ALARM_UNIX_MILLIS on both storage hosts.
export const workerdLauncherTimeSchema = z.number().int().safe().min(0).max(9_223_372_036_854);

export const workerdBridgeCommandSchema = z.union([
  z.object({ jwksRequests: z.literal(true) }).strict(),
  z.object({ setTimeMs: workerdLauncherTimeSchema }).strict(),
  z.object({ observe: z.uuidv7() }).strict(),
  z.object({ c2Evict: z.uuidv7() }).strict(),
  z
    .object({
      c2Witness: z
        .object({
          phase: c2PhaseSchema,
          holdDecidingDiagnostic: c2HeldDiagnosticSchema.optional(),
          waitForUpgrades: z.number().int().positive().optional(),
        })
        .strict(),
    })
    .strict(),
  z
    .object({
      c2SettleAlarms: z
        .object({
          phase: c2PhaseSchema,
          correlationIds: z.array(z.uuidv7()),
          expectRefused: z.uuidv7().optional(),
        })
        .strict()
        .refine(
          ({ correlationIds, expectRefused }) =>
            correlationIds.length > 0 || expectRefused !== undefined,
        ),
    })
    .strict(),
  z.object({ setExternalTimeMs: workerdLauncherTimeSchema }).strict(),
]);

// Row contents are owned and validated by Rust; the bridge relays opaque JSON.
export const workerdBridgeObservationSchema = z
  .object({ complete: z.literal(true) })
  .catchall(z.json());

export const workerdClockAcknowledgementSchema = z.literal(true);

const gatewaySource = `export default { async fetch(request, env) {
  const url = new URL(request.url);
  const binding = url.pathname.startsWith("/public/") ? env.CENTRAL_LINK
    : url.pathname.startsWith("/private/") ? env.PRIVATE_LINK : undefined;
  if (binding === undefined) return new Response("unknown verification transport", {status: 404});
  url.pathname = url.pathname.slice(url.pathname.indexOf("/", 1));
  if (request.method === "GET" && request.headers.get("upgrade")?.toLowerCase() === "websocket"
    && url.pathname.endsWith("/person-link")) {
    // Replay the request Sign-in builds at CENTRAL_LINK, without transport or client headers.
    const headers = new Headers({ connection: "Upgrade", upgrade: "websocket" });
    for (const name of ["authorization", "sec-websocket-key", "sec-websocket-version",
      "sec-websocket-protocol", "sec-websocket-extensions"]) {
      const value = request.headers.get(name);
      if (value !== null) headers.set(name, value);
    }
    return binding.fetch(new Request(new URL(url.pathname, "http://central.invalid"), {
      headers, redirect: "manual",
    }));
  }
  return binding.fetch(new Request(url, request));
} };`;

const repositoryRoot = new URL("../../", import.meta.url).pathname;
const centralCrate = join(repositoryRoot, "crates/collaboration-central-cloudflare");

/** The central Worker built with its `test-support` feature. */
export function centralTestBundle(): WorkerdBundle {
  return {
    bundleDirectory: join(centralCrate, "build-test-support"),
    buildCommand:
      "worker-build crates/collaboration-central-cloudflare --dev --no-opt --out-dir build-test-support -- --features test-support",
    compatibilityDate: "2026-08-21",
    durableObjects: {
      WORKSPACE_STORE: "WorkspaceStoreObject",
      STORAGE_CASES: "StorageCaseObject",
      DRIVER_SCENARIOS: "DriverScenarioObject",
    },
    textBindings: {},
  };
}

/** Separately compiled private-link entry, sharing the public central's namespace. */
export function centralLinkTestBundle(): WorkerdBundle {
  return {
    ...centralTestBundle(),
    bundleDirectory: join(centralCrate, "build-link-test-support"),
    buildCommand:
      "worker-build crates/collaboration-central-cloudflare --dev --no-opt --out-dir build-link-test-support -- --features link,test-support",
    durableObjects: {},
  };
}

function workerOptions(bundle: WorkerdBundle): V4WorkerOptions {
  if (bundle.c2OrdinaryAcceptProbe === true && bundle.c2Witness === undefined)
    throw new Error("ordinary accept probe requires C2 witnesses");
  if (bundle.c2Witness !== undefined && bundle.durableObjects.CENTRAL !== "CentralObject")
    throw new Error("C2 witness requires the real CENTRAL class binding");
  for (const name of ["index.js", "index_bg.wasm"])
    if (!existsSync(join(bundle.bundleDirectory, name)))
      throw new Error(`Worker bundle missing ${name}; build it with: ${bundle.buildCommand}`);
  const durableObjects: Record<string, { className: string; useSQLite: true }> = {};
  for (const [binding, className] of Object.entries(bundle.durableObjects))
    durableObjects[binding] = { className, useSQLite: true };
  return {
    compatibilityDate: bundle.compatibilityDate,
    modulesRoot: bundle.bundleDirectory,
    modules: [
      ...(bundle.c2Witness === undefined
        ? []
        : [
            {
              type: "ESModule" as const,
              path: join(bundle.bundleDirectory, "__c2_witness_entry.js"),
              contents:
                bundle.c2OrdinaryAcceptProbe === true
                  ? ordinaryAcceptProbeSource()
                  : c2WitnessWrapperSource,
            },
          ]),
      { type: "ESModule", path: join(bundle.bundleDirectory, "index.js") },
      { type: "CompiledWasm", path: join(bundle.bundleDirectory, "index_bg.wasm") },
    ],
    bindings: { ...bundle.textBindings },
    durableObjects,
    ...(bundle.c2Witness === undefined
      ? {}
      : { serviceBindings: { TEST_C2_WITNESS: bundle.c2Witness.deliver } }),
    outboundService: (): never => {
      throw new Error("unexpected external workerd fetch");
    },
  };
}

function clockedWorkerOptions(
  bundle: WorkerdBundle,
  clock: ((organization: string) => number) | undefined,
): V4WorkerOptions {
  const options = workerOptions(bundle);
  return {
    ...options,
    name: "verification-central",
    ...(clock === undefined
      ? {}
      : {
          serviceBindings: {
            ...options.serviceBindings,
            TEST_SIGNIN_CLOCK: signInClockService(clock),
          },
        }),
  };
}

async function resolvePersistenceRoot(directory: string | undefined): Promise<string> {
  if (directory === undefined) return mkdtemp(join(tmpdir(), "workerd-central-"));
  const { privateDirectory } = await import("../machines/owned-process.ts");
  await privateDirectory(directory);
  return directory;
}

export type C2EvictionCause =
  | "activeReferences"
  | "platformFailure"
  | "namespaceUnavailable"
  | "witnessDrainFailed";

export class C2EvictionError extends Error {
  readonly reason: C2EvictionCause;

  constructor(reason: C2EvictionCause) {
    super(reason);
    this.reason = reason;
  }
}

function signInWorkers(
  bundle: WorkerdBundle,
  central: V4WorkerOptions,
  signIn: WorkerdSignInOptions,
  jwksRequests: JwksRequests,
): V4WorkerOptions[] {
  const mapping = jwksMappingSchema.parse(signIn.jwks);
  const className = bundle.durableObjects.CENTRAL;
  if (className === undefined)
    throw new Error("sign-in topology requires the public CENTRAL namespace");
  if (Object.keys(signIn.linkBundle.durableObjects).length !== 0)
    throw new Error("private link must not own a separate Durable Object namespace");
  const outboundService = (): Response => {
    jwksRequests.publicRequests++;
    return Response.json(mapping.publicTrapJwks ?? { keys: [] });
  };
  const workers: V4WorkerOptions[] = [
    {
      name: "verification-gateway",
      compatibilityDate: bundle.compatibilityDate,
      modules: true,
      script: gatewaySource,
      serviceBindings: {
        CENTRAL_LINK: "verification-central",
        PRIVATE_LINK: "verification-private-link",
      },
      outboundService: (): never => {
        throw new Error("unexpected gateway outbound fetch");
      },
    },
    {
      ...central,
      outboundService,
      ...(mapping.fixtureUrl === undefined
        ? {}
        : {
            serviceBindings: {
              ...central.serviceBindings,
              SIGN_IN_KEYS: { name: "verification-sign-in-keys", entrypoint: "SignInJwks" },
            },
          }),
    },
    {
      ...workerOptions(signIn.linkBundle),
      name: "verification-private-link",
      durableObjects: {
        CENTRAL: { className, scriptName: "verification-central", useSQLite: true },
      },
    },
  ];
  if (mapping.fixtureUrl !== undefined)
    workers.push({
      name: "verification-sign-in-keys",
      compatibilityDate: bundle.compatibilityDate,
      modules: true,
      script: signInKeysSource,
      serviceBindings: { KEY_FIXTURE: privateKeysFixture(mapping.fixtureUrl, jwksRequests) },
      outboundService: (): never => {
        throw new Error("unexpected keys fixture outbound fetch");
      },
    });
  return workers;
}

export class WorkerdStorage {
  readonly origin: string;
  private readonly runtime: Miniflare;
  private readonly temporaryRoot: string | undefined;
  private readonly c2Witness: C2WitnessRecorder | undefined;
  private readonly jwksRequests: JwksRequests;
  private constructor(
    origin: string,
    runtime: Miniflare,
    temporaryRoot: string | undefined,
    c2Witness: C2WitnessRecorder | undefined,
    jwksRequests: JwksRequests,
  ) {
    this.origin = origin;
    this.runtime = runtime;
    this.temporaryRoot = temporaryRoot;
    this.c2Witness = c2Witness;
    this.jwksRequests = jwksRequests;
  }

  observeJwksRequests(): Readonly<JwksRequests> {
    return { ...this.jwksRequests };
  }

  static async start(
    bundle: WorkerdBundle,
    signIn?: WorkerdSignInOptions,
    clock?: (organization: string) => number,
    options: {
      readonly createRuntime?: (construct: () => Miniflare) => Miniflare | Promise<Miniflare>;
      /** Existing directory that the caller owns and cleans up after this host closes. */
      readonly persistenceDirectory?: string;
    } = {},
  ): Promise<WorkerdStorage> {
    const require = createRequire(import.meta.url);
    z.object({ version: z.literal("5.20261012.0-beta") }).parse(require("miniflare/package.json"));
    z.object({ version: z.literal("1.20261012.1") }).parse(require("workerd"));
    const central = clockedWorkerOptions(bundle, clock);
    const jwksRequests = { privateGets: 0, unexpectedPrivate: 0, publicRequests: 0 };
    const workers =
      signIn === undefined ? [central] : signInWorkers(bundle, central, signIn, jwksRequests);
    const { persistenceDirectory } = options;
    const root = await resolvePersistenceRoot(persistenceDirectory);
    const temporaryRoot = persistenceDirectory === undefined ? root : undefined;
    const createRuntime =
      options.createRuntime ?? ((construct: () => Miniflare): Miniflare => construct());
    let runtime: Miniflare | undefined;
    try {
      runtime = await createRuntime(
        () =>
          new Miniflare(
            convertV4MiniflareOptions({
              host: "127.0.0.1",
              port: 0,
              resourcePersistencePath: root,
              cf: false,
              workers,
            }),
          ),
      );
      const ready = await runtime.ready;
      if (
        ready.hostname !== "127.0.0.1" ||
        [8787, 8788, 19741, 19742, 19743].includes(Number(ready.port))
      )
        throw new Error("workerd did not bind a permitted loopback port");
      return new WorkerdStorage(
        ready.origin,
        runtime,
        temporaryRoot,
        bundle.c2Witness,
        jwksRequests,
      );
    } catch (error: unknown) {
      const failures: unknown[] = [error];
      try {
        await runtime?.dispose();
      } catch (cleanup: unknown) {
        failures.push(cleanup);
      }
      try {
        if (temporaryRoot !== undefined) await rm(temporaryRoot, { recursive: true, force: true });
      } catch (cleanup: unknown) {
        failures.push(cleanup);
      }
      if (failures.length > 1)
        throw new AggregateError(failures, "Worker startup and cleanup failed", { cause: error });
      throw error;
    }
  }

  async close(): Promise<void> {
    const failures: unknown[] = [];
    try {
      await this.runtime.dispose();
    } catch (error: unknown) {
      failures.push(error);
    }
    try {
      if (this.temporaryRoot !== undefined)
        await rm(this.temporaryRoot, { recursive: true, force: true });
    } catch (error: unknown) {
      failures.push(error);
    }
    if (failures.length > 0)
      throw new AggregateError(failures, "Worker cleanup failed", { cause: failures[0] });
  }

  async dispatchJson(path: string, body: unknown): Promise<unknown> {
    if (!path.startsWith("/") || path.startsWith("//"))
      throw new Error("fixture control requires a relative path");
    const response = await this.runtime.dispatchFetch(`${this.origin}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new Error("Worker fixture control failed");
    return response.json();
  }

  async evictCentral(organization: string): Promise<void> {
    const name = z.uuidv7().parse(organization);
    let resumeDelivery: (() => void) | undefined;
    if (this.c2Witness !== undefined) {
      let stub: unknown;
      try {
        // Miniflare's replacement-type utility erases the namespace methods.
        const namespace: unknown = await this.runtime.getDurableObjectNamespace(
          "CENTRAL",
          "verification-central",
        );
        if (
          typeof namespace !== "object" ||
          namespace === null ||
          !("idFromName" in namespace) ||
          typeof namespace.idFromName !== "function" ||
          !("get" in namespace) ||
          typeof namespace.get !== "function"
        )
          throw new C2EvictionError("namespaceUnavailable");
        const objectId: unknown = namespace.idFromName(name);
        stub = namespace.get(objectId);
        resumeDelivery = this.c2Witness.cancelDeliveriesForEviction(String(objectId));
      } catch {
        throw new C2EvictionError("namespaceUnavailable");
      }
      try {
        if (
          typeof stub !== "object" ||
          stub === null ||
          !("fetch" in stub) ||
          typeof stub.fetch !== "function"
        )
          throw new C2EvictionError("witnessDrainFailed");
        const drained: unknown = await stub.fetch(
          "https://c2-witness-control.test/before-eviction",
          {
            method: "POST",
            signal: AbortSignal.timeout(5000),
          },
        );
        if (!z.object({ status: z.literal(204) }).safeParse(drained).success)
          throw new C2EvictionError("witnessDrainFailed");
      } catch {
        resumeDelivery?.();
        throw new C2EvictionError("witnessDrainFailed");
      }
    }
    try {
      await this.runtime.unsafeEvictDurableObject("verification-central", "CentralObject", {
        name,
        webSockets: "hibernate",
      });
    } catch (error) {
      const activeReferences =
        error instanceof Error &&
        error.message ===
          "Timed out waiting to evict Durable Object: it still has active references.";
      throw new C2EvictionError(activeReferences ? "activeReferences" : "platformFailure");
    } finally {
      resumeDelivery?.();
    }
  }
}
