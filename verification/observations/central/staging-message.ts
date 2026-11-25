import { spawn } from "node:child_process";
import { randomUUIDv7 } from "node:crypto";
import { lstat, mkdir, readFile, realpath } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, relative } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import { z } from "zod";
import { artifactSchema, fileSchema, sessionSchema } from "../../declaration.ts";
import {
  command,
  fingerprint,
  privateDirectory,
  qualifyArtifact,
} from "../../machines/owned-process.ts";
import { ObservationFailure, Receipt } from "../../receipt.ts";
import { StagingMessageEvents } from "./staging-message-events.ts";

const absolutePath = z.string().refine(isAbsolute);
const declarationSchema = z.strictObject({
  journey: z.enum(["idle", "running"]),
  answerLines: z.number().int().min(4000).max(12000).default(8000),
  source: z.string().regex(/^[a-f0-9]{40}$/u),
  router: artifactSchema,
  collaboration: artifactSchema,
  root: absolutePath,
  port: z.number().int().min(1024).max(65535),
  machineId: z.uuid(),
  hostPid: z.number().int().positive(),
  claudeAdapter: fileSchema,
  claudeAdapterPid: z.number().int().positive(),
  sender: sessionSchema,
  recipient: sessionSchema,
  evidence: absolutePath,
});
type Plan = z.infer<typeof declarationSchema>;
type CommandResult = Awaited<ReturnType<typeof command>>;

const inspectSchema = z.object({
  result: z.object({
    record: z.object({
      target: sessionSchema,
      state: z.string(),
      capabilities: z.object({
        steer: z.boolean(),
        authStatus: z.object({ kind: z.string() }).optional(),
      }),
      settingsCatalog: z.object({
        configOptions: z.array(
          z.object({
            id: z.string(),
            currentValue: z.object({ value: z.string() }).optional(),
          }),
        ),
      }),
    }),
  }),
});
const senderSchema = z.object({
  result: z.object({
    record: z.object({ session: sessionSchema, endpointRegistered: z.literal(true) }),
  }),
});
const quotaSchema = z.object({
  accounts: z.array(
    z.object({
      provider: z.string(),
      credential_store_status: z.string(),
      oauth_maintenance_state: z.string(),
    }),
  ),
});
const sendSchema = z.object({
  kind: z.literal("returned"),
  value: z.object({
    kind: z.enum(["inOutbox", "answered"]),
    requestId: z.uuid(),
    flushAt: z.string().optional(),
    messageId: z.uuid().optional(),
  }),
});
const statusSchema = z.object({
  kind: z.string(),
  value: z
    .object({
      kind: z.string(),
      messageId: z.uuid().optional(),
      delivery: z
        .object({
          kind: z.string(),
          value: z.object({ kind: z.string(), deadlinePassed: z.boolean().optional() }).optional(),
        })
        .optional(),
    })
    .optional(),
});
function holdPrompt(answerLines: number): string {
  return (
    "This is a bounded verification fixture. Your answer must contain the decimal numbers " +
    `1 through ${answerLines} inclusive, in order, one number per line. ` +
    "Write every number yourself without tools. Do not use ellipses or ranges. " +
    "If an unrelated notice arrives, continue the numbered answer without tools."
  );
}

function firstLine(value: string): string {
  return value.split("\n", 1)[0] ?? "";
}

function serviceDirectory(plan: Plan): string {
  return join(plan.root, "agent-communication");
}

function cli(plan: Plan, args: readonly string[], timeoutSeconds = 20): Promise<CommandResult> {
  return command(
    plan.collaboration.path,
    [...args, "--service-directory", serviceDirectory(plan)],
    {
      timeoutSeconds,
    },
  );
}

function parseCommand(result: CommandResult, label: string): unknown {
  if (result.exit !== 0) throw new Error(`${label} failed with exit ${String(result.exit)}`);
  try {
    return JSON.parse(result.stdout);
  } catch {
    throw new Error(`${label} returned invalid JSON`);
  }
}

async function readPlan(file: string): Promise<Plan> {
  if (!isAbsolute(file) || (await realpath(file)) !== file)
    throw new Error("pass a canonical absolute path to the private declaration");
  const info = await lstat(file);
  if (!info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0)
    throw new Error("the declaration must be an owner-private file");
  const outside = relative(process.cwd(), file);
  if (!outside.startsWith("../") && !isAbsolute(outside))
    throw new Error("keep the private declaration outside the checkout");
  return declarationSchema.parse(JSON.parse(await readFile(file, "utf8")));
}

async function requireLocalClaudeRoute(plan: Plan): Promise<void> {
  const host = await command("/bin/ps", ["-ww", "-p", String(plan.hostPid), "-o", "command="]);
  if (
    host.exit !== 0 ||
    !host.stdout.startsWith(`${plan.router.path} host `) ||
    !host.stdout.includes(`--router-root ${plan.root} `) ||
    !host.stdout.includes("--require-debug-isolation") ||
    !host.stdout.includes(`--port ${plan.port} `)
  )
    throw new Error("declared Host PID is not the signed debug Host on this root and port");
  const relation = await command("/bin/ps", ["-p", String(plan.claudeAdapterPid), "-o", "ppid="]);
  if (relation.exit !== 0 || Number(relation.stdout.trim()) !== plan.hostPid)
    throw new Error("declared Claude adapter is not a child of the declared Host");
  // Only the expected non-secret route and script path are compared. Do not save ps environment output.
  const adapter = await command("/bin/ps", [
    "eww",
    "-p",
    String(plan.claudeAdapterPid),
    "-o",
    "command=",
  ]);
  const assigned = adapter.stdout.match(/(?:^|\s)ANTHROPIC_BASE_URL=([^\s]*)/gu) ?? [];
  if (
    adapter.exit !== 0 ||
    !adapter.stdout.includes(pathToFileURL(plan.claudeAdapter.path).href) ||
    assigned.length !== 1 ||
    assigned[0]?.trim() !== `ANTHROPIC_BASE_URL=http://127.0.0.1:${plan.port}/anthropic`
  )
    throw new Error("cannot confirm the managed Claude adapter's actual debug proxy route");
}

async function prepare(plan: Plan, receipt: Receipt): Promise<void> {
  const production = join(homedir(), ".codex-router");
  if (
    (await realpath(plan.root)) !== plan.root ||
    plan.root === production ||
    plan.root.startsWith(`${production}/`) ||
    [8787, 8788, 19741, 19742, 19743, 43127].includes(plan.port) ||
    plan.router.identifier !== "dev.shravansunder.codex-router.debug" ||
    plan.collaboration.identifier !== "dev.shravansunder.agent-collaboration.debug" ||
    plan.router.team !== "974QD84WVC" ||
    plan.collaboration.team !== "974QD84WVC"
  )
    throw new Error("a non-production root, debug port and signed debug artifacts are required");
  await qualifyArtifact(plan.router);
  await qualifyArtifact(plan.collaboration);
  if ((await fingerprint(plan.claudeAdapter.path)) !== plan.claudeAdapter.sha256)
    throw new Error("Claude adapter fingerprint changed");
  await requireLocalClaudeRoute(plan);
  const environment = { ...process.env, CODEX_ROUTER_DEBUG_ROUTER_ROOT: plan.root };
  Reflect.deleteProperty(environment, "CODEX_ROUTER_USE_HOME_DEFAULT");
  const enrollment = await command(plan.router.path, ["enrollment", "status"], { environment });
  const enrolled =
    enrollment.exit === 0 &&
    firstLine(enrollment.stdout) === "enrolled, connected" &&
    enrollment.stdout.includes(`Machine: ${plan.machineId}\n`) &&
    enrollment.stdout.includes("Sign-in: https://auth-staging.agent-collaboration.com/\n");
  await receipt.record("readiness", { enrolled, exit: enrollment.exit });
  if (!enrolled) throw new Error("declared debug Host is not enrolled and connected to staging");
  const quota = await command(
    plan.router.path,
    ["quota", "--router-root", plan.root, "--format", "json", "--no-refresh"],
    { environment },
  );
  const { accounts } = quotaSchema.parse(parseCommand(quota, "debug Claude credential readiness"));
  const readyClaude = accounts.some(
    (account) =>
      account.provider === "claude" &&
      account.credential_store_status === "ready" &&
      account.oauth_maintenance_state === "healthy",
  );
  await receipt.record("debug-claude-credentials", { ready: readyClaude, exit: quota.exit });
  if (!readyClaude) throw new Error("no already authenticated Claude account on the debug root");
  const identity = senderSchema.parse(
    parseCommand(await cli(plan, ["whoami", "--json"]), "sender identity"),
  );
  if (
    identity.result.record.session.sessionId !== plan.sender.sessionId ||
    identity.result.record.session.endpoint.endpointId !== plan.sender.endpoint.endpointId ||
    identity.result.record.session.endpoint.serviceId !== plan.sender.endpoint.serviceId ||
    plan.sender.endpoint.serviceId !== plan.recipient.endpoint.serviceId ||
    plan.sender.sessionId === plan.recipient.sessionId ||
    plan.recipient.endpoint.endpointId !== "claude-local"
  )
    throw new Error("same-Mac sender and recipient identities do not match the declaration");
  const inspected = await inspect(plan);
  if (inspected.state !== "idle" || (plan.journey === "running" && !inspected.capabilities.steer))
    throw new Error(
      "recipient must be open and idle, with steering advertised for the running case",
    );
  await receipt.record("recipient-ready", {
    model: "haiku",
    steer: inspected.capabilities.steer,
    adapterAuth: inspected.capabilities.authStatus?.kind,
    proxy: `http://127.0.0.1:${plan.port}/anthropic`,
  });
}

async function inspect(plan: Plan): Promise<z.infer<typeof inspectSchema>["result"]["record"]> {
  const result = await cli(plan, [
    "session",
    "inspect",
    "--to",
    JSON.stringify(plan.recipient),
    "--json",
  ]);
  const { record } = inspectSchema.parse(parseCommand(result, "recipient inspect")).result;
  const model = record.settingsCatalog.configOptions.find(({ id }) => id === "model")?.currentValue
    ?.value;
  if (
    record.target.sessionId !== plan.recipient.sessionId ||
    record.target.endpoint.serviceId !== plan.recipient.endpoint.serviceId ||
    record.target.endpoint.endpointId !== "claude-local" ||
    model !== "haiku"
  )
    throw new Error(
      "exact declared Claude recipient or effective Haiku setting cannot be confirmed",
    );
  return record;
}

interface RecipientStream {
  readonly view: StagingMessageEvents;
  assertContinuous(): void;
  close(): Promise<void>;
}

function attachRecipient(plan: Plan): RecipientStream {
  const view = new StagingMessageEvents(plan.recipient);
  const child = spawn(
    plan.collaboration.path,
    [
      "events",
      "listen",
      "--endpoint",
      "claude-local",
      "--session",
      plan.recipient.sessionId,
      "--attach",
      "--service-directory",
      serviceDirectory(plan),
      "--timeout-seconds",
      "420",
    ],
    { stdio: ["ignore", "pipe", "ignore"] },
  );
  let streamError: Error | undefined;
  const exited = new Promise<void>((resolve) => {
    child.once("close", () => {
      streamError ??= new Error("recipient observation ended");
      resolve();
    });
  });
  child.once("error", () => {
    streamError = new Error("failed to start recipient observation");
  });
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  lines.on("line", (line: string): void => {
    if (streamError !== undefined) return;
    try {
      view.ingest(line);
    } catch (error: unknown) {
      streamError = error instanceof Error ? error : new Error("invalid recipient event");
    }
  });
  return {
    view,
    assertContinuous(): void {
      if (streamError !== undefined) throw streamError;
    },
    async close(): Promise<void> {
      child.kill("SIGTERM");
      await exited;
      lines.close();
    },
  };
}

async function observe(plan: Plan, receipt: Receipt): Promise<void> {
  const stream = attachRecipient(plan);
  const { view, assertContinuous: assertStream, close } = stream;
  let hold: Promise<CommandResult | undefined> | undefined;
  let holdOperationId: string | undefined;
  let runningTurn: string | undefined;
  try {
    await requireIdleObservation(plan, stream);
    if (plan.journey === "running") {
      holdOperationId = randomUUIDv7();
      await receipt.record("hold-command", {
        operationId: holdOperationId,
        lines: plan.answerLines,
      });
      hold = cli(
        plan,
        [
          "conversation",
          "prompt",
          "--to",
          JSON.stringify(plan.recipient),
          "--operation-id",
          holdOperationId,
          "--text",
          holdPrompt(plan.answerLines),
          "--timeout-seconds",
          "320",
          "--json",
        ],
        325,
      ).catch(() => undefined);
      for (let attempt = 0; attempt < 180; attempt++) {
        assertStream();
        if (view.activeTurn !== null && view.answeringTurns.has(view.activeTurn)) {
          runningTurn = view.activeTurn;
          break;
        }
        if (view.ends.length !== 0) break;
        await setTimeout(250);
      }
      if (
        runningTurn === undefined ||
        (await inspect(plan)).state !== "running" ||
        view.activeTurn !== runningTurn ||
        view.toolTurns.has(runningTurn)
      )
        throw new Error("the bounded no-tools answer did not produce an observable running turn");
      await receipt.record("held-recipient-turn", { turnId: runningTurn });
    }
    const startCount = view.starts.length;
    const requestId = randomUUIDv7();
    await receipt.record("send-attempt", { requestId, runningTurn });
    assertStream();
    const output = await cli(plan, [
      "message",
      "send",
      "--to",
      JSON.stringify({ machineId: plan.machineId, sessionId: plan.recipient.sessionId }),
      "--request-id",
      requestId,
      "--body",
      `Staging notice verification ${requestId}`,
      "--json",
    ]);
    const send = sendSchema.parse(parseCommand(output, "send"));
    if (send.value.requestId !== requestId) throw new Error("send returned another request ID");
    await receipt.record("send", send);
    await checkDelivery({ plan, receipt, view, requestId, runningTurn, startCount, assertStream });
  } finally {
    const runTurn = runningTurn ?? (hold === undefined ? undefined : view.starts[0]);
    if (runTurn !== undefined && view.activeTurn === runTurn) {
      const cleanup = await cli(plan, [
        "turn",
        "interrupt",
        "--to",
        JSON.stringify(plan.recipient),
        "--turn",
        runTurn,
        "--json",
      ]).catch(() => undefined);
      await receipt.record("interrupt-own-hold", { turnId: runTurn, exit: cleanup?.exit ?? null });
    }
    if (hold !== undefined) {
      const settled = await hold;
      await receipt.record("hold-command-settlement", {
        exit: settled?.exit ?? null,
        permissionRejected: await holdPermissionRejected(plan, holdOperationId, settled),
      });
    }
    await close();
    await receipt.record("recipient-events", {
      starts: view.starts,
      ends: view.ends,
      notices: view.notices,
    });
  }
}

async function requireIdleObservation(plan: Plan, stream: RecipientStream): Promise<void> {
  for (let attempt = 0; !stream.view.ready && attempt < 80; attempt++) {
    stream.assertContinuous();
    await setTimeout(250);
  }
  stream.assertContinuous();
  if (
    !stream.view.ready ||
    (await inspect(plan)).state !== "idle" ||
    stream.view.starts.length !== 0
  )
    throw new Error("recipient did not provide an idle, continuous pre-send observation");
}

async function holdPermissionRejected(
  plan: Plan,
  operationId: string | undefined,
  settled: CommandResult | undefined,
): Promise<boolean> {
  if (operationId === undefined || settled?.exit !== 4) return false;
  const waited = await cli(plan, [
    "conversation",
    "operation",
    "wait",
    "--operation-id",
    operationId,
    "--timeout-seconds",
    "2",
    "--json",
  ]).catch(() => undefined);
  if (waited === undefined) return false;
  try {
    const result = z
      .object({
        kind: z.literal("error"),
        error: z.object({ kind: z.literal("permissionRejected") }),
      })
      .safeParse(JSON.parse(waited.stdout));
    return result.success;
  } catch {
    return false;
  }
}

interface DeliveryCheck {
  readonly plan: Plan;
  readonly receipt: Receipt;
  readonly view: StagingMessageEvents;
  readonly requestId: string;
  readonly runningTurn: string | undefined;
  readonly startCount: number;
  readonly assertStream: () => void;
}

async function checkDelivery(check: DeliveryCheck): Promise<void> {
  const { plan, receipt, view, requestId, runningTurn, startCount, assertStream } = check;
  const expires = performance.now() + 275_000;
  let seen: string | undefined;
  let messageId: string | undefined;
  let deliveryWitnessedWhileRunningAt: number | undefined;
  let firstNoticeAt: number | undefined;
  let previousStatus: string | undefined;
  for (let elapsed = performance.now(); elapsed < expires; elapsed = performance.now()) {
    assertStream();
    const result = await cli(plan, ["message", "status", "--request-id", requestId, "--json"]);
    const status = statusSchema.parse(parseCommand(result, "sender status"));
    const encoded = JSON.stringify(status);
    if (encoded !== previousStatus) {
      await receipt.record("sender-status", { requestId, status, exit: result.exit });
      previousStatus = encoded;
    }
    const delivery = status.value?.delivery?.value;
    const observationId = status.value?.messageId;
    if (observationId !== undefined) {
      if (messageId !== undefined && messageId !== observationId)
        throw new Error("sender reported inconsistent Central message identities");
      messageId = observationId;
    }
    if (
      status.kind !== "returned" ||
      ["refused", "notSent", "notConfirmed", "notFound"].includes(status.value?.kind ?? "") ||
      delivery?.kind === "refused"
    )
      throw new Error("sender reported refusal or broken custody");
    if (delivery?.kind === "seen") seen = status.value?.messageId;
    if (view.notices.length > 1)
      throw new ObservationFailure("recipient received multiple notices");
    const notice = view.notices[0];
    if (notice !== undefined) firstNoticeAt ??= performance.now();
    if (runningTurn !== undefined) {
      if (
        notice === undefined &&
        view.activeTurn === runningTurn &&
        view.toolTurns.has(runningTurn)
      )
        throw new Error("the held turn used a tool");
      if (view.activeTurn === runningTurn && delivery?.kind === "delivered")
        deliveryWitnessedWhileRunningAt ??= performance.now();
      if (
        view.starts.length !== startCount ||
        (notice !== undefined && notice.turnId !== runningTurn)
      )
        throw new ObservationFailure("notice started or entered a different turn");
      if (view.ends.includes(runningTurn) && notice === undefined) {
        if (
          deliveryWitnessedWhileRunningAt !== undefined &&
          performance.now() - deliveryWitnessedWhileRunningAt >= 65_000
        )
          throw new ObservationFailure("accepted message did not enter the already-running turn");
        throw new Error("hold ended before a full notice window after recipient Mac custody");
      }
    } else if (
      view.starts.length > 1 ||
      (notice !== undefined && notice.turnId !== view.starts[0])
    ) {
      throw new ObservationFailure("idle recipient did not receive the notice in one new turn");
    }
    const noticeObserved =
      notice?.turnId != null &&
      (view.ends.includes(notice.turnId) ||
        (firstNoticeAt !== undefined && performance.now() - firstNoticeAt >= 65_000));
    if (seen !== undefined && noticeObserved) {
      await receipt.record("pass", {
        requestId,
        messageId: seen,
        notice,
        starts: view.starts.length,
      });
      return;
    }
    await setTimeout(2500);
  }
  if (seen !== undefined && view.notices.length === 0)
    throw new ObservationFailure("Seen without a matching visible notice");
  if (seen !== undefined)
    throw new Error("observation ended before the notice window was complete");
  throw new Error("the observation bound ended without a sender Seen receipt");
}

export async function runStagingMessage(file: string): Promise<void> {
  const plan = await readPlan(file);
  await privateDirectory(await realpath(join(plan.evidence, "..")));
  await mkdir(plan.evidence, { mode: 0o700 });
  await privateDirectory(plan.evidence);
  const receipt = new Receipt(plan.evidence);
  await receipt.save("target.json", plan);
  try {
    await prepare(plan, receipt);
    await observe(plan, receipt);
    await receipt.save("outcome.json", { kind: "passed", journey: plan.journey });
    process.stdout.write(`Staging ${plan.journey} notice observed. Receipt: ${plan.evidence}\n`);
  } catch (error: unknown) {
    const failed = error instanceof ObservationFailure;
    const reason =
      error instanceof z.ZodError
        ? "product or declaration shape changed"
        : error instanceof Error
          ? error.message
          : "unknown setup failure";
    await receipt.save("outcome.json", { kind: failed ? "failed" : "blocked", reason });
    process.stderr.write(`${reason}. Receipt: ${plan.evidence}\n`);
    process.exitCode = failed ? 1 : 2;
  }
}
