// External witnesses for the real compiled CentralObject. No domain effects or tokens.
import assert from "node:assert/strict";
import { randomUUIDv7 } from "node:crypto";
import { EventEmitter, once } from "node:events";
import { Response, type V4FetchHandler } from "miniflare";
import { z } from "zod";

export const c2PhaseSchema = z.enum([
  "connecting",
  "baseline",
  "evicting",
  "heartbeat",
  "deciding",
  "settling",
]);
const objectIdSchema = z.string().regex(/^[0-9a-f]{64}$/);
const instanceIdSchema = z.uuidv7();
export const c2AttachmentSchema = z
  .object({
    organizationId: z.uuidv7(),
    machineId: z.uuidv7(),
    enrollmentId: z.uuidv7(),
    clientId: z.uuidv7(),
    effectiveExpiresAt: z.number().int().safe().nonnegative(),
  })
  .strict();
const common = z.object({ objectId: objectIdSchema });
const milliseconds = z.number().int().safe().nonnegative().nullable();
export const c2EventSchema = z.discriminatedUnion("kind", [
  common.extend({ kind: z.literal("constructor") }).strict(),
  common
    .extend({
      kind: z.literal("handler"),
      instanceId: instanceIdSchema,
      attachment: c2AttachmentSchema,
    })
    .strict(),
  common
    .extend({
      kind: z.literal("upgrade"),
      instanceId: instanceIdSchema,
      attachments: z.array(c2AttachmentSchema),
    })
    .strict(),
  common
    .extend({
      kind: z.literal("enrollmentRead"),
      query: z.enum(["byEnrollment", "machineById", "activeEnrollment"]),
      outcome: z.enum(["read", "unavailable"]),
    })
    .strict(),
  common
    .extend({
      kind: z.literal("restoreResult"),
      correlationId: z.uuidv7(),
      outcome: z.enum(["admitted", "refused", "unavailable"]),
    })
    .strict(),
  common
    .extend({
      kind: z.literal("alarmSnapshot"),
      correlationId: z.uuidv7(),
      earliestDueMs: milliseconds,
      alarmMs: milliseconds,
    })
    .strict(),
]);
export const c2RecordSchema = z
  .object({
    sequence: z.number().int().positive(),
    phase: c2PhaseSchema,
    instanceId: instanceIdSchema,
    event: c2EventSchema,
  })
  .strict();
export type C2WitnessRecord = z.infer<typeof c2RecordSchema>;

type WitnessPhase = z.infer<typeof c2PhaseSchema>;
export const c2HeldDiagnosticSchema = z
  .object({
    ordinal: z.number().int().positive(),
    kind: z.enum(["alarmSnapshot", "restoreResult"]),
    disposition: z.enum(["record", "refuse"]),
  })
  .strict();
type HeldDiagnostic = z.infer<typeof c2HeldDiagnosticSchema>;

export class C2WitnessRecorder {
  phase: WitnessPhase = "connecting";
  private readonly records: C2WitnessRecord[] = [];
  private readonly instances = new Map<string, string>();

  private readonly changes = new EventEmitter();
  private readonly deliveryByObject = new Map<string, AbortController>();
  private readonly deliverySignal = new WeakMap<Parameters<V4FetchHandler>[0], AbortSignal>();

  // Eviction cancels this object's observer acknowledgements before Workerd
  // waits for references. Queued deliveries receive a refusal until eviction
  // settles. Fencing never makes a request to the object.
  cancelDeliveriesForEviction(objectId: string): () => void {
    this.deliveryFor(objectId).abort();
    return () => {
      this.deliveryByObject.delete(objectId);
    };
  }

  private deliveryFor(objectId: string): AbortController {
    let delivery = this.deliveryByObject.get(objectId);
    if (delivery === undefined) {
      delivery = new AbortController();
      this.deliveryByObject.set(objectId, delivery);
    }
    return delivery;
  }

  readonly deliver: V4FetchHandler = async (request, runtime): Promise<Response> => {
    if (request.url !== "https://c2-witness.test/record") return this.observe(request, runtime);
    const target = common.safeParse(
      await request
        .clone()
        .json()
        .catch(() => undefined),
    );
    if (!target.success) return new Response(null, { status: 400 });
    const signal = this.deliveryFor(target.data.objectId).signal;
    if (signal.aborted) return new Response(null, { status: 503 });

    this.deliverySignal.set(request, signal);
    const cancel = Promise.withResolvers<Response>();
    const refuse = (): void => cancel.resolve(new Response(null, { status: 503 }));
    signal.addEventListener("abort", refuse, { once: true });
    try {
      return await Promise.race([this.observe(request, runtime), cancel.promise]);
    } finally {
      signal.removeEventListener("abort", refuse);
    }
  };
  private heldDiagnostic:
    | (HeldDiagnostic & {
        remaining: number;
        correlationId?: string;
        refused?: true;
        release: () => void;
        wait: Promise<void>;
      })
    | undefined;

  holdDecidingDiagnostic(control: HeldDiagnostic): void {
    assert.equal(this.phase, "deciding");
    assert.equal(this.heldDiagnostic, undefined, "a diagnostic callback is already armed");
    const { promise, resolve } = Promise.withResolvers<void>();
    this.heldDiagnostic = {
      ...control,
      remaining: control.ordinal,
      release: resolve,
      wait: promise,
    };
  }

  async settledAlarms(
    phase: WitnessPhase,
    correlationIds: readonly string[],
    expectRefused?: string,
  ): Promise<readonly C2WitnessRecord[]> {
    assert.equal(this.phase, phase, "settle before changing the recorder phase");
    // Watchdog only. Recorder progress is driven by observed events and the
    // causal hold is released by this read, never by waiting a fixed interval.
    const signal = AbortSignal.timeout(10_000);
    const gate = this.heldDiagnostic;
    assert.equal(gate?.disposition === "refuse", expectRefused !== undefined);
    while (true) {
      const records = this.snapshot();
      if (gate?.correlationId !== undefined) {
        assert.ok(
          correlationIds.includes(gate.correlationId) || gate.correlationId === expectRefused,
          "unexpected held callback correlation",
        );
        gate.release();
      }
      const alarms = new Set(
        records.flatMap(({ phase: recordedPhase, event }) =>
          recordedPhase === phase && event.kind === "alarmSnapshot" ? [event.correlationId] : [],
        ),
      );
      const observerSettled =
        gate === undefined ||
        (gate.disposition === "refuse"
          ? gate.refused === true && gate.correlationId === expectRefused
          : gate.correlationId !== undefined && alarms.has(gate.correlationId));
      if (observerSettled && correlationIds.every((id) => alarms.has(id))) return records;
      await once(this.changes, "changed", { signal });
    }
  }

  async settledLifecycle(
    kind: "upgrade" | "handler",
    count: number,
  ): Promise<readonly C2WitnessRecord[]> {
    const signal = AbortSignal.timeout(10_000);
    while (this.records.filter(({ event }) => event.kind === kind).length < count)
      await once(this.changes, "changed", { signal });
    return this.snapshot();
  }

  snapshot(): readonly C2WitnessRecord[] {
    return structuredClone(this.records);
  }

  readonly observe: V4FetchHandler = async (request): Promise<Response> => {
    if (request.method !== "POST" || request.url !== "https://c2-witness.test/record")
      return new Response("invalid witness request", { status: 400 });
    // Do not return parser diagnostics containing caller data.
    let input: unknown;
    try {
      input = await request.json();
    } catch {
      return new Response(null, { status: 400 });
    }
    const signal = this.deliverySignal.get(request);
    if (signal?.aborted) return new Response(null, { status: 503 });
    const parsed = c2EventSchema.safeParse(input);
    if (!parsed.success) return new Response(null, { status: 400 });
    const event = parsed.data;
    const instanceId =
      event.kind === "constructor" ? randomUUIDv7() : this.instances.get(event.objectId);
    if (instanceId === undefined) return new Response(null, { status: 409 });
    if ("instanceId" in event && event.instanceId !== instanceId)
      return new Response(null, { status: 409 });
    this.instances.set(event.objectId, instanceId);
    const gate = this.heldDiagnostic;
    if (gate !== undefined && this.phase === "deciding" && event.kind === gate.kind) {
      gate.remaining--;
      if (gate.remaining === 0) {
        gate.correlationId = event.correlationId;
        this.changes.emit("changed");
        signal?.addEventListener("abort", gate.release, { once: true });
        await gate.wait;
        signal?.removeEventListener("abort", gate.release);
        this.heldDiagnostic = undefined;
        if (signal?.aborted) return new Response(null, { status: 503 });
        if (gate.disposition === "refuse") {
          gate.refused = true;
          this.changes.emit("changed");
          return new Response(null, { status: 503 });
        }
      }
    }
    this.records.push({ sequence: this.records.length + 1, phase: this.phase, instanceId, event });
    this.changes.emit("changed");
    return event.kind === "constructor"
      ? Response.json({ instanceId })
      : new Response(null, { status: 204 });
  };
}

// The wrapper captures entry to the compiled class. Ordered Rust diagnostics
// settle separately from its WebSocket replies.
export const c2WitnessWrapperSource = `
import * as original from './index.js';
export * from './index.js';
export default original.default;
function observedAuthority(socket) {
  const attachment = socket.deserializeAttachment();
  // The qualification stand-in writes the flat authority snapshot directly.
  return attachment?.authority ?? attachment;
}
export class CentralObject extends original.CentralObject {
  constructor(state, env) {
    // Queue wrapper events before delegating, and route Rust callbacks through
    // the same ordered delivery pipe. No acknowledgement gates the delegate.
    let tail = Promise.resolve();
    const enqueue = task => {
      const delivery = tail.then(task);
      tail = delivery.catch(error => console.warn("C2 wrapper diagnostic failed", String(error)));
      return delivery;
    };
    const binding = env.TEST_C2_WITNESS;
    const witnessBinding = new Proxy(binding, { get(target, property) {
      if (property === 'fetch') return (...args) => enqueue(() => target.fetch(...args));
      return Reflect.get(target, property, target);
    }});
    const witnessEnv = new Proxy(env, { get(target, property, receiver) {
      return property === 'TEST_C2_WITNESS' ? witnessBinding : Reflect.get(target, property, receiver);
    }});
    const witnessWork = new Set();
    const trackWitnessWork = owner => new Proxy(owner, { get(target, property) {
      if (property === 'waitUntil') return promise => {
        witnessWork.add(promise);
        const settled = () => witnessWork.delete(promise);
        void promise.then(settled, settled);
        target.waitUntil(promise);
      };
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    }});
    const trackingState = trackWitnessWork(state);
    super(trackingState, witnessEnv);
    this.witnessState = trackingState;
    this.witnessWork = witnessWork;
    this.witnessBinding = binding;
    this.enqueueWitness = enqueue;
    this.waitForWitness(async () => {
      const response = await binding.fetch('https://c2-witness.test/record', {
        method: 'POST', body: JSON.stringify({kind:'constructor', objectId:state.id.toString()})
      });
      if (!response.ok) throw new Error('constructor witness refused');
      this.witnessInstance = (await response.json()).instanceId;
    });
  }
  waitForWitness(task) {
    this.witnessState.waitUntil(this.enqueueWitness(task).catch(() => {}));
  }
  recordWitness(event) {
    this.waitForWitness(async () => {
      const response = await this.witnessBinding.fetch('https://c2-witness.test/record', {
        method: 'POST', body: JSON.stringify({...event,
          objectId:this.witnessState.id.toString(), instanceId:this.witnessInstance})
      });
      if (response.status !== 204) throw new Error('object witness refused');
    });
  }
  async fetch(request) {
    if (request.method === 'POST' && request.url === 'https://c2-witness-control.test/before-eviction') {
      while (this.witnessWork.size !== 0) await Promise.allSettled(this.witnessWork);
      return new Response(null, {status:204});
    }
    const response = await super.fetch(request);
    if (response.status === 101) this.recordWitness({kind:'upgrade',
      attachments:this.witnessState.getWebSockets().map(observedAuthority)});
    return response;
  }
  async webSocketMessage(socket, message) {
    this.recordWitness({kind:'handler', attachment:observedAuthority(socket)});
    return super.webSocketMessage(socket, message);
  }
}
`;

/** Require real SQL I/O followed by typed restoration on the serving new instance. */
export function assertC2Restore(input: {
  readonly records: readonly C2WitnessRecord[];
  readonly correlationId: string;
  readonly before: readonly C2WitnessRecord[];
  readonly attachment: z.infer<typeof c2AttachmentSchema>;
}): void {
  const fresh = input.records.slice(input.before.length);
  const constructors = fresh.filter((record) => record.event.kind === "constructor");
  const handlers = fresh.filter((record) => record.event.kind === "handler");
  assert.equal(constructors.length, 1, "deciding frame needs one fresh constructor");
  assert.equal(handlers.length, 1, "deciding frame needs one semantic handler");
  const constructor = constructors[0];
  const handler = handlers[0];
  assert.ok(constructor !== undefined && handler !== undefined);
  assert.equal(constructor.phase, "deciding", "constructor witness must arrive while deciding");
  assert.equal(handler.phase, "deciding", "handler witness must arrive while deciding");
  assert.ok(constructor.sequence < handler.sequence, "handler follows fresh constructor");
  const originalObject = input.before.find((record) => record.event.kind === "constructor")?.event
    .objectId;
  assert.equal(
    constructor.event.objectId,
    originalObject,
    "same physical organization object must reconstruct",
  );
  assert.equal(
    handler.event.objectId,
    originalObject,
    "handler uses the original organization object",
  );
  assert.equal(handler.instanceId, constructor.instanceId, "handler must use new instance");
  assert.ok(
    !input.before.some((record) => record.instanceId === constructor.instanceId),
    "serving instance must be new",
  );
  assert.ok(handler.event.kind === "handler");
  assert.deepEqual(handler.event.attachment, input.attachment, "original attachment must restore");
  const restorations = fresh.filter(
    (record) =>
      record.event.kind === "restoreResult" && record.event.correlationId === input.correlationId,
  );
  assert.equal(restorations.length, 1, "matching restoration witness required exactly once");
  const restored = restorations[0];
  assert.ok(restored !== undefined, "matching restoration witness required");
  assert.equal(restored.instanceId, constructor.instanceId);
  assert.ok(restored.event.kind === "restoreResult");
  assert.equal(restored.event.outcome, "admitted", "live restoration must admit");
  assertC2LiveReads({
    records: fresh,
    instanceId: constructor.instanceId,
    objectId: constructor.event.objectId,
    after: handler.sequence,
    before: restored.sequence,
  });
  const alarms = fresh.filter(
    (record) =>
      record.event.kind === "alarmSnapshot" && record.event.correlationId === input.correlationId,
  );
  assert.equal(alarms.length, 1, "matching alarm snapshot required exactly once");
  const alarm = alarms[0];
  assert.ok(
    alarm !== undefined && alarm.event.kind === "alarmSnapshot",
    "matching actual alarm readback required",
  );
  assert.equal(alarm.instanceId, constructor.instanceId);
  assert.ok(alarm.sequence > restored.sequence, "alarm readback follows restoration");
  assert.equal(
    alarm.event.alarmMs,
    alarm.event.earliestDueMs,
    "platform alarm must match EARLIEST_DUE",
  );
}

/** Reads must belong to this member's interval, not another request's admission. */
export function assertC2LiveReads(input: {
  readonly records: readonly C2WitnessRecord[];
  readonly instanceId: string;
  readonly objectId: string;
  readonly after: number;
  readonly before: number;
}): void {
  const required = ["byEnrollment", "machineById", "activeEnrollment"];
  let next = 0;
  for (const record of input.records) {
    if (
      record.sequence > input.after &&
      record.sequence < input.before &&
      record.instanceId === input.instanceId &&
      record.event.objectId === input.objectId &&
      record.event.kind === "enrollmentRead" &&
      record.event.outcome === "read" &&
      required[next] === record.event.query
    )
      next += 1;
  }
  assert.equal(
    next,
    required.length,
    "all live enrollment queries must complete in this member's interval",
  );
}
