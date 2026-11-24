// Exercises the central session service in a SQLite Durable Object and reopens it
// after each applying request. The driver supplies clock, IDs and secrets.
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { z } from "zod";
import { WorkerdStorage, centralTestBundle } from "./workerd-storage.ts";

const fixtureTime = Date.parse("2026-11-22T10:00:00.000Z");
const fixtureDeadline = "2026-11-22T10:10:00.000Z";
const sessionId = (number: number): string =>
  `019ac1c0-0000-7000-8000-${number.toString(16).padStart(12, "0")}`;
const fixtureSecret = (seed: number): string => Buffer.alloc(32, seed).toString("base64url");
const machineId = sessionId(1);
const enrollmentId = sessionId(2);
const harnessId = sessionId(30);
const otherHarnessId = sessionId(31);
const fixtureWorkspace = "019a3dce-2000-7000-8000-000000000004";
const fixtureProject = "019a3dce-2000-7000-8000-000000000005";
const ordinary = { kind: "ordinary" };
const operator = { kind: "routerOperator" };
const endCause = { kind: "endedForGood" };

const jsonSchema = z.json();
type JsonValue = z.infer<typeof jsonSchema>;

const responseSchema = z.strictObject({
  reply: jsonSchema,
  reads: z.array(
    z.strictObject({ statement: z.enum(["address", "other"]), rowsRead: z.int().nonnegative() }),
  ),
});
const reservationSchema = z.strictObject({
  result: z.literal("reserved"),
  sessionId: z.uuidv7(),
  secret: z.string(),
  reservedUntil: z.string(),
  workspace: z.uuidv7(),
});

type SessionResponse = z.infer<typeof responseSchema>;

function createRequest(
  operation: number,
  attempt = 1,
  creator: JsonValue = operator,
  sessionKind: JsonValue = ordinary,
): JsonValue {
  return {
    operationId: sessionId(operation),
    attempt,
    creator,
    agentProgram: "codex",
    sessionKind,
    project: {
      kind: "named",
      projectRef: { workspace: fixtureWorkspace, project: fixtureProject },
    },
  };
}

function createCommand(request: JsonValue, candidate: number, secretSeed: number): JsonValue {
  return { kind: "create", request, candidateId: sessionId(candidate), secretSeed };
}

function bindCommand(session: string, harnessSessionId: string, secret: string): JsonValue {
  return { kind: "bind", request: { sessionId: session, harnessSessionId, secret } };
}

function endCommand(target: JsonValue): JsonValue {
  return { kind: "end", request: { target, cause: endCause } };
}

function creator(session: string, presented: JsonValue): JsonValue {
  return { kind: "session", sessionId: session, presented };
}

function presentedSecret(secret: string): JsonValue {
  return { kind: "secret", secret };
}

let central: WorkerdStorage;

before(async () => {
  central = await WorkerdStorage.start(centralTestBundle());
});

after(async () => {
  await central?.close();
});

interface CaseOptions {
  readonly now?: number;
  readonly enrollment?: string;
  readonly reopen?: boolean;
  readonly organization?: string;
}

async function fetchCase(
  object: string,
  operation: "sessions" | "crash",
  body: JsonValue,
): Promise<Response> {
  return fetch(`${central.origin}/test-support/storage/${object}/${operation}`, {
    method: "POST",
    headers: { "x-case-migrator": JSON.stringify("sessions") },
    body: JSON.stringify(body),
  });
}

async function command(
  object: string,
  input: JsonValue,
  options: CaseOptions = {},
): Promise<SessionResponse> {
  const scopedObject = [options.organization ?? sessionId(700), object].join(".");
  const response = await fetchCase(scopedObject, "sessions", {
    machineId,
    enrollmentId: options.enrollment ?? enrollmentId,
    nowMs: options.now ?? fixtureTime,
    command: input,
  });
  assert.equal(response.status, 200, await response.clone().text());
  if (options.reopen ?? true) {
    const crash = await fetchCase(scopedObject, "crash", null);
    assert.equal(crash.ok, false);
    await crash.text();
  }
  return responseSchema.parse(await response.json());
}

async function refusedStoreCommand(
  object: string,
  input: JsonValue,
  failurePattern: RegExp,
  options: Pick<CaseOptions, "now" | "enrollment" | "organization"> = {},
): Promise<void> {
  const scopedObject = [options.organization ?? sessionId(700), object].join(".");
  const response = await fetchCase(scopedObject, "sessions", {
    machineId,
    enrollmentId: options.enrollment ?? enrollmentId,
    nowMs: options.now ?? fixtureTime,
    command: input,
  });
  assert.equal(response.status, 409, await response.clone().text());
  const { failure } = z.strictObject({ failure: z.string() }).parse(await response.json());
  assert.match(failure, failurePattern);
  const crash = await fetchCase(scopedObject, "crash", null);
  assert.equal(crash.ok, false);
  await crash.text();
}

test("Workerd reopens retries, activates once, and retains the address after End", async () => {
  const object = "session-retries";
  await command(object, { kind: "seed", unrelatedSessions: 0 });

  const initial = createRequest(10, 2);
  await command(object, createCommand(initial, 100, 1));
  assert.deepEqual((await command(object, createCommand(initial, 101, 2))).reply, {
    result: "superseded",
  });
  assert.deepEqual((await command(object, createCommand(createRequest(10), 102, 3))).reply, {
    result: "superseded",
  });

  const rotated = reservationSchema.parse(
    (await command(object, createCommand(createRequest(10, 3), 103, 4), { now: fixtureTime + 1 }))
      .reply,
  );
  assert.deepEqual(rotated, {
    result: "reserved",
    sessionId: sessionId(100),
    secret: fixtureSecret(4),
    reservedUntil: fixtureDeadline,
    workspace: fixtureWorkspace,
  });

  assert.deepEqual(
    (await command(object, bindCommand(rotated.sessionId, harnessId, fixtureSecret(1)))).reply,
    { result: "wrongSecret" },
  );
  const bind = bindCommand(rotated.sessionId, harnessId, rotated.secret);
  assert.equal(
    z.object({ result: z.string() }).parse((await command(object, bind)).reply).result,
    "active",
  );
  assert.deepEqual(
    (await command(object, bindCommand(rotated.sessionId, otherHarnessId, rotated.secret))).reply,
    { result: "alreadyBoundToDifferentId" },
  );

  const target = { kind: "centralId", sessionId: rotated.sessionId };
  const end = (await command(object, endCommand(target), { now: fixtureTime + 2 })).reply;
  assert.deepEqual(end, {
    result: "settled",
    settlement: {
      state: "ended",
      cause: endCause,
      time: "2026-11-22T10:00:00.002Z",
    },
  });
  assert.deepEqual((await command(object, endCommand(target), { now: fixtureTime + 9 })).reply, {
    result: "alreadySettled",
    settlement: z.object({ settlement: z.json() }).parse(end).settlement,
  });

  assert.deepEqual(
    (await command(object, bindCommand(rotated.sessionId, harnessId, fixtureSecret(99)))).reply,
    end,
  );

  const later = reservationSchema.parse(
    (await command(object, createCommand(createRequest(11), 104, 5))).reply,
  );
  assert.deepEqual(
    (await command(object, bindCommand(later.sessionId, harnessId, later.secret))).reply,
    { result: "addressTaken" },
  );
});

test("Workerd orders creator authentication before replay and binds a delegate to that creator", async () => {
  const object = "session-creator";
  await command(object, { kind: "seed", unrelatedSessions: 0 });

  const parent = reservationSchema.parse(
    (await command(object, createCommand(createRequest(10), 100, 1))).reply,
  );
  await command(object, bindCommand(parent.sessionId, harnessId, parent.secret));
  const parentCreator = creator(parent.sessionId, presentedSecret(parent.secret));

  const child = { kind: "delegate", parent: parent.sessionId };
  const mismatched = { kind: "delegate", parent: sessionId(888) };
  assert.deepEqual(
    (await command(object, createCommand(createRequest(11, 1, parentCreator, mismatched), 101, 2)))
      .reply,
    { result: "refused", reason: { kind: "parentNotValid" } },
  );
  const childRequest = createRequest(11, 1, parentCreator, child);
  const delegate = reservationSchema.parse(
    (await command(object, createCommand(childRequest, 101, 2))).reply,
  );
  await command(object, bindCommand(delegate.sessionId, otherHarnessId, delegate.secret));
  assert.deepEqual(
    (await command(object, { kind: "facts", sessionId: delegate.sessionId })).reply,
    {
      state: "live",
      parent: { sessionId: parent.sessionId, machineId, enrollmentId },
    },
  );

  const storeOrganization = sessionId(700);
  const routeRequest = {
    kind: "approvalFacts",
    sessionId: delegate.sessionId,
    requestedOrganization: storeOrganization,
  };
  assert.deepEqual((await command(object, routeRequest)).reply, {
    live: true,
    route: { kind: "delegate", parent: { sessionId: parent.sessionId, machineId, enrollmentId } },
  });
  assert.deepEqual(
    await command(object, {
      ...routeRequest,
      requestedOrganization: sessionId(701),
    }),
    { reply: { live: false, route: null }, reads: [] },
  );

  const foreignOrganization = sessionId(701);
  await command(
    object,
    { kind: "seed", unrelatedSessions: 0 },
    { organization: foreignOrganization },
  );
  assert.deepEqual(await command(object, routeRequest, { organization: foreignOrganization }), {
    reply: { live: false, route: null },
    reads: [],
  });

  const usingNothing = createRequest(11, 1, creator(parent.sessionId, { kind: "nothing" }), child);
  assert.deepEqual((await command(object, createCommand(usingNothing, 102, 3))).reply, {
    result: "noSecret",
  });
  const usingWrong = createRequest(
    11,
    1,
    creator(parent.sessionId, presentedSecret(fixtureSecret(255))),
    child,
  );
  assert.deepEqual((await command(object, createCommand(usingWrong, 102, 3))).reply, {
    result: "wrongSecret",
  });
  await command(object, endCommand({ kind: "centralId", sessionId: parent.sessionId }));
  assert.deepEqual(
    (await command(object, { ...routeRequest, sessionId: parent.sessionId })).reply,
    { live: false, route: null },
  );
  assert.deepEqual((await command(object, routeRequest)).reply, {
    live: true,
    route: { kind: "delegate", parent: { sessionId: parent.sessionId, machineId, enrollmentId } },
  });

  assert.deepEqual((await command(object, createCommand(childRequest, 102, 3))).reply, {
    result: "sessionEnded",
  });
  assert.deepEqual(
    (
      await command(object, createCommand(createRequest(12), 102, 3), {
        enrollment: sessionId(999),
      })
    ).reply,
    { result: "revoked" },
  );
  await command(object, { kind: "corruptState", sessionId: parent.sessionId });
  await refusedStoreCommand(object, routeRequest, /session_state/);
});

test("Workerd checks End by operation contents and rolls back failures at each insert boundary", async () => {
  const object = "session-rollback";
  await command(object, { kind: "seed", unrelatedSessions: 0 });

  const initial = createRequest(10);
  const candidate = 100;
  await command(object, { kind: "failSessionInsert" });
  await refusedStoreCommand(
    object,
    createCommand(initial, candidate, 1),
    /^the store is unavailable$/,
  );
  assert.deepEqual(
    (await command(object, { kind: "countActor", sessionId: sessionId(candidate) })).reply,
    {
      count: 0,
    },
  );

  await command(object, { kind: "clearFailures" });
  const parent = reservationSchema.parse(
    (await command(object, createCommand(initial, candidate, 1))).reply,
  );
  await command(object, bindCommand(parent.sessionId, harnessId, parent.secret));
  const childRequest = createRequest(
    11,
    1,
    creator(parent.sessionId, presentedSecret(parent.secret)),
    { kind: "delegate", parent: parent.sessionId },
  );
  await command(object, { kind: "failDelegateInsert" });
  await refusedStoreCommand(
    object,
    createCommand(childRequest, 101, 2),
    /^the store is unavailable$/,
  );
  assert.deepEqual(
    (await command(object, { kind: "countActor", sessionId: sessionId(101) })).reply,
    {
      count: 0,
    },
  );
  await command(object, { kind: "clearFailures" });
  reservationSchema.parse((await command(object, createCommand(childRequest, 101, 2))).reply);

  const operationTarget = { kind: "operationId", operationId: sessionId(11) };
  assert.deepEqual(
    (await command(object, endCommand({ ...operationTarget, contentsDigest: fixtureSecret(255) })))
      .reply,
    { result: "notYoursToEnd" },
  );
  const { digest } = z
    .strictObject({ digest: z.string() })
    .parse((await command(object, { kind: "digest", request: childRequest })).reply);
  assert.deepEqual(
    (await command(object, endCommand({ ...operationTarget, contentsDigest: digest }))).reply,
    {
      result: "settled",
      settlement: {
        state: "abandoned",
        cause: { kind: "givenUp" },
        time: "2026-11-22T10:00:00.000Z",
      },
    },
  );
});

test("Workerd fails a typed read when a stored session state is unknown", async () => {
  const object = "session-corrupt-state";
  await command(object, { kind: "seed", unrelatedSessions: 0 });
  const reserved = reservationSchema.parse(
    (await command(object, createCommand(createRequest(10), 100, 1))).reply,
  );
  await command(object, { kind: "corruptState", sessionId: reserved.sessionId });
  await refusedStoreCommand(
    object,
    { kind: "facts", sessionId: reserved.sessionId },
    /session_state/,
  );
});

test("Workerd settles idle-ended sessions at the stored time and can retry a failed settlement", async () => {
  const object = "session-terminal-idle";
  await command(object, { kind: "seed", unrelatedSessions: 0 });
  const reserved = reservationSchema.parse(
    (await command(object, createCommand(createRequest(10), 100, 1))).reply,
  );
  const activated = reservationSchema.parse(
    (await command(object, createCommand(createRequest(11), 101, 2))).reply,
  );
  await command(object, bindCommand(activated.sessionId, harnessId, activated.secret));
  const replacementId = sessionId(90);
  await command(
    object,
    { kind: "replaceIdleEnrollment", replacementId },
    {
      now: fixtureTime + 600_000,
    },
  );

  const oldLinkRead = await command(object, { kind: "facts", sessionId: activated.sessionId });
  assert.deepEqual(oldLinkRead.reply, { state: "unavailable" });
  assert.ok(measuredRows(oldLinkRead) <= 3);
  const bind = bindCommand(reserved.sessionId, otherHarnessId, fixtureSecret(254));
  assert.deepEqual((await command(object, bind)).reply, { result: "revoked" });
  await command(object, { kind: "failSessionUpdate" });
  await refusedStoreCommand(object, bind, /^the store is unavailable$/, {
    enrollment: replacementId,
    now: fixtureTime + 600_001,
  });
  await command(object, { kind: "clearFailures" });

  const afterClockRollback = { enrollment: replacementId, now: fixtureTime + 599_999 };
  assert.deepEqual((await command(object, bind, afterClockRollback)).reply, {
    result: "settled",
    settlement: { state: "abandoned", cause: { kind: "timeRanOut" }, time: fixtureDeadline },
  });
  assert.deepEqual(
    (await command(object, createCommand(createRequest(11, 2), 102, 3), afterClockRollback)).reply,
    {
      result: "settled",
      settlement: { state: "ended", cause: { kind: "machineRevoked" }, time: fixtureDeadline },
    },
  );
});

test("Workerd settles an expired reservation before checking the Bind secret", async () => {
  const object = "session-bind-at-deadline";
  await command(object, { kind: "seed", unrelatedSessions: 0 });
  const reserved = reservationSchema.parse(
    (await command(object, createCommand(createRequest(10), 100, 1))).reply,
  );
  const first = await command(
    object,
    bindCommand(reserved.sessionId, harnessId, fixtureSecret(2)),
    { now: fixtureTime + 600_000 },
  );
  assert.deepEqual(first.reply, {
    result: "settled",
    settlement: { state: "abandoned", cause: { kind: "timeRanOut" }, time: fixtureDeadline },
  });
  assert.deepEqual(
    (
      await command(object, bindCommand(reserved.sessionId, harnessId, reserved.secret), {
        now: fixtureTime + 599_999,
      })
    ).reply,
    first.reply,
  );
});

function measuredRows(response: SessionResponse, addressOnly = false): number {
  const observations = addressOnly
    ? response.reads.filter(({ statement }) => statement === "address")
    : response.reads;
  assert.ok(observations.length > 0);
  return observations.reduce((total, observation) => total + observation.rowsRead, 0);
}

for (const size of [20, 10_000]) {
  test(`Workerd row ceilings with ${size} actor sessions`, async (context) => {
    const object = `session-budget-${size}`;
    await command(object, { kind: "seed", unrelatedSessions: size - 2 });
    const first = reservationSchema.parse(
      (await command(object, createCommand(createRequest(10), 100, 1))).reply,
    );
    const addressMiss = measuredRows(
      await command(object, bindCommand(first.sessionId, harnessId, first.secret)),
      true,
    );
    const delegateCreate = createRequest(
      11,
      1,
      creator(first.sessionId, presentedSecret(first.secret)),
      {
        kind: "delegate",
        parent: first.sessionId,
      },
    );
    const second = reservationSchema.parse(
      (await command(object, createCommand(delegateCreate, 101, 2))).reply,
    );
    const addressFound = measuredRows(
      await command(object, bindCommand(second.sessionId, harnessId, second.secret)),
      true,
    );
    await command(object, bindCommand(second.sessionId, otherHarnessId, second.secret));
    const liveDelegate = await command(object, { kind: "facts", sessionId: second.sessionId });
    assert.deepEqual(liveDelegate.reply, {
      state: "live",
      parent: { sessionId: first.sessionId, machineId, enrollmentId },
    });
    const delegateReads = measuredRows(liveDelegate);
    const live = await command(object, { kind: "facts", sessionId: first.sessionId });
    assert.deepEqual(live.reply, { state: "live" });
    await command(object, endCommand({ kind: "centralId", sessionId: first.sessionId }));
    const ended = await command(object, { kind: "facts", sessionId: first.sessionId });
    const missing = await command(object, { kind: "facts", sessionId: sessionId(999) });
    assert.deepEqual(ended.reply, { state: "unavailable" });
    assert.deepEqual(missing.reply, { state: "unavailable" });

    await command(
      object,
      { kind: "replaceIdleEnrollment", replacementId: sessionId(90) },
      { now: fixtureTime + 2 },
    );
    const endedDelegate = await command(object, { kind: "facts", sessionId: second.sessionId });
    assert.deepEqual(endedDelegate.reply, { state: "unavailable" });
    assert.ok(measuredRows(endedDelegate) <= 3, JSON.stringify(endedDelegate.reads));
    context.diagnostic("ended-enrollment delegate admission = " + measuredRows(endedDelegate));

    const admission = [measuredRows(live), measuredRows(ended), measuredRows(missing)] as const;
    context.diagnostic(
      `address found/missing = ${addressFound}/${addressMiss}; admission live/ended/missing = ${admission.join("/")}`,
    );
    context.diagnostic(`live delegate admission = ${delegateReads}`);
    assert.ok(delegateReads <= 5, JSON.stringify(liveDelegate.reads));
    assert.ok(addressFound <= 2 && addressMiss <= 1);
    assert.ok(
      admission[0] <= 5 && admission[1] <= 3 && admission[2] <= 1,
      JSON.stringify({ live: live.reads, ended: ended.reads, missing: missing.reads }),
    );
  });
}
