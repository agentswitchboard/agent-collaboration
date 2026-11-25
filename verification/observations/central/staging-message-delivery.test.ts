import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { ObservationFailure } from "../../receipt.ts";
import { checkDelivery, sendCheckpoint, type DeliveryCheck } from "./staging-message-delivery.ts";
import { StagingMessageEvents } from "./staging-message-events.ts";

const target = {
  endpoint: { serviceId: "018f0000-0000-7000-8000-000000000001", endpointId: "claude-local" },
  sessionId: "fixture-session",
};
const generation = { serviceEpoch: "fixture-epoch", generation: 1 };
const noticeMs = Date.parse("2026-01-01T12:00:00Z");
const messageId = "018f0000-0000-7000-8000-000000000009";
const status = {
  exit: 0,
  output: {
    kind: "returned",
    value: {
      kind: "answered",
      messageId,
      delivery: {
        kind: "checked",
        value: { kind: "seen", completedAt: new Date(noticeMs).toISOString() },
      },
    },
  },
};

function initialView(): StagingMessageEvents {
  const view = new StagingMessageEvents(target);
  view.ingest(JSON.stringify({ kind: "listenerReady", target, generation }));
  return view;
}

function wrap(view: StagingMessageEvents, message: unknown): void {
  view.ingest(JSON.stringify({ kind: "providerSessionEvent", target, generation, message }));
}

function recordNotice(view: StagingMessageEvents, sequence: number, time: number = noticeMs): void {
  const hex = time.toString(16).padStart(12, "0");
  wrap(view, {
    sequence,
    event: {
      kind: "itemStarted",
      item: {
        itemId: `${hex.slice(0, 8)}-${hex.slice(8)}-7000-8000-000000000000`,
        kind: { kind: "userMessage" },
        text: "You have 1 new message. Read them with message_inbox.",
      },
    },
  });
}

function fixture(running: boolean = false) {
  const view = initialView();
  wrap(view, { kind: "snapshotEnd", cursor: { epoch: 1, afterSequence: 1 } });
  if (running) wrap(view, { sequence: 2, event: { kind: "turnStarted", turnId: "turn" } });
  const records: Array<{ kind: string; value: unknown }> = [];
  let elapsed = 0;
  let onPoll: () => void = () => {};
  const check: DeliveryCheck = {
    view,
    receipt: {
      record: async (kind, value) => {
        records.push({ kind, value });
      },
    },
    requestId: "fixture-request",
    checkpoint: sendCheckpoint(view),
    answeredId: undefined,
    runningTurn: running ? "turn" : undefined,
    startCount: view.starts.length,
    seenWindowMs: 125_000,
    assertStream: () => {},
    readStatus: async () => status,
    clock: {
      monotonic: () => elapsed,
      utc: () => noticeMs + 10_000 + elapsed,
      poll: async () => {
        elapsed += 2_500;
        onPoll();
      },
    },
  };
  if (!running) wrap(view, { sequence: 2, event: { kind: "turnStarted", turnId: "turn" } });
  recordNotice(view, 3);
  return {
    view,
    records,
    check,
    whenPolled: (action: () => void): void => {
      onPoll = action;
    },
  };
}

async function mustFail(t: TestContext, check: DeliveryCheck, expected: RegExp): Promise<void> {
  await assert.rejects(checkDelivery(check), (error: unknown) => {
    assert.ok(error instanceof ObservationFailure);
    assert.match(error.message, expected);
    t.diagnostic(JSON.stringify({ verdict: "failed", reason: error.message }));
    return true;
  });
}

test("pre-send notice fails both running and idle delivery", async (t) => {
  for (const running of [false, true]) {
    const { view, check, records } = fixture(running);
    assert.throws(() => sendCheckpoint(view), /predates the send checkpoint/u);
    await mustFail(t, { ...check, checkpoint: view.checkpoint() }, /predates the send checkpoint/u);
    assert.equal(
      records.some(({ kind }) => kind === "pass"),
      false,
    );
  }
  const history = initialView();
  recordNotice(history, 1);
  wrap(history, { kind: "snapshotEnd", cursor: { epoch: 1, afterSequence: 1 } });
  assert.throws(() => sendCheckpoint(history), /predates the send checkpoint/u);
});

test("observation lost during final sender status read fails", async (t) => {
  const { check, records } = fixture();
  let disconnected = false;
  await mustFail(
    t,
    {
      ...check,
      readStatus: async () => {
        await Promise.resolve();
        disconnected = true;
        return status;
      },
      assertStream: () => {
        if (disconnected) throw new Error("synthetic recipient disconnected");
      },
    },
    /recipient disconnected/u,
  );
  assert.deepEqual(records, [
    { kind: "continuity", value: { passed: false, reason: "synthetic recipient disconnected" } },
  ]);
});

test("a second notice inside the duplicate window fails", async (t) => {
  const { check, view, records, whenPolled } = fixture();
  whenPolled(() => recordNotice(view, 4, noticeMs + 2_500));
  await mustFail(t, check, /received multiple notices/u);
  assert.equal(
    records.some(({ kind }) => kind === "duplicate-window"),
    true,
  );
  assert.equal(
    records.some(({ kind }) => kind === "pass"),
    false,
  );
});

test("requires matching notice and sender times and an unchanged Answered identity", async (t) => {
  const { check } = fixture(true);
  await mustFail(
    t,
    { ...check, answeredId: target.endpoint.serviceId },
    /inconsistent Central message identities/u,
  );
  await mustFail(
    t,
    {
      ...check,
      readStatus: async () => ({
        ...status,
        output: {
          ...status.output,
          value: {
            ...status.output.value,
            delivery: {
              kind: "checked",
              value: { kind: "seen", completedAt: new Date(noticeMs + 6).toISOString() },
            },
          },
        },
      }),
    },
    /notice time does not match sender Seen/u,
  );
});

test("commits a matching Seen only after continuous coverage through the next mark", async () => {
  const { check, view, records } = fixture(true);
  await checkDelivery(check);
  assert.equal(check.clock.monotonic(), 55_000);
  assert.equal(view.notices[0]?.turnId, "turn");
  assert.deepEqual(
    records.map(({ kind }) => kind),
    ["sender-status", "duplicate-window", "pass"],
  );
});
