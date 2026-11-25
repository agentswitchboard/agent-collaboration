import assert from "node:assert/strict";
import test from "node:test";
import { StagingMessageEvents } from "./staging-message-events.ts";

const target = {
  endpoint: { serviceId: "018f0000-0000-7000-8000-000000000001", endpointId: "claude-local" },
  sessionId: "fixture-session",
};
const generation = { serviceEpoch: "fixture-epoch", generation: 1 };

function observer(): StagingMessageEvents {
  const view = new StagingMessageEvents(target);
  view.ingest(JSON.stringify({ kind: "listenerReady", target, generation }));
  wrap(view, { kind: "snapshotEnd", cursor: { epoch: 1, afterSequence: 8 } });
  return view;
}

function wrap(view: StagingMessageEvents, message: unknown): void {
  view.ingest(JSON.stringify({ kind: "providerSessionEvent", target, generation, message }));
}

test("captures the exact notice under the live turn without counting item updates", () => {
  const view = observer();
  const item = {
    itemId: "notice-item",
    kind: { kind: "userMessage" },
    text: "You have 1 new message. Read them with message_inbox.",
  };
  wrap(view, { sequence: 9, event: { kind: "turnStarted", turnId: "live-turn" } });
  wrap(view, { sequence: 10, event: { kind: "itemStarted", item } });
  wrap(view, { sequence: 11, event: { kind: "itemUpdated", item } });
  wrap(view, { sequence: 12, event: { kind: "turnEnded", turnId: "live-turn" } });
  assert.deepEqual(view.notices, [{ turnId: "live-turn", itemId: "notice-item", sequence: 10 }]);
  assert.deepEqual(view.ends, ["live-turn"]);
});

test("allows coalesced item sequences and fails on an explicit stream gap", () => {
  const view = observer();
  wrap(view, { sequence: 10, event: { kind: "turnStarted", turnId: "coalesced-turn" } });
  wrap(view, { kind: "snapshotEnd", cursor: { epoch: 1, afterSequence: 10 } });
  assert.equal(view.activeTurn, "coalesced-turn");
  assert.throws(
    () => wrap(view, { sequence: 10, event: { kind: "turnStarted", turnId: "replayed-turn" } }),
    /reordered or replayed/u,
  );
  assert.throws(
    () => wrap(view, { sequence: 11, event: { kind: "resyncRequired" } }),
    /contiguous coverage/u,
  );
});
