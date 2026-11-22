// A baseline that spans several result-limited reads: its events are the baseline, not
// post-baseline history, and the cursor chain across all reads is still checked.
import assert from "node:assert/strict";
import { test } from "node:test";
import { boundedObservationResultSchema, type SessionRef } from "./observation-models.ts";
import { foldObservationReads, type ObservationRead } from "./observation-ledger.ts";

const TARGET: SessionRef = {
  endpoint: { serviceId: "svc-1", endpointId: "cursor-local" },
  sessionId: "session-1",
};
const event = (sequence: number): unknown => ({
  sequence: BigInt(sequence),
  event: { kind: "stateChanged", state: { kind: "idle" } },
});

function read(
  after: number | null,
  sequences: readonly number[],
  limited: boolean,
): ObservationRead {
  return {
    request: {
      epoch: after === null ? null : 4n,
      afterSequence: after === null ? null : BigInt(after),
      serviceEpoch: after === null ? null : "e",
    },
    result: boundedObservationResultSchema.parse({
      target: TARGET,
      generation: { serviceEpoch: "e" },
      attached: true,
      events: sequences.map(event),
      endReason: limited ? "resultLimitReached" : "deadlineReached",
      continuationGap: true,
      epoch: 4n,
    }),
    notifications: [],
  };
}

test("a baseline of several reads is excluded from post-baseline history and kept as baseline events", () => {
  const reads = [read(null, [1, 2], true), read(2, [3, 4], false), read(4, [5], false)];
  const ledger = foldObservationReads(TARGET, reads, 2);
  assert.equal(ledger.kind, "continuous");
  if (ledger.kind !== "continuous") return;
  assert.deepEqual(
    ledger.baselineEvents.map((numbered) => numbered.sequence),
    [1n, 2n, 3n, 4n],
  );
  assert.deepEqual(
    ledger.events.map((numbered) => numbered.sequence),
    [5n],
  );
  assert.equal(ledger.baselineSequence, 4n);
});

test("the default single-read baseline is unchanged, and a broken chain inside the baseline is refused", () => {
  const ledger = foldObservationReads(TARGET, [read(null, [1, 2], false), read(2, [3], false)]);
  assert.equal(ledger.kind, "continuous");
  if (ledger.kind === "continuous") {
    assert.deepEqual(
      ledger.events.map((numbered) => numbered.sequence),
      [3n],
    );
    assert.deepEqual(
      ledger.baselineEvents.map((numbered) => numbered.sequence),
      [1n, 2n],
    );
  }
  const skipped = foldObservationReads(TARGET, [read(null, [1, 2], true), read(3, [4], false)], 2);
  assert.equal(skipped.kind, "rejected");
});

// Rejects dropping a marker-only cursor, accepting duplicate cuts, or trusting a wrong outer cursor.
test("a quiet snapshot initializes the next position and its cursor must agree on both surfaces", () => {
  const baseline = read(null, [], false);
  const cursor = { epoch: 4n, afterSequence: 0n };
  const marker = { kind: "snapshotEnd", cursor } as const;
  const quiet = {
    ...baseline,
    result: { ...baseline.result, events: [marker] },
    notifications: [{ event: marker, cursor }],
  };
  const recent = foldObservationReads(TARGET, [quiet, read(0, [1], false)]);
  assert.equal(recent.kind, "continuous");
  if (recent.kind === "continuous") {
    assert.equal(recent.baselineSequence, 0n);
    assert.equal(recent.lastSequence, 1n);
  }
  assert.equal(
    foldObservationReads(TARGET, [
      { ...quiet, result: { ...quiet.result, events: [marker, marker] } },
    ]).kind,
    "rejected",
  );
  assert.equal(
    foldObservationReads(TARGET, [{ ...quiet, notifications: [{ event: marker, cursor: null }] }])
      .kind,
    "rejected",
  );
});

test("a matching stream epoch and overlapping sequences cannot splice two Host runs", () => {
  const baseline = read(null, [1, 2], false);
  const prior = {
    ...baseline,
    result: { ...baseline.result, generation: { serviceEpoch: "run-a" } },
  };
  const later = read(2, [3], false);
  const guarded = {
    ...later,
    request: { ...later.request, serviceEpoch: "run-a" },
    result: { ...later.result, generation: { serviceEpoch: "run-b" } },
  };
  assert.deepEqual(foldObservationReads(TARGET, [prior, guarded]), {
    kind: "rejected",
    read: 1,
    reason: "the Host run changed without an admission refusal",
  });
  const gap = { kind: "resyncRequired" } as const;
  const refusal = {
    ...guarded,
    result: { ...guarded.result, endReason: "resyncRequired" as const, events: [gap] },
    notifications: [{ event: gap, cursor: null }],
  };
  assert.deepEqual(foldObservationReads(TARGET, [prior, refusal]), {
    kind: "resyncRequired",
    read: 1,
  });
  assert.deepEqual(
    foldObservationReads(TARGET, [
      prior,
      {
        ...refusal,
        result: { ...refusal.result, epoch: 5n },
        notifications: [{ event: gap, cursor: { epoch: 5n, afterSequence: 2n } }],
      },
    ]),
    { kind: "rejected", read: 1, reason: "notification 0 carries the wrong cursor" },
  );
});
