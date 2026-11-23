import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ApprovalWorkerdHost,
  appliedSchema,
  assertRollbackAtEachWrite,
  commandPath,
  committedResult,
  dueSchema,
  holder,
  hourMs,
  initializeApprovals,
  invokeApprovalWitness,
  personDecision,
  personId,
  raiseRequest,
  typedResult,
  unrelatedSession,
  uuid,
} from "./workerd-approval-store-witness.ts";

test("Workerd persists an admitted retained refusal, conflict, or reoffer deadline before replying", async (): Promise<void> => {
  const central = await ApprovalWorkerdHost.start();
  try {
    for (const observation of ["refusal", "conflict", "reoffer"]) {
      const database = `approval-observed-${observation}`;
      await initializeApprovals(central, database);
      const request = raiseRequest(1, "Observed expiry");
      assert.equal(
        (
          await invokeApprovalWitness({
            central,
            database,
            command: { kind: "raise", holder, request },
          })
        ).writes,
        3,
      );
      const invalid = personDecision(request.requestId, uuid(5701), 15);
      const refused = await invokeApprovalWitness({ central, database, atMs: 1, command: invalid });
      assert.deepEqual(typedResult(refused), { kind: "invalid" });
      const command =
        observation === "refusal"
          ? invalid
          : observation === "conflict"
            ? personDecision(request.requestId, uuid(5701), 1)
            : { kind: "reoffer", holder, requestId: request.requestId };
      const timedOut = await assertRollbackAtEachWrite({
        central,
        database,
        command,
        atMs: hourMs,
        writes: 2,
      });
      assert.deepEqual(
        typedResult(timedOut),
        observation === "refusal"
          ? { kind: "invalid" }
          : observation === "conflict"
            ? { kind: "operationConflict" }
            : null,
      );
      const received = appliedSchema.parse(committedResult(timedOut));
      assert.deepEqual(
        received.offers.map((value) => value.requestId),
        [request.requestId],
      );
      await central.reopen();
      const due = dueSchema.parse(
        await central.dispatchJson(commandPath(database), {
          atMs: hourMs,
          command: { kind: "dueRows", cutoffMs: hourMs, limit: 10 },
        }),
      );
      assert.deepEqual(due.rows, []);
      const beforeDeadline = await invokeApprovalWitness({
        central,
        database,
        atMs: hourMs - 1,
        command: personDecision(request.requestId, uuid(5702), 1),
      });
      assert.deepEqual(typedResult(beforeDeadline), { kind: "tooLate" });
      assert.equal(beforeDeadline.writes, 2);
    }
  } finally {
    await central.close();
  }
});

test("Workerd suppresses corruption of a hidden target for Read, Decide and PassOn", async (): Promise<void> => {
  const central = await ApprovalWorkerdHost.start();
  const database = "approval-hidden-row";
  try {
    await initializeApprovals(central, database, true);
    const request = raiseRequest(1, "Parent only");
    const personRead = {
      kind: "read",
      organization: holder.organization,
      person: personId,
      requestId: request.requestId,
    };
    const personDecide = personDecision(request.requestId, uuid(5703), 1);
    const foreignSessionDecide = {
      ...personDecide,
      kind: "decide",
      actor: { kind: "session", session: unrelatedSession },
    };
    const foreignPassOn = {
      kind: "passOn",
      organization: holder.organization,
      parent: unrelatedSession,
      requestId: request.requestId,
    };
    for (const state of ["missing", "healthy", "corrupt"]) {
      if (state === "healthy") {
        await invokeApprovalWitness({
          central,
          database,
          command: { kind: "raise", holder, request },
        });
      }
      if (state === "corrupt") {
        committedResult(
          await invokeApprovalWitness({
            central,
            database,
            command: { kind: "corrupt", requestId: request.requestId, field: "recordState" },
          }),
        );
      }
      for (const command of [personRead, personDecide, foreignSessionDecide, foreignPassOn]) {
        const hidden = await invokeApprovalWitness({ central, database, command });
        assert.deepEqual(
          typedResult(hidden),
          command.kind === "read" ? null : { kind: "notVisible" },
        );
        assert.equal(hidden.writes, 0);
      }
    }
  } finally {
    await central.close();
  }
});

test("Workerd recovers Owed endings after reopen only for the exact admitted run", async (): Promise<void> => {
  const central = await ApprovalWorkerdHost.start();
  const database = "approval-recover-owed";
  try {
    await initializeApprovals(central, database);
    const request = raiseRequest(1, "Recover ending");
    await invokeApprovalWitness({ central, database, command: { kind: "raise", holder, request } });
    await invokeApprovalWitness({
      central,
      database,
      command: personDecision(request.requestId, uuid(5704), 2),
    });
    await central.reopen();
    const admitted = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "recover", holder },
    });
    assert.deepEqual(typedResult(admitted), { Ok: null });
    const offers = appliedSchema.parse(committedResult(admitted)).offers;
    assert.deepEqual(
      offers.map((offer) => offer.requestId),
      [request.requestId],
    );
    assert.equal(admitted.writes, 0);

    for (const wrong of [
      { ...holder, runId: uuid(5705) },
      { ...holder, organization: uuid(5706) },
    ]) {
      const refused = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "recover", holder: wrong },
      });
      assert.deepEqual(typedResult(refused), { Err: { kind: "runNotRegistered" } });
      assert.deepEqual(appliedSchema.parse(committedResult(refused)).offers, []);
      assert.equal(refused.writes, 0);
    }
  } finally {
    await central.close();
  }
});
