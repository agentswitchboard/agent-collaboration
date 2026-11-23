import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";
import {
  ApprovalWorkerdHost,
  assertRollbackAtEachWrite,
  committedResult,
  holder,
  hourMs,
  initializeApprovals,
  invokeApprovalWitness,
  personDecision,
  raiseRequest,
  snapshot,
  typedResult,
  uuid,
} from "./workerd-approval-store-witness.ts";

test("Workerd Decide replays accepted and refused answers through the shared 24-hour log", async (): Promise<void> => {
  const central = await ApprovalWorkerdHost.start();
  const database = "approval-request-log";
  try {
    await initializeApprovals(central, database);
    const request = raiseRequest(1, "Logged decision");
    await invokeApprovalWitness({ central, database, command: { kind: "raise", holder, request } });
    const invalid = personDecision(request.requestId, uuid(5601), 15);
    const chose = personDecision(request.requestId, uuid(5602), 2);
    const refusal = await assertRollbackAtEachWrite({
      central,
      database,
      atMs: 1,
      command: invalid,
      writes: 2,
    });
    assert.deepEqual(typedResult(refusal), { kind: "invalid" });
    const accepted = await assertRollbackAtEachWrite({
      central,
      database,
      atMs: 1,
      command: chose,
      writes: 4,
    });
    assert.equal(z.object({ kind: z.string() }).parse(typedResult(accepted)).kind, "savedDecision");
    const logRows = (await snapshot(central, database))
      .map((row) => z.array(z.unknown()).parse(JSON.parse(row)))
      .filter((row) => row[0] === "requestLog");
    assert.deepEqual(
      logRows.map((row) => row[1]),
      [uuid(5601), uuid(5602)],
    );

    const withinWindow = 24 * hourMs;
    for (const { command, firstReply } of [
      { command: invalid, firstReply: refusal },
      { command: chose, firstReply: accepted },
    ]) {
      const replay = await invokeApprovalWitness({
        central,
        database,
        atMs: withinWindow,
        command,
      });
      assert.deepEqual(typedResult(replay), typedResult(firstReply));
      assert.equal(replay.writes, 0);
    }

    const conflict = await invokeApprovalWitness({
      central,
      database,
      atMs: withinWindow,
      command: personDecision(request.requestId, uuid(5602), 1),
    });
    assert.deepEqual(typedResult(conflict), { kind: "operationConflict" });
    assert.equal(conflict.writes, 0);
    const expired = await invokeApprovalWitness({
      central,
      database,
      atMs: withinWindow + 1,
      command: chose,
    });
    assert.equal(z.object({ kind: z.string() }).parse(typedResult(expired)).kind, "standing");
    assert.equal(expired.writes, 2);
  } finally {
    await central.close();
  }
});
