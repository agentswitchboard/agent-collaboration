import assert from "node:assert/strict";
import { rm } from "node:fs/promises";
import { test } from "node:test";
import { z } from "zod";
import { createFixtureDirectory } from "../test-support/fixture-directory.ts";
import { centralTestBundle, WorkerdStorage } from "./workerd-storage.ts";
import {
  appliedSchema,
  dueSchema,
  hourMs,
  uuid,
  holder,
  requestingSession,
  parentSession,
  unrelatedSession,
  personId,
  raiseRequest,
  stopRequest,
  commandPath,
  invokeApprovalWitness,
  committedResult,
  typedResult,
  initializeApprovals,
  snapshot,
  personDecision,
  assertRollbackAtEachWrite,
} from "./workerd-approval-store-witness.ts";

test("Workerd approvals reopen with exact receipts, real parent routing and one accepted decision", async (): Promise<void> => {
  const persistenceDirectory = createFixtureDirectory("workerd-approval-store");
  const database = "approval-reopen";
  let central = await WorkerdStorage.start(centralTestBundle(), undefined, undefined, {
    persistenceDirectory,
  });
  try {
    await initializeApprovals(central, database, true);
    const request = raiseRequest(1, "Original");
    const unknown = raiseRequest(1, "Unknown", uuid(5999));
    const missing = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "raise", holder, request: unknown },
    });
    assert.deepEqual(typedResult(missing), { kind: "sessionEnded" });
    assert.equal(missing.writes, 0);

    const first = await assertRollbackAtEachWrite({
      central,
      database,
      command: { kind: "raise", holder, request },
      writes: 3,
    });
    assert.equal(first.rowsChanged, 3);
    const firstReceipt = typedResult(first);
    assert.equal(z.object({ kind: z.string() }).parse(firstReceipt).kind, "receipt");
    await central.close();
    central = await WorkerdStorage.start(centralTestBundle(), undefined, undefined, {
      persistenceDirectory,
    });

    const rows = await snapshot(central, database);
    const replay = await invokeApprovalWitness({
      central,
      database,
      atMs: 1,
      command: { kind: "raise", holder, request },
    });
    assert.deepEqual(typedResult(replay), firstReceipt);
    assert.equal(replay.writes, 0);

    const mismatch = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "raise", holder, request: raiseRequest(1, "Changed") },
    });
    assert.deepEqual(typedResult(mismatch), { kind: "contentsDiffer" });
    assert.equal(mismatch.writes, 0);
    const otherHolder = {
      ...holder,
      machine: uuid(5301),
      enrollment: uuid(5302),
      runId: uuid(5303),
    };
    committedResult(
      await invokeApprovalWitness({
        central,
        database,
        command: { kind: "storeEnrollment", holder: otherHolder, live: true },
      }),
    );
    committedResult(
      await invokeApprovalWitness({
        central,
        database,
        command: { kind: "register", holder: otherHolder },
      }),
    );
    const foreignRequest = {
      ...request,
      holder: { routerRun: otherHolder.runId, sessionId: uuid(5304) },
    };
    const foreign = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "raise", holder: otherHolder, request: foreignRequest },
    });
    assert.deepEqual(typedResult(foreign), { kind: "contentsDiffer" });
    assert.equal(foreign.writes, 0);

    const destination = rows
      .map((row) => z.array(z.unknown()).parse(JSON.parse(row)))
      .find((row) => row[0] === "destination");
    assert.equal(destination?.[4], parentSession.sessionId);
    const parentDecision = (session: typeof parentSession): Record<string, unknown> => ({
      ...personDecision(request.requestId, uuid(5305), 1),
      actor: { kind: "session", session },
    });
    const wrongParent = await invokeApprovalWitness({
      central,
      database,
      command: parentDecision(unrelatedSession),
    });
    assert.deepEqual(typedResult(wrongParent), { kind: "notVisible" });
    assert.equal(wrongParent.writes, 0);

    const passOn = {
      kind: "passOn",
      organization: holder.organization,
      parent: parentSession,
      requestId: request.requestId,
    };
    assert.deepEqual(
      typedResult(
        await assertRollbackAtEachWrite({ central, database, command: passOn, writes: 2 }),
      ),
      { kind: "forwarded" },
    );
    const chosen = personDecision(request.requestId, uuid(5306), 2);
    const race = await Promise.all([
      invokeApprovalWitness({ central, database, atMs: 1, command: parentDecision(parentSession) }),
      invokeApprovalWitness({ central, database, atMs: 1, command: chosen }),
    ]);
    const results = race.map(
      (reply) => z.object({ kind: z.string() }).parse(typedResult(reply)).kind,
    );
    assert.deepEqual(results.toSorted(), ["savedDecision", "standing"]);
    assert.equal(
      race.map((reply) => reply.writes).reduce((sum, writes) => sum + writes, 0),
      2,
    );
    const winner = results.indexOf("savedDecision");
    const won = race[winner];
    assert.ok(won);
    const replayDecision = await invokeApprovalWitness({
      central,
      database,
      atMs: 2,
      command: winner === 0 ? parentDecision(parentSession) : chosen,
    });
    assert.deepEqual(typedResult(replayDecision), typedResult(won));
    assert.equal(replayDecision.writes, 0);
    const offered = appliedSchema.parse(committedResult(won)).offers[0];
    assert.ok(offered);
    const delivery = await assertRollbackAtEachWrite({
      central,
      database,
      command: { kind: "deliver", holder, request: offered, reply: { kind: "received" } },
      writes: 1,
    });
    assert.equal(delivery.rowsChanged, 1);

    const marker = raiseRequest(2, "Stopped before Raise");
    const stop = { kind: "stop", holder, request: stopRequest(marker) };
    await assertRollbackAtEachWrite({ central, database, command: stop, writes: 1 });
    assert.deepEqual(
      typedResult(
        await invokeApprovalWitness({
          central,
          database,
          command: { kind: "raise", holder, request: marker },
        }),
      ),
      { kind: "alreadyStopped" },
    );
  } finally {
    await central.close();
    await rm(persistenceDirectory, { recursive: true, force: true });
  }
});

test("Workerd approvals time out only selected rows and seek the deadline index at scale", async (): Promise<void> => {
  const central = await WorkerdStorage.start(centralTestBundle());
  try {
    const database = "approval-deadlines";
    await initializeApprovals(central, database);
    for (const [ordinal, atMs] of [
      [1, 0],
      [2, 1],
      [3, 0],
    ]) {
      assert.ok(ordinal !== undefined && atMs !== undefined);
      const reply = await invokeApprovalWitness({
        central,
        database,
        atMs,
        command: { kind: "raise", holder, request: raiseRequest(ordinal, "Page") },
      });
      assert.equal(reply.writes, 3);
    }

    const dueBefore = dueSchema.parse(
      await central.dispatchJson(commandPath(database), {
        atMs: hourMs,
        command: { kind: "dueRows", cutoffMs: hourMs, limit: 10 },
      }),
    );
    assert.equal(dueBefore.rows.length, 2);
    const requestRows = (await snapshot(central, database))
      .map((row) => z.array(z.unknown()).parse(JSON.parse(row)))
      .filter((row) => row[0] === "request");
    const expiredRequests = dueBefore.rows.map((due) => {
      const row = requestRows.find((candidate) => candidate[1] === due.subject);
      return z.string().parse(row?.[2]);
    });
    const [firstExpired, secondExpired] = expiredRequests;
    assert.ok(firstExpired !== undefined && secondExpired !== undefined);
    assert.deepEqual(expiredRequests.toSorted(), [uuid(1), uuid(3)]);

    const page = { kind: "list", organization: holder.organization, person: personId, limit: 1 };
    const read = await assertRollbackAtEachWrite({
      central,
      database,
      atMs: hourMs,
      command: page,
      writes: 2,
    });
    const sent = appliedSchema.parse(committedResult(read));
    assert.deepEqual(
      sent.offers.map((offer) => offer.requestId),
      [firstExpired],
    );
    assert.equal(z.object({ hasNextPage: z.boolean() }).parse(sent.result).hasNextPage, true);

    const rollback = await invokeApprovalWitness({
      central,
      database,
      atMs: hourMs - 1,
      command: personDecision(firstExpired, uuid(5401), 1),
    });
    assert.deepEqual(typedResult(rollback), { kind: "tooLate" });
    assert.equal(rollback.writes, 0);
    const pending = dueSchema.parse(
      await central.dispatchJson(commandPath(database), {
        atMs: hourMs,
        command: { kind: "dueRows", cutoffMs: hourMs, limit: 10 },
      }),
    );
    assert.equal(pending.rows.length, 1);
    const due = pending.rows[0];
    assert.ok(due);
    const stale = await invokeApprovalWitness({
      central,
      database,
      atMs: hourMs,
      command: { kind: "deadline", subject: due.subject, deadlineMs: hourMs + 1 },
    });
    assert.deepEqual(committedResult(stale), []);
    assert.equal(stale.writes, 0);
    const alarm = await assertRollbackAtEachWrite({
      central,
      database,
      atMs: hourMs + 10,
      command: { kind: "deadline", subject: due.subject, deadlineMs: hourMs },
      writes: 2,
    });
    assert.deepEqual(z.array(z.object({ requestId: z.string() })).parse(committedResult(alarm)), [
      { requestId: secondExpired },
    ]);

    const readings: number[][] = [];
    for (const waiting of [20, 10_000]) {
      const indexed = `approval-due-${waiting}`;
      await initializeApprovals(central, indexed);
      await invokeApprovalWitness({
        central,
        database: indexed,
        command: { kind: "raise", holder, request: raiseRequest(1, "Indexed") },
      });
      const seeded = await invokeApprovalWitness({
        central,
        database: indexed,
        command: { kind: "seedDuePopulation", waiting },
      });
      assert.equal(committedResult(seeded), waiting);
      const results = dueSchema.parse(
        await central.dispatchJson(commandPath(indexed), {
          atMs: hourMs,
          command: { kind: "dueRows", cutoffMs: hourMs, limit: 1 },
        }),
      );
      assert.equal(results.rows.length, 1);
      assert.equal(results.rows[0]?.occurrenceMs, hourMs);
      console.log(`${waiting} waiting approvals, ${results.rowsRead.join(",")} Workerd rows read`);
      readings.push(results.rowsRead);
    }
    assert.deepEqual(readings, [[1], [1]]);
  } finally {
    await central.close();
  }
});

test("Workerd approvals bound retirement and refuse unknown stored kinds, states and endings", async (): Promise<void> => {
  const central = await WorkerdStorage.start(centralTestBundle());
  try {
    const database = "approval-retirement";
    await initializeApprovals(central, database);
    for (let ordinal = 1; ordinal <= 256; ordinal += 1) {
      const added = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "raise", holder, request: raiseRequest(ordinal, "Outstanding") },
      });
      assert.equal(added.writes, 3);
    }
    const limitRefusal = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "raise", holder, request: raiseRequest(257, "Overflow") },
    });
    assert.deepEqual(typedResult(limitRefusal), { kind: "tooManyOutstanding" });
    assert.equal(limitRefusal.writes, 0);
    const before = await snapshot(central, database);
    const retired = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "retire", holder },
    });
    assert.deepEqual(committedResult(retired), { Ok: null });
    assert.equal(retired.writes, 513);
    assert.equal(retired.rowsChanged, 513);
    const after = await snapshot(central, database);
    assert.equal(after.length, before.length - 256);
    const requests = after
      .map((row) => z.array(z.unknown()).parse(JSON.parse(row)))
      .filter((row) => row[0] === "request");
    assert.equal(requests.length, 256);
    assert.ok(requests.every((row) => row[3] === "ended" && row[8] === "closed_without_receipt"));
    console.log(
      "retirement at 256 requests committed 513 guarded row changes in one Workerd transaction",
    );

    for (const field of ["requestKind", "recordState", "ending"]) {
      const corruptDatabase = `approval-${field}`;
      await initializeApprovals(central, corruptDatabase);
      const request = raiseRequest(1, "Decode");
      await invokeApprovalWitness({
        central,
        database: corruptDatabase,
        command: { kind: "raise", holder, request },
      });
      if (field === "ending") {
        const decided = await invokeApprovalWitness({
          central,
          database: corruptDatabase,
          command: personDecision(request.requestId, uuid(5501), 1),
        });
        assert.equal(decided.writes, 2);
      }
      committedResult(
        await invokeApprovalWitness({
          central,
          database: corruptDatabase,
          command: { kind: "corrupt", requestId: request.requestId, field },
        }),
      );
      const rejected = await invokeApprovalWitness({
        central,
        database: corruptDatabase,
        command: {
          kind: "read",
          organization: holder.organization,
          person: personId,
          requestId: request.requestId,
        },
      });
      assert.match(rejected.error ?? "", /Corrupt/);
      assert.equal(rejected.writes, 0);
    }
  } finally {
    await central.close();
  }
});

test("Workerd approval endings and a run replacement roll back after every guarded write", async (): Promise<void> => {
  const central = await WorkerdStorage.start(centralTestBundle());
  const database = "approval-ending-rollback";
  try {
    await initializeApprovals(central, database);
    const stopped = raiseRequest(1, "Waiting stop");
    const decided = raiseRequest(2, "Decided");
    const sessionEnded = raiseRequest(3, "Session ended");
    const enrollmentEnded = raiseRequest(4, "Enrollment ended", unrelatedSession.sessionId);
    for (const request of [stopped, decided, sessionEnded, enrollmentEnded]) {
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
    }

    await assertRollbackAtEachWrite({
      central,
      database,
      writes: 2,
      command: { kind: "stop", holder, request: stopRequest(stopped) },
    });
    const accepted = await assertRollbackAtEachWrite({
      central,
      database,
      writes: 2,
      command: personDecision(decided.requestId, uuid(5502), 1),
    });
    const offered = appliedSchema.parse(committedResult(accepted)).offers[0];
    assert.ok(offered);
    await assertRollbackAtEachWrite({
      central,
      database,
      writes: 1,
      command: { kind: "deliver", holder, request: offered, reply: { kind: "notHeld" } },
    });

    committedResult(
      await invokeApprovalWitness({
        central,
        database,
        command: {
          kind: "storeSession",
          organization: holder.organization,
          session: requestingSession,
          parent: null,
          live: false,
        },
      }),
    );
    await assertRollbackAtEachWrite({
      central,
      database,
      writes: 2,
      command: {
        kind: "endSession",
        organization: holder.organization,
        session: requestingSession,
      },
    });

    const replacement = { ...holder, runId: uuid(5503) };
    await assertRollbackAtEachWrite({
      central,
      database,
      writes: 5,
      command: { kind: "register", holder: replacement },
    });
    const retired = await invokeApprovalWitness({
      central,
      database,
      command: { kind: "stop", holder, request: stopRequest(enrollmentEnded) },
    });
    assert.deepEqual(typedResult(retired), {
      kind: "authorityRefused",
      refusal: { kind: "runRetired" },
    });
    assert.equal(retired.writes, 0);

    const last = {
      ...raiseRequest(5, "Enrollment", unrelatedSession.sessionId),
      holder: { routerRun: replacement.runId, sessionId: unrelatedSession.sessionId },
    } satisfies ReturnType<typeof raiseRequest>;
    assert.equal(
      (
        await invokeApprovalWitness({
          central,
          database,
          command: { kind: "raise", holder: replacement, request: last },
        })
      ).writes,
      3,
    );
    committedResult(
      await invokeApprovalWitness({
        central,
        database,
        command: { kind: "storeEnrollment", holder: replacement, live: false },
      }),
    );
    await assertRollbackAtEachWrite({
      central,
      database,
      writes: 3,
      command: { kind: "endEnrollment", holder: replacement },
    });
    const due = dueSchema.parse(
      await central.dispatchJson(commandPath(database), {
        atMs: hourMs,
        command: { kind: "dueRows", cutoffMs: hourMs, limit: 10 },
      }),
    );
    assert.deepEqual(due.rows, []);
  } finally {
    await central.close();
  }
});
