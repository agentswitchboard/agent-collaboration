import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";
import { commandPath, holder, raiseRequest, uuid } from "./workerd-approval-store-witness.ts";
import { centralTestBundle, WorkerdStorage } from "./workerd-storage.ts";

const applyingTime = Date.parse("2026-11-22T10:00:00.000Z");
const database = `${holder.organization}.authenticated-approval`;
const reservationSchema = z.object({
  result: z.literal("reserved"),
  sessionId: z.uuidv7(),
  secret: z.string(),
});
const sessionAnswerSchema = z.object({ reply: z.unknown() });
const approvalAnswerSchema = z.object({
  reply: z.object({ result: z.unknown(), offers: z.array(z.object({ requestId: z.string() })) }),
  committed: z.literal(true),
  writes: z.number().int(),
});

async function callSession(
  central: WorkerdStorage,
  command: Record<string, unknown>,
): Promise<unknown> {
  const response = await fetch(`${central.origin}/test-support/storage/${database}/sessions`, {
    method: "POST",
    headers: { "x-case-migrator": JSON.stringify("sessions") },
    body: JSON.stringify({
      machineId: holder.machine,
      enrollmentId: holder.enrollment,
      nowMs: applyingTime,
      command,
    }),
  });
  assert.equal(response.status, 200, await response.clone().text());
  return sessionAnswerSchema.parse(await response.json()).reply;
}

async function createBoundSession(
  central: WorkerdStorage,
  ordinal: number,
  parent?: { readonly sessionId: string; readonly secret: string },
): Promise<z.infer<typeof reservationSchema>> {
  const reserved = reservationSchema.parse(
    await callSession(central, {
      kind: "create",
      request: {
        operationId: uuid(6000 + ordinal),
        attempt: 1,
        agentProgram: "codex",
        creator:
          parent === undefined
            ? { kind: "routerOperator" }
            : {
                kind: "session",
                sessionId: parent.sessionId,
                presented: { kind: "secret", secret: parent.secret },
              },
        sessionKind:
          parent === undefined
            ? { kind: "ordinary" }
            : { kind: "delegate", parent: parent.sessionId },
      },
      candidateId: uuid(6100 + ordinal),
      secretSeed: ordinal,
    }),
  );
  await callSession(central, {
    kind: "bind",
    request: {
      sessionId: reserved.sessionId,
      harnessSessionId: uuid(6200 + ordinal),
      secret: reserved.secret,
    },
  });
  return reserved;
}

async function callApproval(
  central: WorkerdStorage,
  command: Record<string, unknown>,
): Promise<z.infer<typeof approvalAnswerSchema>> {
  return approvalAnswerSchema.parse(
    await central.dispatchJson(commandPath(database), {
      atMs: applyingTime,
      command: { kind: "authenticated", holder, operation: command },
    }),
  );
}

test("Workerd authenticates the exact live parent before applying a session's approval", async (): Promise<void> => {
  const central = await WorkerdStorage.start(centralTestBundle());
  try {
    await callSession(central, { kind: "seed", unrelatedSessions: 0 });
    const parent = await createBoundSession(central, 1);
    const child = await createBoundSession(central, 2, parent);
    await central.dispatchJson(commandPath(database), {
      atMs: applyingTime,
      command: { kind: "storeEnrollment", holder, live: true },
    });
    assert.deepEqual((await callApproval(central, { kind: "registerRun" })).reply.result, {
      kind: "registered",
    });
    const request = raiseRequest(6301, "Session approval", child.sessionId);
    const raised = await callApproval(central, { kind: "raise", request });
    assert.equal(
      z.object({ kind: z.literal("receipt") }).parse(raised.reply.result).kind,
      "receipt",
    );

    const pass = {
      kind: "passOn",
      sessionId: parent.sessionId,
      request: { requestId: request.requestId, operationId: uuid(6400) },
    };
    const missing = await callApproval(central, { ...pass, presented: { kind: "nothing" } });
    assert.deepEqual(missing.reply.result, { kind: "notAdmitted" });
    assert.equal(missing.writes, 0);
    const wrong = await callApproval(central, {
      ...pass,
      presented: { kind: "secret", secret: child.secret },
    });
    assert.deepEqual(wrong.reply.result, { kind: "notAdmitted" });
    assert.equal(wrong.writes, 0);
    const forwarded = await callApproval(central, {
      ...pass,
      presented: { kind: "secret", secret: parent.secret },
    });
    assert.deepEqual(forwarded.reply.result, { kind: "passedOn", to: { kind: "people" } });
    const replay = await callApproval(central, {
      ...pass,
      presented: { kind: "secret", secret: parent.secret },
    });
    assert.deepEqual(replay.reply.result, forwarded.reply.result);
    assert.equal(replay.writes, 0);

    const decide = {
      kind: "decide",
      request: {
        requestId: request.requestId,
        operationId: uuid(6401),
        value: { kind: "approval", optionId: 2 },
      },
    };
    const unrelated = await callApproval(central, {
      ...decide,
      sessionId: child.sessionId,
      presented: { kind: "secret", secret: child.secret },
    });
    assert.deepEqual(unrelated.reply.result, { kind: "notVisible" });
    assert.equal(unrelated.writes, 0);
    const accepted = await callApproval(central, {
      ...decide,
      sessionId: parent.sessionId,
      presented: { kind: "secret", secret: parent.secret },
    });
    assert.deepEqual(accepted.reply.result, {
      kind: "savedDecision",
      decision: {
        answer: { kind: "approval", optionId: 2 },
        by: { kind: "session", session: parent.sessionId },
      },
    });
    assert.deepEqual(
      accepted.reply.offers.map((offer) => offer.requestId),
      [request.requestId],
    );
  } finally {
    await central.close();
  }
});
