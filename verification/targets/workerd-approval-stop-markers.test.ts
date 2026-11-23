import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { z } from "zod";
import type { Raise_Serialize } from "../../crates/approval-request-protocol/contracts/approval-requests.ts";
import {
  ApprovalWorkerdHost,
  committedResult,
  holder,
  initializeApprovals,
  invokeApprovalWitness,
  raiseRequest,
  requestingSession,
  snapshot,
  stopRequest,
  typedResult,
  uuid,
} from "./workerd-approval-store-witness.ts";

test("Workerd reopens both local-refusal marker families and rejects invalid stored pairs", async (): Promise<void> => {
  const central = await ApprovalWorkerdHost.start();
  const database = "approval-stop-marker-families";
  const providerRaise = raiseRequest(1, "Provider request");
  const parkedRaise: Raise_Serialize = {
    kind: "unprovenCall",
    requestId: uuid(2),
    holder: { routerRun: holder.runId },
    invocation: createHash("sha256").update("held end request").digest("base64url"),
    offer: {
      operation: { kind: "endSessionForGood", sessionId: requestingSession.sessionId },
    },
  };
  const markerCases = [
    {
      raise: providerRaise,
      family: "provider",
      otherFamily: "unprovenCall",
      corrupt: "providerMarkerWithCallCause",
    },
    {
      raise: parkedRaise,
      family: "unproven_call",
      otherFamily: "provider",
      corrupt: "callMarkerWithProviderCause",
    },
  ] as const;
  const standing = {
    kind: "stands",
    ending: { kind: "cancelled", reason: { kind: "stopped", cause: "holderRefusedLocally" } },
  };
  try {
    await initializeApprovals(central, database);
    for (const { raise } of markerCases) {
      const request = { ...stopRequest(raise), cause: "holderRefusedLocally" };
      const result = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "stop", holder, request },
      });
      assert.deepEqual(typedResult(result), standing);
      assert.equal(result.writes, 1);
    }
    await central.reopen();
    const storedRows = (await snapshot(central, database))
      .map((row) => z.array(z.unknown()).parse(JSON.parse(row)))
      .filter((row) => row[0] === "request");
    for (const markerCase of markerCases) {
      const marker = storedRows.find((row) => row[2] === markerCase.raise.requestId);
      assert.deepEqual(marker?.slice(-3, -1), [markerCase.family, "holder_refused_locally"]);
      const request = { ...stopRequest(markerCase.raise), cause: "holderRefusedLocally" };
      const crossFamily = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "stop", holder, request: { ...request, kind: markerCase.otherFamily } },
      });
      assert.deepEqual(typedResult(crossFamily), { kind: "notVisible" });
      assert.equal(crossFamily.writes, 0);

      const replay = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "raise", holder, request: markerCase.raise },
      });
      assert.deepEqual(typedResult(replay), { kind: "alreadyStopped" });
      assert.equal(replay.writes, 0);

      committedResult(
        await invokeApprovalWitness({
          central,
          database,
          command: { kind: "corrupt", requestId: request.requestId, field: markerCase.corrupt },
        }),
      );
      const rejected = await invokeApprovalWitness({
        central,
        database,
        command: { kind: "stop", holder, request },
      });
      assert.match(rejected.error ?? "", /Corrupt.*approval_requests\.stop_cause/);
      assert.equal(rejected.committed, false);
      assert.equal(rejected.writes, 0);
    }
  } finally {
    await central.close();
  }
});
