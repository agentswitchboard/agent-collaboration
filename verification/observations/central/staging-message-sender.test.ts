import assert from "node:assert/strict";
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { qualifyRemoteSender, readRemoteReceipt } from "./staging-message-sender.ts";

const sender = {
  sessionId: "fixture-sender",
  endpoint: { endpointId: "codex-local", serviceId: "018f0000-0000-7000-8000-000000000002" },
};
const receiverMachine = "018f0000-0000-7000-8000-000000000003";
const machineId = "018f0000-0000-7000-8000-000000000004";
const requestId = "018f0000-0000-7000-8000-000000000005";
const source = "a".repeat(40);

test("requires a distinct qualified Mac and rejects a receipt for another request", async (t) => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), "staging-sender-test-")));
  t.after(async () => {
    await rm(directory, { recursive: true });
  });
  const handoff = { machineId, directory };
  await writeFile(
    join(directory, "sender-qualified.json"),
    JSON.stringify({
      source,
      machineId,
      sender,
      debugOnly: true,
      enrolledConnected: true,
      routerSha256: "a".repeat(64),
      cliSha256: "b".repeat(64),
    }),
    { mode: 0o600 },
  );
  assert.equal(
    (await qualifyRemoteSender(handoff, { source, receiverMachine, sender })).machineId,
    machineId,
  );
  await assert.rejects(
    qualifyRemoteSender(handoff, { source, receiverMachine: machineId, sender }),
    /distinct qualified staging Mac/u,
  );
  const read = { handoff, sender, requestId, operation: "send" } as const;
  assert.equal(await readRemoteReceipt(read), undefined);
  await writeFile(
    join(directory, `${requestId}-send.json`),
    JSON.stringify({
      requestId: receiverMachine,
      machineId,
      sender,
      observedAt: "2026-11-25T12:00:00Z",
      exit: 0,
      output: {},
    }),
    { mode: 0o600 },
  );
  await assert.rejects(readRemoteReceipt(read), /another request, machine, or session/u);
  const sendOutput = { kind: "returned", value: { kind: "inOutbox", requestId } };
  await writeFile(
    join(directory, `${requestId}-send.json`),
    JSON.stringify({
      requestId,
      machineId,
      sender,
      observedAt: "2026-11-25T12:00:00Z",
      exit: null,
      output: sendOutput,
    }),
  );
  const sendReceipt = await readRemoteReceipt(read);
  assert.equal(sendReceipt?.exit, null);
  assert.deepEqual(sendReceipt?.output, sendOutput);
});
