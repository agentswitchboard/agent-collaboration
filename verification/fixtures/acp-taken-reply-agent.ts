// Confirm an exact taken request, then hold its reply across a controlled end.
import assert from "node:assert/strict";
import { once } from "node:events";
import { closeSync } from "node:fs";
import { createConnection } from "node:net";
import { AcpExchange } from "./acp-exchange.ts";
import { requireArgument, wireJson } from "./acp-stdio.ts";
import { sendSocketNotice } from "./fixture-socket-notice.ts";
import { fileAppears, startReleaseBarrier } from "./release-barrier.ts";

const receipt = requireArgument(2);
const notices = requireArgument(3);
const targetMethod = requireArgument(4);
const agent = new AcpExchange();
startReleaseBarrier(receipt);

const configOptions = [
  {
    id: "mode",
    name: "Mode",
    category: "mode",
    type: "select",
    currentValue: "ask",
    options: [
      { value: "ask", name: "Ask" },
      { value: "plan", name: "Plan" },
    ],
  },
  {
    id: "fast",
    name: "Fast",
    category: "model_config",
    type: "boolean",
    currentValue: true,
  },
];

await agent.initialize("taken-reply-fixture", {
  agentCapabilities: {
    loadSession: true,
    sessionCapabilities: { close: {}, resume: {} },
  },
  _meta: { steering: { supported: true } },
});

let taken: Record<string, unknown>;
for (;;) {
  const request = await agent.read();
  if (request.method === targetMethod) {
    assert.notEqual(request.id, undefined);
    taken = request;
    break;
  }
  if (request.id === undefined) continue;
  switch (request.method) {
    case "session/new":
      agent.respond(request, { sessionId: "reactive-session-1", configOptions });
      break;
    case "session/load":
    case "session/resume":
    case "session/set_config_option":
      agent.respond(request, { configOptions });
      break;
    case "session/prompt":
      assert.equal(targetMethod, "_session/steering");
      await sendSocketNotice(notices, "warm-prompt\n");
      break;
    default:
      throw new Error(`unexpected preparation request ${String(request.method)}`);
  }
}

await sendSocketNotice(notices, `${wireJson({ method: taken.method, id: taken.id })}\n`);
const action = await Promise.race(
  [".child-exit", ".input-error", ".output-error", ".refuse", ".confirm"].map(
    async (suffix): Promise<string> => {
      await fileAppears(`${receipt}${suffix}`);
      return suffix;
    },
  ),
);

switch (action) {
  case ".child-exit":
    process.exit(0);
  case ".input-error":
    process.stdout.write(Uint8Array.of(0xff, 0x0a));
    break;
  case ".output-error": {
    // Keep the peer alive with stdout open while the Host's later write
    // hits the closed read end of the real stdin pipe.
    const keepalive = createConnection(notices);
    await once(keepalive, "connect");
    process.stdin.pause();
    closeSync(0);
    keepalive.write("stdin-closed\n");
    await once(keepalive, "end");
    process.exit(0);
  }
  case ".refuse":
    agent.send({
      jsonrpc: "2.0",
      id: taken.id,
      error: { code: -32601, message: "fixture refusal" },
    });
    break;
  case ".confirm": {
    assert.equal(targetMethod, "session/close");
    agent.respond(taken, {});
    const fence = "fixture-buffered-reply-fence";
    agent.send({ jsonrpc: "2.0", id: fence, method: "fixture/reply-fence", params: {} });
    const response = await agent.read();
    assert.equal(response.id, fence);
    await sendSocketNotice(notices, "reply-buffered\n");
    break;
  }
  default:
    throw new Error(`unexpected fixture action ${action}`);
}

await agent.drain();
