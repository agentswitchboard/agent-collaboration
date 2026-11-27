import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { decode, encode, field } from "../../../exact-json.ts";

const relay = join(import.meta.dirname, "acp-stdio-capture.ts");
const fixtures = join(import.meta.dirname, "../../../fixtures");

const initialize = {
  jsonrpc: "2.0",
  id: 1n,
  method: "initialize",
  params: { protocolVersion: 1n, clientCapabilities: { fs: { readTextFile: true } } },
};
const prompt = {
  jsonrpc: "2.0",
  id: 3n,
  method: "session/prompt",
  params: { sessionId: "fixture-session", prompt: [{ type: "text", text: "fixture only" }] },
};
const permissionAnswer = {
  jsonrpc: "2.0",
  id: 91n,
  result: { outcome: { outcome: "cancelled" } },
};
const rawOutput = {
  stdout: "café\n",
  stderr: "to-stderr\n",
  exitCode: 3n,
  extra: [{ nested: { count: 9007199254740993n, kept: false } }],
};

interface FixtureCase {
  readonly name: string;
  readonly args: readonly string[];
  readonly updates: readonly unknown[];
  readonly permission?: true;
}

const cases: readonly FixtureCase[] = [
  {
    name: "host-tool-output-agent.ts",
    args: [encode(rawOutput), "", "true"],
    updates: [
      {
        sessionUpdate: "tool_call",
        toolCallId: "call-1",
        title: "router-collaboration: endpoints_list",
        kind: "other",
      },
      {
        sessionUpdate: "tool_call_update",
        toolCallId: "call-1",
        name: "router-collaboration-endpoints_list",
        rawOutput,
      },
      {
        sessionUpdate: "tool_call_update",
        toolCallId: "call-1",
        name: "router-collaboration-endpoints_list",
        status: "completed",
      },
    ],
  },
  {
    name: "provider-streamed-items-agent.ts",
    args: [],
    updates: [
      {
        sessionUpdate: "tool_call",
        toolCallId: "tool-one",
        title: "Run check",
        kind: "execute",
        status: "pending",
      },
      {
        sessionUpdate: "tool_call_update",
        toolCallId: "tool-one",
        title: "Check passed",
        status: "completed",
      },
    ],
  },
  {
    name: "permission-offer-agent.ts",
    args: ["malformed"],
    updates: [],
    permission: true,
  },
];

function verifyFixtureReport(fixture: FixtureCase, capture: string): void {
  const report = spawnSync(
    process.execPath,
    [join(import.meta.dirname, "acp-tool-frame-report.ts"), "--cursor", capture],
    { encoding: "utf8", timeout: 5_000 },
  );
  assert.equal(report.status, 0, report.stderr);
  assert.match(report.stdout, /\| Command \| Unobserved \|/);
  const toolCount = fixture.updates.length + (fixture.permission === true ? 1 : 0);
  assert.match(report.stdout, new RegExp(`Tool frames: ${toolCount}\\.`));
  if (fixture.name === "host-tool-output-agent.ts") {
    assert.match(
      report.stdout,
      /\$\.params\.update\.rawOutput\.extra\[0\]\.nested \{"count", "kept"\}/,
    );
    assert.match(
      report.stdout,
      /\| Standard error \| Unobserved \| ` \$\.params\.update\.rawOutput\.stderr `/,
    );
  }
  if (fixture.permission === true) {
    assert.match(
      report.stdout,
      /\| Command \| Unobserved \| ` \$\.params\.toolCall\.rawInput\.command `/,
    );
    assert.doesNotMatch(report.stdout, /"echo safe"|"secret"|fixture-session/);
  }
}

for (const fixture of cases) {
  test(
    `records every frame through the stdio process for ${fixture.name}`,
    { timeout: 10_000 },
    async (t) => {
      const directory = realpathSync(mkdtempSync(join(tmpdir(), "acp-tool-capture-")));
      const receipt = join(directory, "reply.json");
      const child = spawn(process.execPath, [
        relay,
        directory,
        "--",
        process.execPath,
        join(fixtures, fixture.name),
        ...fixture.args,
        ...(fixture.permission === true ? [receipt] : []),
      ]);
      assert.ok(child.pid);
      const capture = join(directory, `acp-${child.pid}.jsonl`);
      const closed = once(child, "close");
      t.after(async (): Promise<void> => {
        child.kill();
        await closed;
        rmSync(directory, { recursive: true, force: true });
      });
      const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
      const reader = lines[Symbol.asyncIterator]();
      const crossed: [string, unknown][] = [];
      function send(message: unknown): void {
        crossed.push(["clientToServer", message]);
        child.stdin.write(`${encode(message)}\n`);
      }
      async function receive(): Promise<unknown> {
        const next = await reader.next();
        assert.equal(next.done, false, "relay closed before the expected frame");
        const message = decode(next.value);
        crossed.push(["serverToClient", message]);
        return message;
      }
      send(initialize);
      assert.equal(field(await receive(), "id"), 1n);
      send({
        jsonrpc: "2.0",
        id: 2n,
        method: "session/new",
        params: { cwd: directory, mcpServers: [] },
      });
      assert.equal(field(await receive(), "result", "sessionId"), "fixture-session");
      send(prompt);
      const updates: unknown[] = [];
      for (;;) {
        const frame = await receive();
        if (field(frame, "id") === 3n) break;
        if (field(frame, "method") === "session/request_permission") {
          assert.deepEqual(field(frame, "params", "toolCall", "rawInput"), {
            command: "echo safe",
            api_key: "secret",
          });
          send(permissionAnswer);
        }
        if (field(frame, "method") === "session/update") {
          const update = field(frame, "params", "update");
          if (["tool_call", "tool_call_update"].includes(String(field(update, "sessionUpdate"))))
            updates.push(update);
        }
      }
      child.stdin.end();
      assert.deepEqual(await closed, [0, null]);
      assert.deepEqual(updates, fixture.updates);
      const recorded = readFileSync(capture, "utf8").trimEnd().split("\n").map(decode);
      assert.deepEqual(
        recorded.map((entry) => [
          field(entry, "seq"),
          field(entry, "direction"),
          field(entry, "message"),
        ]),
        crossed.map(([direction, message], index) => [BigInt(index), direction, message]),
      );
      if (fixture.permission === true)
        assert.deepEqual(decode(readFileSync(receipt, "utf8")), permissionAnswer.result);
      verifyFixtureReport(fixture, capture);
    },
  );
}

test("the qualified Claude preload keeps the same exact tool values", (t) => {
  const directory = realpathSync(mkdtempSync(join(tmpdir(), "acp-tool-preload-")));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const newSession = {
    jsonrpc: "2.0",
    id: 2n,
    method: "session/new",
    params: { cwd: directory, mcpServers: [] },
  };
  const child = spawnSync(
    process.execPath,
    [join(fixtures, "host-tool-output-agent.ts"), encode(rawOutput), "", "true"],
    {
      input: [initialize, newSession, prompt].map((frame) => `${encode(frame)}\n`).join(""),
      env: {
        ...process.env,
        NODE_OPTIONS: `--import="${join(import.meta.dirname, "acp-stdio-recorder.mjs")}"`,
        CODEX_ROUTER_D0_ACP_RECORDING_DIR: directory,
      },
      timeout: 5_000,
    },
  );
  assert.equal(child.status, 0);
  const [capture, ...other] = readdirSync(directory);
  assert.match(capture ?? "", /^acp-\d+\.jsonl$/);
  assert.deepEqual(other, []);
  assert.ok(capture);
  const frames = readFileSync(join(directory, capture), "utf8").trimEnd().split("\n").map(decode);
  const providerMessages = frames
    .filter((frame) => field(frame, "direction") === "serverToClient")
    .map((frame) => field(frame, "message"));
  assert.deepEqual(
    providerMessages,
    child.stdout.toString("utf8").trimEnd().split("\n").map(decode),
  );
});
