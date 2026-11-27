import assert from "node:assert/strict";
import { test } from "node:test";
import { encode } from "../../../exact-json.ts";
import { renderToolFrameReport } from "./acp-tool-frame-report.ts";

function capture(updates: readonly unknown[]): string {
  const start = [
    {
      direction: "clientToServer",
      message: {
        id: 1,
        method: "initialize",
        params: { clientCapabilities: { embeddedContext: true } },
      },
    },
    {
      direction: "serverToClient",
      message: { id: 1, result: { agentInfo: { name: "fixture", version: "1" } } },
    },
    {
      direction: "clientToServer",
      message: { id: 2, method: "session/prompt", params: { sessionId: "private-session" } },
    },
  ];
  const tool = updates.map((update) => ({
    direction: "serverToClient",
    message: { method: "session/update", params: { sessionId: "private-session", update } },
  }));
  const done = {
    direction: "serverToClient",
    message: { id: 2, result: { stopReason: "end_turn" } },
  };
  return (
    [...start, ...tool, done]
      .map(({ direction, message }, seq) =>
        encode({ seq, connection: "fixture", direction, message: { jsonrpc: "2.0", ...message } }),
      )
      .join("\n") + "\n"
  );
}

const announcement = {
  sessionUpdate: "tool_call",
  toolCallId: "toolu_fixture",
  title: "Exit 99 is also just a title",
  rawInput: { command: "printf hello", cwd: "/private/folder" },
  _meta: { provider: [{ "new-key": { value: "private-value" } }] },
};
const completed = {
  sessionUpdate: "tool_call_update",
  toolCallId: "toolu_fixture",
  status: "completed",
  rawOutput: { stdout: "alpha", stderr: "beta", exitCode: 3 },
  content: [{ type: "diff", path: "/private/folder/notes.txt", oldText: "two", newText: "three" }],
  locations: [{ path: "/private/folder/notes.txt", line: 2 }],
};

test("complete frame inventory and all eight path comparisons come from structure", () => {
  const claude = capture([announcement, completed]);
  const cursor = capture([{ ...announcement, rawInput: { cmd: "printf hello" } }, completed]);

  const report = renderToolFrameReport({ claude, cursor });

  const tableRows = report
    .split("\n")
    .filter((line) => line.startsWith("| "))
    .slice(1);
  assert.equal(tableRows.length, 8);
  for (const [field, path] of [
    ["Command", ".rawInput.command"],
    ["Working folder", ".rawInput.cwd"],
    ["Output", ".rawOutput.stdout"],
    ["Standard error", ".rawOutput.stderr"],
    ["Exit code", ".rawOutput.exitCode"],
    ["File path", ".content[0].path"],
    ["Old and new text", ".content[0].oldText"],
    ["Line number", ".locations[0].line"],
  ] as const) {
    assert.ok(tableRows.find((line) => line.startsWith(`| ${field} |`))?.includes(path), field);
  }
  assert.match(report, /\.content\[0\]\.newText/);
  assert.match(report, /\| Command .* \| Different paths \|/);
  assert.match(report, /\| Exit code .* \| Same paths \|/);
  assert.match(report, /_meta\.provider\[0\]\["new-key"\] \{"value"\}/);
  assert.match(report, /Repeated tool keys with unchanged values: ` \["toolCallId"\] `/);
  assert.match(report, /Repeated tool keys with changed values: ` \["sessionUpdate"\] `/);
  assert.match(report, /fixture","1/);
  assert.doesNotMatch(report, /\/private\/folder|private-session|private-value|Exit 99/);
});

test("a capture without tool updates stays unobserved", () => {
  const report = renderToolFrameReport({ cursor: capture([]), claude: capture([announcement]) });
  assert.match(report, /\| Command .* \| Unobserved \| Undetermined \|/);
  assert.match(report, /\| Exit code \| Absent from recognized fields \| Unobserved \|/);
});

test("corrupt, truncated or missing lines cannot produce a complete report", () => {
  const complete = capture([announcement]);
  const withoutReply = complete.split("\n").slice(0, -2).join("\n") + "\n";
  assert.match(
    renderToolFrameReport({ claude: withoutReply }),
    /Prompt requests: 1. Without a matching reply: 1./,
  );
  assert.throws(
    () => renderToolFrameReport({ claude: complete.trimEnd() }),
    /ends without a complete line/,
  );
  assert.throws(
    () => renderToolFrameReport({ claude: complete.replace(/^.*\n/, "") }),
    /incomplete ACP capture at line 1/,
  );
  assert.throws(
    () =>
      renderToolFrameReport({
        claude: complete.replace(/^.*\n/, '{"seq":0,"unparsed":"private"}\n'),
      }),
    /incomplete ACP capture at line 1/,
  );
});
