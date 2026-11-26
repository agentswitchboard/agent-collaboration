import { createFixtureDirectory } from "../test-support/fixture-directory.ts";
import assert from "node:assert/strict";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { z } from "zod";
import { containsExpected } from "./acp-stdio.ts";

const schema = z.object({
  name: z.string(),
  steps: z.array(z.unknown()),
  input: z.string(),
  expected: z.object({
    status: z.number(),
    stdout: z.string(),
    stderr: z.string(),
    receipt: z.string().nullable(),
    marker: z.boolean(),
  }),
});
const packedRows = readFileSync(new URL("./baselines/scripted.jsonl", import.meta.url), "utf8")
  .trimEnd()
  .split("\n")
  .map((line, index) =>
    z
      .strictObject({ name: z.string(), source: z.string() })
      .parse(JSON.parse(line), { error: (): string => `scripted JSONL row ${index + 1}` }),
  );
assert.deepEqual(packedRows.map((row) => row.name).sort(), [
  "scripted-early-exit.json",
  "scripted-exact-params.json",
  "scripted-exchange-and-tail.json",
  "scripted-large-request-id.json",
  "scripted-mismatch.json",
  "scripted-uncaptured-response.json",
  "scripted-unexpected-eof.json",
]);
for (const { name, source } of packedRows.toSorted((left, right) =>
  left.name.localeCompare(right.name),
)) {
  let entry: z.infer<typeof schema>;
  try {
    entry = schema.parse(JSON.parse(source));
  } catch (error) {
    throw new Error(`Invalid fixture ${name}`, { cause: error });
  }
  test(`scripted ACP ${entry.name} matches original replies, diagnostics and receipt bytes`, (): void => {
    const root = createFixtureDirectory("acp-fixture");
    try {
      const receipt = join(root, "receipt.jsonl");
      const marker = join(root, "marker");
      const steps = JSON.stringify(entry.steps)
        .replaceAll("<receipt>", receipt)
        .replaceAll("<marker>", marker);
      const result = spawnSync(
        process.execPath,
        [new URL("./scripted-acp-agent.ts", import.meta.url).pathname, steps],
        { input: entry.input, encoding: "utf8" },
      );
      assert.deepEqual(
        {
          status: result.status,
          stdout: result.stdout,
          stderr: result.stderr,
          receipt: existsSync(receipt) ? readFileSync(receipt, "utf8") : null,
          marker: existsSync(marker),
        },
        entry.expected,
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}

test("partial fixture expectations preserve JSON numeric equality and exact array lengths", (): void => {
  assert.equal(containsExpected({ number: 1n, extra: "retained" }, { number: 1 }), true);
  assert.equal(containsExpected(true, 1n), true);
  assert.equal(containsExpected(0, false), true);
  assert.equal(containsExpected([1n, 2n], [1n]), false);
  assert.equal(containsExpected({ number: 1n }, { number: 2n }), false);
});

const initialRequests = [
  { jsonrpc: "2.0", id: 1, method: "initialize", params: {} },
  { jsonrpc: "2.0", id: 2, method: "session/load", params: {} },
];
const setterMethod = "session/set_config_option";
const setterParams = { configId: "model", value: "new" };
const setter = { jsonrpc: "2.0", id: 3, method: setterMethod, params: setterParams };

for (const scenario of [
  { name: "allows EOF", input: initialRequests, responseIds: [1, 2], marker: false },
  {
    name: "records and answers a matching setter",
    input: [...initialRequests, setter],
    responseIds: [1, 2, 3],
    marker: true,
  },
  {
    name: "rejects an unrelated request",
    input: [...initialRequests, { ...setter, method: "session/prompt" }],
    responseIds: [1, 2],
    marker: false,
    diagnostic: /step 5 mismatch[\s\S]*actual:\s+\{[\s\S]*"method": "session\/prompt"/,
  },
  {
    name: "requires the preceding load",
    input: initialRequests.slice(0, 1),
    responseIds: [1],
    marker: false,
    diagnostic: /step 3 mismatch[\s\S]*actual:\s+"EOF"/,
  },
]) {
  test(`scripted ACP optional terminal setter ${scenario.name}`, (): void => {
    const root = createFixtureDirectory("acp-setting");
    try {
      const marker = join(root, "setting");
      const diagnostic = join(root, "wire.json");
      const steps = [
        { action: "expect_request", requestName: "init", method: "initialize", params: {} },
        { action: "respond", requestName: "init", result: {} },
        { action: "expect_request", requestName: "load", method: "session/load", params: {} },
        { action: "respond", requestName: "load", result: {} },
        {
          action: "expect_request",
          requestName: "setter",
          method: setterMethod,
          params: setterParams,
          exitOnEof: true,
        },
        { action: "write_marker", path: marker },
        { action: "respond", requestName: "setter", result: {} },
      ];
      const result = spawnSync(
        process.execPath,
        [new URL("./scripted-acp-agent.ts", import.meta.url).pathname, JSON.stringify(steps)],
        {
          input: `${scenario.input.map((request) => JSON.stringify(request)).join("\n")}\n`,
          encoding: "utf8",
          env: { ...process.env, ACP_FIXTURE_DIAGNOSTICS: diagnostic },
        },
      );
      assert.ifError(result.error);
      assert.equal(existsSync(marker), scenario.marker);
      assert.deepEqual(
        result.stdout
          .trimEnd()
          .split("\n")
          .map((line) => JSON.parse(line)),
        scenario.responseIds.map((id) => ({ jsonrpc: "2.0", id, result: {} })),
      );
      if (scenario.diagnostic === undefined) {
        assert.equal(existsSync(diagnostic), false, result.stderr);
        assert.equal(result.status, 0, result.stderr);
      } else {
        assert.equal(result.status, 1, result.stderr);
        assert.match(readFileSync(diagnostic, "utf8"), scenario.diagnostic);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}
