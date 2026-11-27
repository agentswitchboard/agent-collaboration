// Keyless: every test here stays on static evidence or mechanical decision steps, so no
// model request is made.
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { describe, test } from "node:test";

import { check, inspect, run } from "@sunstack/sdk";
import type { LoadedConfiguration } from "@sunstack/sdk";

import {
  collectAskedQuestions,
  findLastJudgment,
  loadReviewConfiguration,
  runLoadedReviewChecks,
} from "./run-review-checks.ts";

const reviewDirectory = resolve(import.meta.dirname, "..");
const fixtureRoot = resolve(reviewDirectory, "test-fixtures");
const pausedBackoffTest = resolve(
  fixtureRoot,
  "crates/sample-service/tests/integration/paused_backoff.rs",
);

async function loadConfiguration(): Promise<LoadedConfiguration> {
  const loaded = await loadReviewConfiguration(reviewDirectory);
  assert.equal(loaded.kind, "loaded");
  if (loaded.kind !== "loaded") throw new Error("unreachable");
  return loaded.configuration;
}

/**
 * The review configuration with its model budget cut to the smallest the SDK admits. A budget
 * of 0 is refused ("expected a nonzero u32"), so 1 is the closest guard: if the paused-time
 * route ever regressed into a judgment while the SDK could reach a credential, the run could
 * spend at most one request before the assertions fail.
 */
async function loadConfigurationWithOneRequestBudget(): Promise<LoadedConfiguration> {
  const loaded = await loadConfiguration();
  return {
    ...loaded,
    configuration: {
      ...loaded.configuration,
      limits: { ...loaded.configuration.limits, max_model_requests: 1 },
    },
  };
}

describe("review configuration", () => {
  test("is admitted with the four review rules", async () => {
    const configuration = await loadConfiguration();

    const admission = await check(configuration);

    assert.equal(admission.kind, "valid");
    if (admission.kind !== "valid") return;
    assert.equal(admission.summary.active_rule_count, 4);
    assert.equal(admission.summary.evidence_kind_count, 4);
  });

  test("selects the expected pieces for each evidence kind on the fixture tree", async () => {
    const configuration = await loadConfiguration();

    const inspection = await inspect(configuration, { root: fixtureRoot, scope: { kind: "all" } });

    assert.equal(inspection.kind, "inspected");
    if (inspection.kind !== "inspected") return;
    const placesByKind = new Map<string, string[]>();
    for (const piece of inspection.report.pieces) {
      const places = placesByKind.get(piece.kind) ?? [];
      places.push(`${piece.where.file}:${piece.where.first_line}`);
      placesByKind.set(piece.kind, places);
    }
    assert.deepEqual(Object.fromEntries(placesByKind), {
      prod_call: ["crates/sample-service/src/lib.rs:5"],
      test_call_inline: ["crates/sample-service/src/lib.rs:16"],
      test_call_file: ["crates/sample-service/tests/integration/paused_backoff.rs:5"],
      // Only the top-level Host crate's source: not its tests or lib.rs, and not the nested
      // crates/example/crates/codex-router-host lookalike.
      rust_file: ["crates/codex-router-host/src/service_wiring.rs:1"],
    });
  });
});

describe("mechanical route", () => {
  test("a tokio sleep in a file on paused time passes without a model request", async () => {
    const configuration = await loadConfigurationWithOneRequestBudget();

    const outcome = await run(configuration, {
      root: fixtureRoot,
      scope: { kind: "all" },
      paths: [pausedBackoffTest],
    });

    assert.equal(outcome.kind, "completed");
    if (outcome.kind !== "completed") return;
    assert.equal(outcome.report.usage.model_requests, 0);
    assert.deepEqual(
      outcome.report.results.map((result) => [result.name, result.outcome, result.path]),
      [["review.time.test_file_waits_on_real_time", "pass", ["is_tokio", "paused"]]],
    );
  });

  test("runLoadedReviewChecks reports the paused-time file with no nudges", async () => {
    const configuration = await loadConfigurationWithOneRequestBudget();

    const outcome = await runLoadedReviewChecks({
      configuration,
      repositoryRoot: fixtureRoot,
      absolutePaths: [pausedBackoffTest],
    });

    assert.equal(outcome.kind, "completed");
    if (outcome.kind !== "completed") return;
    assert.deepEqual(outcome.report, {
      nudges: [],
      unavailable: [],
      piecesSelected: 1,
      piecesEvaluated: 1,
      modelRequests: 0,
    });
  });
});

describe("findLastJudgment", () => {
  test("keeps a judged answer when a later mechanical step ends uncertain", async () => {
    const asked = collectAskedQuestions(await loadConfiguration());

    const judgment = findLastJudgment(
      asked.get("review.time.test_file_waits_on_real_time"),
      ["is_tokio", "waits_on_real_time", "read_whole"],
      { waits_on_real_time: { type: "noul", yes_probability: 0.2 } },
    );

    assert.deepEqual(judgment, { question: "waits_on_real_time", probability: 0.2 });
  });

  test("takes the last of several judged questions on the path", async () => {
    const asked = collectAskedQuestions(await loadConfiguration());

    const judgment = findLastJudgment(
      asked.get("review.time.production_decides_on_system_clock"),
      ["clock_home", "clock_source", "system_rule"],
      {
        clock_source: { type: "noul", yes_probability: 0.1 },
        system_rule: { type: "noul", yes_probability: 0.5 },
      },
    );

    assert.deepEqual(judgment, { question: "system_rule", probability: 0.5 });
  });

  test("has no judgment when only mechanical steps ran", async () => {
    const asked = collectAskedQuestions(await loadConfiguration());

    const judgment = findLastJudgment(
      asked.get("review.time.test_file_waits_on_real_time"),
      ["is_tokio", "paused"],
      {},
    );

    assert.equal(judgment, undefined);
  });
});
