// The command's exit contract, end to end through argument parsing, git and printing. The SDK
// runner is a stub here, so every case is keyless; the real runner is covered in
// run-review-checks.test.ts.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { resolve } from "node:path";
import { after, before, describe, test } from "node:test";
import { promisify } from "node:util";

import { reviewExitCodes, runReviewCommand } from "./review-command.ts";
import type { RunChecksOnFiles } from "./review-command.ts";
import type { ReviewReport, ReviewRunOutcome } from "./run-review-checks.ts";
import { TemporaryGitRepository } from "./test-support/temporary-git-repository.ts";

const runExecFile = promisify(execFile);
const entryScript = resolve(import.meta.dirname, "review-checks.ts");

interface CommandRun {
  readonly exitCode: number;
  readonly lines: readonly string[];
  readonly errors: readonly string[];
  readonly runnerCalls: readonly (readonly string[])[];
}

async function runCommand(
  repositoryRoot: string,
  commandArguments: readonly string[],
  outcome: ReviewRunOutcome,
): Promise<CommandRun> {
  const lines: string[] = [];
  const errors: string[] = [];
  const runnerCalls: string[][] = [];
  const runChecks: RunChecksOnFiles = async (_root, absolutePaths) => {
    runnerCalls.push([...absolutePaths]);
    return outcome;
  };
  const exitCode = await runReviewCommand({
    commandArguments,
    workingDirectory: repositoryRoot,
    runChecks,
    output: { line: (text) => lines.push(text), error: (text) => errors.push(text) },
  });
  return { exitCode, lines, errors, runnerCalls };
}

const emptyReport: ReviewReport = {
  nudges: [],
  unavailable: [],
  piecesSelected: 0,
  piecesEvaluated: 0,
  modelRequests: 0,
};
const completedWithNothing: ReviewRunOutcome = { kind: "completed", report: emptyReport };

interface RangeRepository {
  readonly repository: TemporaryGitRepository;
  /** The repository root as git reports it, symbolic links resolved. */
  root: string;
  base: string;
  docsChange: string;
  rustChange: string;
}

/** A repository with a base commit, a docs-only commit and a Rust commit on main. */
function useRangeRepository(): RangeRepository {
  const range: RangeRepository = {
    repository: new TemporaryGitRepository(),
    root: "",
    base: "",
    docsChange: "",
    rustChange: "",
  };
  before(() => {
    const { repository } = range;
    repository.write("crates/alpha/src/lib.rs", "pub fn alpha() {}\n");
    range.base = repository.commitAll("base");
    repository.write("README.md", "# docs only\n");
    range.docsChange = repository.commitAll("docs only");
    repository.write("crates/alpha/src/clock.rs", "pub fn clock() {}\n");
    range.rustChange = repository.commitAll("rust change");
    range.root = repository.git("rev-parse", "--show-toplevel");
  });
  after(() => range.repository.remove());
  return range;
}

describe("runReviewCommand arguments and revisions", () => {
  const range = useRangeRepository();
  const { repository } = range;

  for (const badArguments of [
    ["--base="],
    ["--base=--output=x"],
    ["--head=-x"],
    ["--base"],
    ["--bogus"],
    ["positional"],
  ]) {
    test(`bad syntax ${JSON.stringify(badArguments)} exits 2 before git or the runner`, async () => {
      const run = await runCommand(repository.root, badArguments, completedWithNothing);

      assert.equal(run.exitCode, reviewExitCodes.badArguments);
      assert.deepEqual(run.runnerCalls, []);
    });
  }

  test("an unknown revision exits 1 and names it", async () => {
    const run = await runCommand(repository.root, ["--base", "no-such-rev"], completedWithNothing);

    assert.equal(run.exitCode, reviewExitCodes.notCompleted);
    assert.deepEqual(run.errors, ["Unknown revision for --base: no-such-rev"]);
    assert.deepEqual(run.runnerCalls, []);
  });

  test("a range with no changed Rust files completes with a zero-work line", async () => {
    const run = await runCommand(
      repository.root,
      ["--base", range.base, "--head", range.docsChange],
      completedWithNothing,
    );

    assert.equal(run.exitCode, reviewExitCodes.completed);
    assert.deepEqual(run.lines, [
      `No changed Rust files under crates/ in ${range.base}...${range.docsChange}: nothing to check.`,
    ]);
    assert.deepEqual(run.runnerCalls, []);
  });

  test("a run that selects no pieces completes with a zero-work line", async () => {
    const run = await runCommand(repository.root, ["--base", range.base], completedWithNothing);

    assert.equal(run.exitCode, reviewExitCodes.completed);
    assert.deepEqual(run.runnerCalls, [[resolve(range.root, "crates/alpha/src/clock.rs")]]);
    assert.equal(run.lines.at(-1), "No rule pieces selected in 1 files: nothing to check.");
  });
});

describe("runReviewCommand run outcomes", () => {
  const range = useRangeRepository();
  const { repository } = range;

  test("an incomplete run exits 1 and still lists the unavailable piece", async () => {
    const run = await runCommand(
      repository.root,
      ["--base", range.base, "--head", range.rustChange],
      {
        kind: "incomplete",
        gaps: ["1 unavailable"],
        report: {
          ...emptyReport,
          unavailable: [
            {
              file: "crates/alpha/src/clock.rs",
              line: 1n,
              rule: "review.example",
              reason: "credential_unavailable",
            },
          ],
          piecesSelected: 1,
        },
      },
    );

    assert.equal(run.exitCode, reviewExitCodes.notCompleted);
    assert.ok(
      run.lines.includes(
        "crates/alpha/src/clock.rs:1  review.example  unavailable (credential_unavailable)",
      ),
    );
    assert.deepEqual(run.errors, ["The review run is incomplete: 1 unavailable."]);
  });

  test("a completed run with a mechanical nudge exits 0", async () => {
    const run = await runCommand(repository.root, ["--base", range.base], {
      kind: "completed",
      report: {
        ...emptyReport,
        nudges: [
          {
            kind: "finding",
            file: "crates/alpha/src/clock.rs",
            line: 1n,
            rule: "review.example",
            message: "Look here.",
            judgment: undefined,
          },
        ],
        piecesSelected: 1,
        piecesEvaluated: 1,
      },
    });

    assert.equal(run.exitCode, reviewExitCodes.completed);
    assert.ok(run.lines.includes("crates/alpha/src/clock.rs:1  review.example  Look here."));
  });
});

describe("runReviewCommand checkout check", () => {
  const range = useRangeRepository();
  const { repository } = range;

  test("a different checked-out commit exits 1 before the runner", async () => {
    const run = await runCommand(
      repository.root,
      ["--base", range.base, "--head", range.rustChange],
      completedWithNothing,
    );
    repository.git("switch", "--quiet", "--detach", range.docsChange);
    const mismatched = await runCommand(
      repository.root,
      ["--base", range.base, "--head", range.rustChange],
      completedWithNothing,
    );
    repository.git("switch", "--quiet", "main");

    assert.equal(run.exitCode, reviewExitCodes.completed);
    assert.equal(mismatched.exitCode, reviewExitCodes.notCompleted);
    assert.match(mismatched.errors[0] ?? "", /is not the requested head/u);
    assert.deepEqual(mismatched.runnerCalls, []);
  });

  test("a modified selected file at the right commit exits 1 before the runner", async () => {
    repository.write("crates/alpha/src/clock.rs", "pub fn edited() {}\n");
    const run = await runCommand(repository.root, ["--base", range.base], completedWithNothing);
    repository.git("restore", "--worktree", "--", "crates/alpha/src/clock.rs");

    assert.equal(run.exitCode, reviewExitCodes.notCompleted);
    assert.deepEqual(run.errors.slice(1), ["  crates/alpha/src/clock.rs"]);
    assert.deepEqual(run.runnerCalls, []);
  });
});

describe("review-checks entry point", () => {
  const repository = new TemporaryGitRepository();

  before(() => {
    repository.write("README.md", "# no Rust\n");
    repository.commitAll("base");
  });

  after(() => repository.remove());

  async function runEntry(commandArguments: readonly string[]): Promise<number> {
    try {
      await runExecFile(process.execPath, [entryScript, ...commandArguments], {
        cwd: repository.root,
      });
      return 0;
    } catch (error: unknown) {
      if (error instanceof Error && "code" in error && typeof error.code === "number") {
        return error.code;
      }
      throw error;
    }
  }

  test("exits 2 on bad arguments", async () => {
    assert.equal(await runEntry(["--base="]), reviewExitCodes.badArguments);
  });

  test("exits 1 on an unknown revision", async () => {
    assert.equal(await runEntry(["--base", "no-such-rev"]), reviewExitCodes.notCompleted);
  });

  test("exits 0 when there is nothing to check", async () => {
    assert.equal(await runEntry(["--base", "HEAD"]), reviewExitCodes.completed);
  });
});
