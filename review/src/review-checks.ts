// Runs the review checks on a change's Rust files under `crates/`.
//   node review/src/review-checks.ts [--base <rev>] [--head <rev>]
// Exit codes: 0 the run completed (nudges and uncertain results included) or there was nothing
// to check; 1 git, the checkout or the configuration stopped the checks, or the run did not
// complete; 2 bad arguments.
import { resolve } from "node:path";

import { runReviewCommand } from "./review-command.ts";
import { runReviewChecks } from "./run-review-checks.ts";

const reviewDirectory = resolve(import.meta.dirname, "..");

process.exitCode = await runReviewCommand({
  commandArguments: process.argv.slice(2),
  workingDirectory: process.cwd(),
  runChecks: (repositoryRoot, absolutePaths) =>
    runReviewChecks(reviewDirectory, repositoryRoot, absolutePaths),
  output: {
    line: (text) => console.log(text),
    error: (text) => console.error(text),
  },
});
