import { resolve } from "node:path";
import { parseArgs } from "node:util";

import { listChangedRustFiles } from "./changed-rust-files.ts";
import { displayText, formatReport } from "./format-nudges.ts";
import {
  checkCheckoutMatchesHead,
  findRepositoryRoot,
  resolveCommit,
  resolveDefaultBase,
} from "./git-revisions.ts";
import type { ReviewRunOutcome } from "./run-review-checks.ts";

export const reviewExitCodes = {
  /** The run completed, nudges and uncertain results included, or there was nothing to check. */
  completed: 0,
  /** Git, the checkout or the configuration stopped the checks, or the run did not complete. */
  notCompleted: 1,
  badArguments: 2,
} as const;

export type ReviewExitCode = (typeof reviewExitCodes)[keyof typeof reviewExitCodes];

export interface CommandOutput {
  line(text: string): void;
  error(text: string): void;
}

/** Runs the checks on exactly these files; the composition root supplies the SDK runner. */
export type RunChecksOnFiles = (
  repositoryRoot: string,
  absolutePaths: readonly string[],
) => Promise<ReviewRunOutcome>;

export interface ReviewCommandRequest {
  readonly commandArguments: readonly string[];
  readonly workingDirectory: string;
  readonly runChecks: RunChecksOnFiles;
  readonly output: CommandOutput;
}

const usage = "usage: node review/src/review-checks.ts [--base <rev>] [--head <rev>]";

type ParsedArguments =
  | { readonly kind: "parsed"; readonly base: string | undefined; readonly head: string }
  | { readonly kind: "rejected"; readonly message: string };

function checkRevisionArgument(name: string, value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  if (value.length === 0) return `--${name} needs a revision, not an empty value`;
  if (value.startsWith("-")) return `--${name} needs a revision, not an option-shaped value`;
  return undefined;
}

export function parseReviewArguments(commandArguments: readonly string[]): ParsedArguments {
  let values: { base?: string | undefined; head?: string | undefined };
  try {
    ({ values } = parseArgs({
      args: [...commandArguments],
      options: { base: { type: "string" }, head: { type: "string" } },
      strict: true,
      allowPositionals: false,
    }));
  } catch (error: unknown) {
    return { kind: "rejected", message: error instanceof Error ? error.message : String(error) };
  }
  const problem =
    checkRevisionArgument("base", values.base) ?? checkRevisionArgument("head", values.head);
  if (problem !== undefined) return { kind: "rejected", message: problem };
  return { kind: "parsed", base: values.base, head: values.head ?? "HEAD" };
}

interface ResolvedRequest {
  readonly repositoryRoot: string;
  readonly baseObjectId: string;
  readonly headObjectId: string;
  readonly rangeLabel: string;
}

type RequestResolution =
  | { readonly kind: "resolved"; readonly request: ResolvedRequest }
  | { readonly kind: "stopped"; readonly problem: string };

async function resolveRequest(
  workingDirectory: string,
  base: string | undefined,
  head: string,
): Promise<RequestResolution> {
  const root = await findRepositoryRoot(workingDirectory);
  if (root.kind === "not_a_repository") {
    return { kind: "stopped", problem: `Not inside a git repository: ${root.message}` };
  }
  const headCommit = await resolveCommit(root.path, head);
  if (headCommit.kind === "unknown_revision") {
    return { kind: "stopped", problem: `Unknown revision for --head: ${head}` };
  }
  if (base === undefined) {
    const defaultBase = await resolveDefaultBase(root.path, headCommit.objectId);
    if (defaultBase.kind === "git_failed") {
      return { kind: "stopped", problem: `${defaultBase.command} failed: ${defaultBase.message}` };
    }
    return {
      kind: "resolved",
      request: {
        repositoryRoot: root.path,
        baseObjectId: defaultBase.objectId,
        headObjectId: headCommit.objectId,
        rangeLabel: `merge-base(origin/main, ${head})...${head}`,
      },
    };
  }
  const baseCommit = await resolveCommit(root.path, base);
  if (baseCommit.kind === "unknown_revision") {
    return { kind: "stopped", problem: `Unknown revision for --base: ${base}` };
  }
  return {
    kind: "resolved",
    request: {
      repositoryRoot: root.path,
      baseObjectId: baseCommit.objectId,
      headObjectId: headCommit.objectId,
      rangeLabel: `${base}...${head}`,
    },
  };
}

async function describeCheckoutMismatch(
  request: ResolvedRequest,
  repositoryRelativePaths: readonly string[],
): Promise<readonly string[]> {
  const state = await checkCheckoutMatchesHead(
    request.repositoryRoot,
    request.headObjectId,
    repositoryRelativePaths,
  );
  switch (state.kind) {
    case "matches":
      return [];
    case "different_commit":
      return [
        `The checked-out commit (${state.checkedOutObjectId ?? "none"}) is not the requested head (${request.headObjectId}). The checks read the working tree: check out the head first.`,
      ];
    case "modified_files":
      return [
        "These files differ from the requested head, staged or not; the checks would read the edited text:",
        ...state.repositoryRelativePaths.map((path) => `  ${path}`),
      ];
    case "git_failed":
      return [`${state.command} failed: ${state.message}`];
    default:
      return assertNever(state);
  }
}

function printOutcome(
  outcome: ReviewRunOutcome,
  absolutePaths: readonly string[],
  output: CommandOutput,
): ReviewExitCode {
  switch (outcome.kind) {
    case "completed":
      for (const line of formatReport(outcome.report, absolutePaths)) output.line(line);
      return reviewExitCodes.completed;
    case "incomplete":
      for (const line of formatReport(outcome.report, absolutePaths)) output.line(line);
      output.error(`The review run is incomplete: ${outcome.gaps.join("; ")}.`);
      return reviewExitCodes.notCompleted;
    case "stopped":
      output.error(`The review stopped at ${outcome.stage}: ${outcome.ending}.`);
      for (const problem of outcome.problems) output.error(`  ${displayText(problem)}`);
      if (outcome.partial !== undefined) {
        output.line("Partial results from before it stopped (incomplete):");
        for (const line of formatReport(outcome.partial, absolutePaths)) output.line(line);
      }
      return reviewExitCodes.notCompleted;
    default:
      return assertNever(outcome);
  }
}

function assertNever(value: never): never {
  throw new Error(`Unhandled value: ${JSON.stringify(value)}`);
}

/** Parses the arguments, resolves and checks the range, runs the checks and prints them. */
export async function runReviewCommand(request: ReviewCommandRequest): Promise<ReviewExitCode> {
  const { output } = request;
  const parsed = parseReviewArguments(request.commandArguments);
  if (parsed.kind === "rejected") {
    output.error(displayText(parsed.message));
    output.error(usage);
    return reviewExitCodes.badArguments;
  }
  const resolution = await resolveRequest(request.workingDirectory, parsed.base, parsed.head);
  if (resolution.kind === "stopped") {
    output.error(displayText(resolution.problem));
    return reviewExitCodes.notCompleted;
  }
  const resolved = resolution.request;
  const changed = await listChangedRustFiles(resolved.repositoryRoot, resolved);
  if (changed.kind === "git_failed") {
    output.error(displayText(`${changed.command} failed: ${changed.message}`));
    return reviewExitCodes.notCompleted;
  }
  const paths = changed.repositoryRelativePaths;
  const range = displayText(resolved.rangeLabel);
  if (paths.length === 0) {
    output.line(`No changed Rust files under crates/ in ${range}: nothing to check.`);
    return reviewExitCodes.completed;
  }
  const mismatch = await describeCheckoutMismatch(resolved, paths);
  if (mismatch.length > 0) {
    for (const line of mismatch) output.error(displayText(line));
    return reviewExitCodes.notCompleted;
  }
  output.line(`Checking ${paths.length} changed Rust files in ${range}`);
  const absolutePaths = paths.map((path) => resolve(resolved.repositoryRoot, path));
  const outcome = await request.runChecks(resolved.repositoryRoot, absolutePaths);
  return printOutcome(outcome, absolutePaths, output);
}
