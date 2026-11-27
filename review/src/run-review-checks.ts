import { check, load, run } from "@sunstack/sdk";
import type {
  Diagnostic,
  LoadedConfiguration,
  RunOutcome,
  SdkFailure,
  SdkReport,
  WorkUnavailableReason,
} from "@sunstack/sdk";

// The SDK does not export its judgment answer type by name; it is read off the report.
type JudgmentAnswer = SdkReport["uncertain"][number]["answers"][string];

/** The probability a model gave to the last question judged on a piece's decision path. */
export interface LastJudgment {
  readonly question: string;
  readonly probability: number;
}

/** A place the checks ask the reviewer to look at. */
export type ReviewNudge =
  | {
      readonly kind: "finding";
      readonly file: string;
      readonly line: bigint;
      readonly rule: string;
      readonly message: string;
      /** Absent for a finding reached only by mechanical steps. */
      readonly judgment: LastJudgment | undefined;
    }
  | {
      readonly kind: "uncertain";
      readonly file: string;
      readonly line: bigint;
      readonly rule: string;
      /** Absent when no question was judged before the step that could not decide. */
      readonly judgment: LastJudgment | undefined;
    };

/** A rule piece the run could not evaluate, with the SDK's reason. */
export interface UnavailablePiece {
  readonly file: string;
  readonly line: bigint;
  readonly rule: string;
  readonly reason: WorkUnavailableReason["kind"];
}

export interface ReviewReport {
  readonly nudges: readonly ReviewNudge[];
  readonly unavailable: readonly UnavailablePiece[];
  /** Rule pieces the evidence kinds selected in the named files. */
  readonly piecesSelected: number;
  /** Selected pieces that reached an ending: pass, finding, not applicable or uncertain. */
  readonly piecesEvaluated: number;
  readonly modelRequests: number;
}

export type ReviewStage = "load" | "check" | "run";
export type ReviewStopEnding = "invalid" | "unavailable" | "cancelled" | "failed";

export type ReviewRunOutcome =
  | { readonly kind: "completed"; readonly report: ReviewReport }
  | { readonly kind: "incomplete"; readonly report: ReviewReport; readonly gaps: readonly string[] }
  | {
      readonly kind: "stopped";
      readonly stage: ReviewStage;
      readonly ending: ReviewStopEnding;
      readonly problems: readonly string[];
      /** Results the run produced before it stopped; they are incomplete. */
      readonly partial: ReviewReport | undefined;
    };

export type ReviewConfigurationLoad =
  | { readonly kind: "loaded"; readonly configuration: LoadedConfiguration }
  | { readonly kind: "stopped"; readonly outcome: Extract<ReviewRunOutcome, { kind: "stopped" }> };

export interface LoadedReviewCheckRequest {
  readonly configuration: LoadedConfiguration;
  /** Absolute repository root the checked paths belong to. */
  readonly repositoryRoot: string;
  /** Absolute paths of the files to check; must not be empty. */
  readonly absolutePaths: readonly string[];
}

type AskedQuestions = ReadonlyMap<string, ReadonlyMap<string, string>>;

function describeDiagnostics(diagnostics: readonly Diagnostic[]): readonly string[] {
  return diagnostics.map((diagnostic) => {
    const where = diagnostic.path === null ? "" : ` at ${diagnostic.path}`;
    const field = diagnostic.json_path.length > 0 ? ` (${diagnostic.json_path.join(".")})` : "";
    return `${diagnostic.code}${field}${where}: ${diagnostic.message}`;
  });
}

function describeFailure(failure: SdkFailure): readonly string[] {
  return [`${failure.phase}: ${failure.reason}: ${failure.message}`];
}

function stopped(
  stage: ReviewStage,
  ending: ReviewStopEnding,
  problems: readonly string[],
  partial?: ReviewReport,
): Extract<ReviewRunOutcome, { kind: "stopped" }> {
  return { kind: "stopped", stage, ending, problems, partial };
}

/** Maps each rule name to the question each of its asking steps asks. */
export function collectAskedQuestions(configuration: LoadedConfiguration): AskedQuestions {
  const askedByRule = new Map<string, ReadonlyMap<string, string>>();
  for (const rulebook of configuration.configuration.rulebooks) {
    for (const entry of Object.values(rulebook.entries.value)) {
      const askedByStep = new Map<string, string>();
      for (const [stepName, step] of Object.entries(entry.decision.steps)) {
        if ("ask" in step) askedByStep.set(stepName, step.ask);
      }
      askedByRule.set(entry.name, askedByStep);
    }
  }
  return askedByRule;
}

function answerProbability(answer: JudgmentAnswer): number {
  switch (answer.type) {
    case "noul":
      return answer.yes_probability;
    case "choice":
    case "score":
      return answer.confidence;
    default:
      return assertNever(answer);
  }
}

/** Walks the decision path backwards to the last step that asked a question with an answer. */
export function findLastJudgment(
  askedByStep: ReadonlyMap<string, string> | undefined,
  path: readonly string[],
  answers: Readonly<Record<string, JudgmentAnswer>> | undefined,
): LastJudgment | undefined {
  for (const stepName of path.toReversed()) {
    const question = askedByStep?.get(stepName);
    const answer = question === undefined ? undefined : answers?.[question];
    if (question !== undefined && answer !== undefined) {
      return { question, probability: answerProbability(answer) };
    }
  }
  return undefined;
}

function readReport(report: SdkReport, asked: AskedQuestions): ReviewReport {
  const findings = report.findings.map((finding): ReviewNudge => ({
    kind: "finding",
    file: finding.file,
    line: finding.line,
    rule: finding.rule,
    message: finding.message,
    judgment:
      finding.judgment === null
        ? undefined
        : findLastJudgment(
            asked.get(finding.rule),
            finding.judgment.path_taken,
            finding.judgment.answers,
          ),
  }));
  const uncertain: ReviewNudge[] = [];
  const unavailable: UnavailablePiece[] = [];
  let piecesEvaluated = 0;
  for (const result of report.results) {
    const place = { file: result.file, line: result.first_line, rule: result.name };
    switch (result.outcome) {
      case "uncertain":
        uncertain.push({
          kind: "uncertain",
          ...place,
          judgment: findLastJudgment(asked.get(result.name), result.path, result.answers),
        });
        piecesEvaluated += 1;
        break;
      case "unavailable":
        unavailable.push({ ...place, reason: result.reason.kind });
        break;
      case "pass":
      case "finding":
      case "not_applicable":
        piecesEvaluated += 1;
        break;
      case "incomplete":
        break;
      default:
        assertNever(result);
    }
  }
  return {
    nudges: [...findings, ...uncertain],
    unavailable,
    piecesSelected: report.results.length,
    piecesEvaluated,
    modelRequests: report.usage.model_requests,
  };
}

/** One line per reason the run stopped short, with how many parts it covers. */
function countIncompleteReasons(parts: SdkReport["incomplete"]): readonly string[] {
  const counts = new Map<string, number>();
  for (const part of parts) counts.set(part.reason.kind, (counts.get(part.reason.kind) ?? 0) + 1);
  return [...counts].map(([reason, count]) => `${count} ${reason}`);
}

function readRunOutcome(outcome: RunOutcome, asked: AskedQuestions): ReviewRunOutcome {
  const partial = (report: SdkReport | null): ReviewReport | undefined =>
    report === null ? undefined : readReport(report, asked);
  switch (outcome.kind) {
    case "completed":
      return { kind: "completed", report: readReport(outcome.report, asked) };
    case "incomplete":
      return {
        kind: "incomplete",
        report: readReport(outcome.report, asked),
        gaps: countIncompleteReasons(outcome.report.incomplete),
      };
    case "invalid":
      return stopped(
        "run",
        "invalid",
        describeDiagnostics(outcome.diagnostics),
        partial(outcome.report),
      );
    case "unavailable":
      return stopped("run", "unavailable", describeDiagnostics(outcome.diagnostics));
    case "cancelled":
      return stopped("run", "cancelled", [], partial(outcome.report));
    case "failed":
      return stopped("run", "failed", describeFailure(outcome.failure), partial(outcome.report));
    default:
      return assertNever(outcome);
  }
}

function assertNever(value: never): never {
  throw new Error(`Unhandled SDK value: ${JSON.stringify(value)}`);
}

/** Loads the review configuration the `sunstack` CLI reads, from its directory. */
export async function loadReviewConfiguration(
  reviewDirectory: string,
): Promise<ReviewConfigurationLoad> {
  const loaded = await load({ directory: reviewDirectory });
  switch (loaded.kind) {
    case "loaded":
      return { kind: "loaded", configuration: loaded.value };
    case "invalid":
    case "unavailable":
      return {
        kind: "stopped",
        outcome: stopped("load", loaded.kind, describeDiagnostics(loaded.diagnostics)),
      };
    case "failed":
      return {
        kind: "stopped",
        outcome: stopped("load", "failed", describeFailure(loaded.failure)),
      };
    default:
      return assertNever(loaded);
  }
}

/** Admits a loaded configuration and runs it on exactly the given files. */
export async function runLoadedReviewChecks(
  request: LoadedReviewCheckRequest,
): Promise<ReviewRunOutcome> {
  const admission = await check(request.configuration);
  switch (admission.kind) {
    case "valid":
      break;
    case "invalid":
    case "unavailable":
    case "cancelled":
      return stopped("check", admission.kind, describeDiagnostics(admission.diagnostics));
    case "failed":
      return stopped("check", "failed", describeFailure(admission.failure));
    default:
      return assertNever(admission);
  }
  // The SDK's `since` scope reads the whole worktree today, so the changed files are named
  // explicitly and run with scope `all`. Once `since` follows the diff's own paths, the
  // explicit list can give way to it.
  const outcome = await run(request.configuration, {
    root: request.repositoryRoot,
    scope: { kind: "all" },
    paths: [...request.absolutePaths],
  });
  return readRunOutcome(outcome, collectAskedQuestions(request.configuration));
}

/** Loads the configuration in `reviewDirectory`, then runs it on exactly the given files. */
export async function runReviewChecks(
  reviewDirectory: string,
  repositoryRoot: string,
  absolutePaths: readonly string[],
): Promise<ReviewRunOutcome> {
  const loaded = await loadReviewConfiguration(reviewDirectory);
  if (loaded.kind === "stopped") return loaded.outcome;
  return runLoadedReviewChecks({
    configuration: loaded.configuration,
    repositoryRoot,
    absolutePaths,
  });
}
