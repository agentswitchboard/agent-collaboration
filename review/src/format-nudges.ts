import type {
  LastJudgment,
  ReviewNudge,
  ReviewReport,
  UnavailablePiece,
} from "./run-review-checks.ts";

const namedEscapes: Readonly<Record<string, string>> = { "\n": "\\n", "\r": "\\r", "\t": "\\t" };

/** C0 controls, DEL and C1 controls: characters that can break a line or drive a terminal. */
function isControlCharacter(character: string): boolean {
  const codePoint = character.codePointAt(0) ?? 0;
  return codePoint < 0x20 || (codePoint >= 0x7f && codePoint <= 0x9f);
}

function escapeControlCharacters(text: string, escape: (character: string) => string): string {
  return Array.from(text, (character) =>
    isControlCharacter(character) ? escape(character) : character,
  ).join("");
}

/**
 * Escapes control characters so a file name or message can't break a line of output or drive
 * the terminal. Only the display changes; the paths given to git and the SDK stay exact.
 */
export function displayText(text: string): string {
  return escapeControlCharacters(
    text,
    (character) =>
      namedEscapes[character] ??
      `\\u${(character.codePointAt(0) ?? 0).toString(16).padStart(4, "0")}`,
  );
}

function formatPlace(file: string, line: bigint, rule: string): string {
  return `${displayText(file)}:${line}  ${displayText(rule)}`;
}

function formatJudgment(judgment: LastJudgment | undefined): string {
  if (judgment === undefined) return "";
  return `  (p(${displayText(judgment.question)})=${judgment.probability.toFixed(2)})`;
}

export function formatNudge(nudge: ReviewNudge): string {
  const place = formatPlace(nudge.file, nudge.line, nudge.rule);
  switch (nudge.kind) {
    case "finding":
      return `${place}  ${displayText(nudge.message)}${formatJudgment(nudge.judgment)}`;
    case "uncertain":
      return `${place}  uncertain: read the code and decide${formatJudgment(nudge.judgment)}`;
    default:
      return assertNever(nudge);
  }
}

function assertNever(value: never): never {
  throw new Error(`Unhandled nudge: ${JSON.stringify(value)}`);
}

export function formatUnavailablePiece(piece: UnavailablePiece): string {
  return `${formatPlace(piece.file, piece.line, piece.rule)}  unavailable (${piece.reason})`;
}

export function formatSummary(report: ReviewReport): string {
  const findings = report.nudges.filter((nudge) => nudge.kind === "finding").length;
  const uncertain = report.nudges.length - findings;
  return [
    `${report.piecesSelected} pieces selected`,
    `${report.piecesEvaluated} evaluated`,
    `${report.unavailable.length} unavailable`,
    `${findings} findings`,
    `${uncertain} uncertain`,
    `${report.modelRequests} model requests`,
  ].join(", ");
}

/** Every line a report prints: nudges, unavailable pieces, the CLI fallback once, the summary. */
export function formatReport(
  report: ReviewReport,
  absolutePaths: readonly string[],
): readonly string[] {
  if (report.piecesSelected === 0) {
    return [`No rule pieces selected in ${absolutePaths.length} files: nothing to check.`];
  }
  return [
    ...report.nudges.map(formatNudge),
    ...report.unavailable.map(formatUnavailablePiece),
    ...formatCliFallback(report.unavailable, absolutePaths),
    formatSummary(report),
  ];
}

/**
 * Quotes one argument for a POSIX shell. An argument holding a control character uses ANSI-C
 * quoting (`$'...'`), so the command stays on one line and still pastes exactly.
 */
function shellQuote(argument: string): string {
  if (/^[\w./@:+-]+$/u.test(argument)) return argument;
  if (!Array.from(argument).some(isControlCharacter)) {
    return `'${argument.replaceAll("'", "'\\''")}'`;
  }
  const escaped = escapeControlCharacters(
    argument.replaceAll("\\", "\\\\").replaceAll("'", "\\'"),
    (character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      // `\x` names a byte; a C1 control is two UTF-8 bytes, so it is named by code point.
      return codePoint < 0x80
        ? `\\x${codePoint.toString(16).padStart(2, "0")}`
        : `\\u${codePoint.toString(16).padStart(4, "0")}`;
    },
  );
  return `$'${escaped}'`;
}

/**
 * Names the `sunstack` CLI command that evaluates the same files, when some pieces were not
 * evaluated. The script never runs that command itself.
 */
export function formatCliFallback(
  unavailable: readonly UnavailablePiece[],
  absolutePaths: readonly string[],
): readonly string[] {
  if (unavailable.length === 0) return [];
  const command = [
    "sunstack",
    "lint",
    "--config",
    "review/sunstack.lint.json",
    ...absolutePaths.map(shellQuote),
  ].join(" ");
  return [
    `${unavailable.length} pieces were not evaluated. Read them yourself, or run the same checks with the sunstack CLI from the repository root:`,
    `  ${command}`,
  ];
}
