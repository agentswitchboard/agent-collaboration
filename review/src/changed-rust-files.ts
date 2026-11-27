import { runGit, splitNulSeparated } from "./git-command.ts";

/** Two commits already resolved to object ids; never raw user input. */
export interface ResolvedRange {
  readonly baseObjectId: string;
  readonly headObjectId: string;
}

export type ChangedRustFiles =
  | { readonly kind: "listed"; readonly repositoryRelativePaths: readonly string[] }
  | { readonly kind: "git_failed"; readonly command: string; readonly message: string };

/** A changed path is checked when it is a Rust file anywhere under the top-level `crates/`. */
export function isCheckedRustPath(repositoryRelativePath: string): boolean {
  return repositoryRelativePath.startsWith("crates/") && repositoryRelativePath.endsWith(".rs");
}

/**
 * Lists the Rust files under `crates/` that the range adds, copies, modifies or renames,
 * as exact repository-relative paths. Deleted files are left out: there is nothing to read.
 */
export async function listChangedRustFiles(
  repositoryRoot: string,
  range: ResolvedRange,
): Promise<ChangedRustFiles> {
  const output = await runGit(repositoryRoot, [
    "diff",
    "--name-only",
    "--diff-filter=ACMR",
    "-z",
    `${range.baseObjectId}...${range.headObjectId}`,
    "--",
  ]);
  if (output.kind === "failed") {
    return { kind: "git_failed", command: output.command, message: output.message };
  }
  const repositoryRelativePaths = splitNulSeparated(output.stdout)
    .filter(isCheckedRustPath)
    .toSorted();
  return { kind: "listed", repositoryRelativePaths };
}
