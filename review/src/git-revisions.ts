import { runGit, splitNulSeparated } from "./git-command.ts";

export type RepositoryRoot =
  | { readonly kind: "found"; readonly path: string }
  | { readonly kind: "not_a_repository"; readonly message: string };

export type CommitLookup =
  | { readonly kind: "resolved"; readonly objectId: string }
  | { readonly kind: "unknown_revision"; readonly revision: string };

export type DefaultBase =
  | { readonly kind: "resolved"; readonly objectId: string }
  | { readonly kind: "git_failed"; readonly command: string; readonly message: string };

/** Whether the working tree holds exactly the requested head for the files to be read. */
export type CheckoutState =
  | { readonly kind: "matches" }
  | { readonly kind: "different_commit"; readonly checkedOutObjectId: string | undefined }
  | { readonly kind: "modified_files"; readonly repositoryRelativePaths: readonly string[] }
  | { readonly kind: "git_failed"; readonly command: string; readonly message: string };

export async function findRepositoryRoot(workingDirectory: string): Promise<RepositoryRoot> {
  const output = await runGit(workingDirectory, ["rev-parse", "--show-toplevel"]);
  if (output.kind === "failed") return { kind: "not_a_repository", message: output.message };
  return { kind: "found", path: output.stdout.trim() };
}

/**
 * Resolves one revision to a commit object id. `--end-of-options` keeps a revision from ever
 * being read as a git option.
 */
export async function resolveCommit(
  repositoryRoot: string,
  revision: string,
): Promise<CommitLookup> {
  const output = await runGit(repositoryRoot, [
    "rev-parse",
    "--verify",
    "--quiet",
    "--end-of-options",
    `${revision}^{commit}`,
  ]);
  if (output.kind === "failed") return { kind: "unknown_revision", revision };
  return { kind: "resolved", objectId: output.stdout.trim() };
}

/** The default base is where the head left `origin/main`. */
export async function resolveDefaultBase(
  repositoryRoot: string,
  headObjectId: string,
): Promise<DefaultBase> {
  const output = await runGit(repositoryRoot, ["merge-base", "origin/main", headObjectId]);
  if (output.kind === "failed") {
    return { kind: "git_failed", command: output.command, message: output.message };
  }
  return { kind: "resolved", objectId: output.stdout.trim() };
}

/**
 * The checks read files from the working tree, so the checked-out commit must be the requested
 * head and none of the files to be read may differ from it, staged or not.
 */
export async function checkCheckoutMatchesHead(
  repositoryRoot: string,
  headObjectId: string,
  repositoryRelativePaths: readonly string[],
): Promise<CheckoutState> {
  const checkedOut = await resolveCommit(repositoryRoot, "HEAD");
  if (checkedOut.kind !== "resolved" || checkedOut.objectId !== headObjectId) {
    return {
      kind: "different_commit",
      checkedOutObjectId: checkedOut.kind === "resolved" ? checkedOut.objectId : undefined,
    };
  }
  // Literal pathspecs: a file name is matched as written, never as a glob.
  const output = await runGit(repositoryRoot, [
    "--literal-pathspecs",
    "diff",
    "--name-only",
    "-z",
    "HEAD",
    "--",
    ...repositoryRelativePaths,
  ]);
  if (output.kind === "failed") {
    return { kind: "git_failed", command: output.command, message: output.message };
  }
  const modified = splitNulSeparated(output.stdout).toSorted();
  if (modified.length > 0) return { kind: "modified_files", repositoryRelativePaths: modified };
  return { kind: "matches" };
}
