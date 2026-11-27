import { execFile } from "node:child_process";
import { promisify } from "node:util";

const runExecFile = promisify(execFile);

export type GitOutput =
  | { readonly kind: "succeeded"; readonly stdout: string }
  | { readonly kind: "failed"; readonly command: string; readonly message: string };

/** Runs git with an argument vector (never a shell string) inside the repository. */
export async function runGit(
  repositoryRoot: string,
  gitArguments: readonly string[],
): Promise<GitOutput> {
  const command = `git ${gitArguments.join(" ")}`;
  try {
    const { stdout } = await runExecFile("git", gitArguments, {
      cwd: repositoryRoot,
      maxBuffer: 16 * 1024 * 1024,
    });
    return { kind: "succeeded", stdout };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message.trim() : String(error);
    return { kind: "failed", command, message };
  }
}

/** Splits `-z` output into its entries, dropping the trailing empty one. */
export function splitNulSeparated(stdout: string): readonly string[] {
  return stdout.split("\0").filter((entry) => entry.length > 0);
}
