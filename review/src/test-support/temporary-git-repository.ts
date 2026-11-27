import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

/** A throwaway git repository inside the test's own temporary directory. */
export class TemporaryGitRepository {
  readonly root: string;

  constructor() {
    this.root = mkdtempSync(join(tmpdir(), "review-checks-repository-"));
    this.git("init", "--quiet", "--initial-branch=main");
  }

  git(...gitArguments: readonly string[]): string {
    return execFileSync(
      "git",
      [
        "-c",
        "user.name=Review Checks Test",
        "-c",
        "user.email=review-checks@example.invalid",
        "-c",
        "commit.gpgsign=false",
        ...gitArguments,
      ],
      { cwd: this.root, encoding: "utf8" },
    ).trim();
  }

  write(repositoryRelativePath: string, body: string): void {
    const path = join(this.root, repositoryRelativePath);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, body);
  }

  commitAll(message: string): string {
    this.git("add", "--all");
    this.git("commit", "--quiet", "-m", message);
    return this.git("rev-parse", "HEAD");
  }

  remove(): void {
    rmSync(this.root, { recursive: true, force: true });
  }
}
