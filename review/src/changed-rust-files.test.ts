import assert from "node:assert/strict";
import { renameSync, rmSync } from "node:fs";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { isCheckedRustPath, listChangedRustFiles } from "./changed-rust-files.ts";
import { TemporaryGitRepository } from "./test-support/temporary-git-repository.ts";

const renamedFileBody = Array.from(
  { length: 40 },
  (_, index) => `pub fn unchanged_helper_${index}() {}`,
).join("\n");

describe("listChangedRustFiles", () => {
  const repository = new TemporaryGitRepository();
  let base = "";
  let head = "";
  let main = "";

  before(() => {
    repository.write("crates/alpha/src/kept.rs", "pub fn kept() {}\n");
    repository.write("crates/alpha/src/deleted.rs", "pub fn deleted() {}\n");
    repository.write("crates/alpha/src/old_name.rs", renamedFileBody);
    repository.write("README.md", "# fixture\n");
    base = repository.commitAll("base");

    repository.git("switch", "--quiet", "-c", "change");
    repository.write("crates/alpha/src/kept.rs", "pub fn kept() { let _ = 1; }\n");
    rmSync(join(repository.root, "crates/alpha/src/deleted.rs"));
    renameSync(
      join(repository.root, "crates/alpha/src/old_name.rs"),
      join(repository.root, "crates/alpha/src/new_name.rs"),
    );
    repository.write("crates/beta/src/added.rs", "pub fn added() {}\n");
    repository.write("crates/beta/Cargo.toml", '[package]\nname = "beta"\n');
    repository.write("scripts/outside_crates.rs", "fn main() {}\n");
    head = repository.commitAll("change");

    // A commit on main after the branch point is not part of the change.
    repository.git("switch", "--quiet", "main");
    repository.write("crates/alpha/src/main_only.rs", "pub fn main_only() {}\n");
    main = repository.commitAll("main moves on");
  });

  after(() => repository.remove());

  test("lists added, modified and renamed Rust files under crates/ only", async () => {
    const changed = await listChangedRustFiles(repository.root, {
      baseObjectId: main,
      headObjectId: head,
    });

    assert.deepEqual(changed, {
      kind: "listed",
      repositoryRelativePaths: [
        "crates/alpha/src/kept.rs",
        "crates/alpha/src/new_name.rs",
        "crates/beta/src/added.rs",
      ],
    });
  });

  test("lists nothing for an empty range", async () => {
    const changed = await listChangedRustFiles(repository.root, {
      baseObjectId: base,
      headObjectId: base,
    });

    assert.deepEqual(changed, { kind: "listed", repositoryRelativePaths: [] });
  });

  test("reports a git failure for an object id the repository doesn't hold", async () => {
    const missing = "0".repeat(40);

    const changed = await listChangedRustFiles(repository.root, {
      baseObjectId: missing,
      headObjectId: head,
    });

    assert.equal(changed.kind, "git_failed");
    if (changed.kind !== "git_failed") return;
    assert.equal(
      changed.command,
      `git diff --name-only --diff-filter=ACMR -z ${missing}...${head} --`,
    );
  });
});

describe("isCheckedRustPath", () => {
  test("accepts Rust files under the top-level crates/ only", () => {
    const candidates = [
      "crates/alpha/src/lib.rs",
      "crates/alpha/tests/integration/main.rs",
      "crates/alpha/Cargo.toml",
      "review/test-fixtures/crates/alpha/src/lib.rs",
      "scripts/tool.rs",
    ];

    const checked = candidates.filter(isCheckedRustPath);

    assert.deepEqual(checked, [
      "crates/alpha/src/lib.rs",
      "crates/alpha/tests/integration/main.rs",
    ]);
  });
});
