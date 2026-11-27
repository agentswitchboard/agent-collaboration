import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { checkCheckoutMatchesHead, resolveCommit } from "./git-revisions.ts";
import { TemporaryGitRepository } from "./test-support/temporary-git-repository.ts";

describe("resolveCommit", () => {
  const repository = new TemporaryGitRepository();
  let first = "";

  before(() => {
    repository.write("crates/alpha/src/lib.rs", "pub fn alpha() {}\n");
    first = repository.commitAll("first");
  });

  after(() => repository.remove());

  test("resolves a revision to its commit object id", async () => {
    const lookup = await resolveCommit(repository.root, "main");

    assert.deepEqual(lookup, { kind: "resolved", objectId: first });
  });

  test("reports an unknown revision", async () => {
    const lookup = await resolveCommit(repository.root, "no-such-branch");

    assert.deepEqual(lookup, { kind: "unknown_revision", revision: "no-such-branch" });
  });

  test("never reads a revision as a git option", async () => {
    const lookup = await resolveCommit(repository.root, "--output=escaped.txt");

    assert.equal(lookup.kind, "unknown_revision");
    assert.equal(existsSync(join(repository.root, "escaped.txt")), false);
  });
});

describe("checkCheckoutMatchesHead", () => {
  const repository = new TemporaryGitRepository();
  let first = "";
  let second = "";
  const selected = ["crates/alpha/src/lib.rs"];

  before(() => {
    repository.write("crates/alpha/src/lib.rs", "pub fn alpha() {}\n");
    repository.write("crates/alpha/src/other.rs", "pub fn other() {}\n");
    first = repository.commitAll("first");
    repository.write("crates/alpha/src/lib.rs", "pub fn alpha() { let _ = 2; }\n");
    second = repository.commitAll("second");
  });

  after(() => repository.remove());

  test("matches when the head is checked out and the selected files are clean", async () => {
    repository.write("crates/alpha/src/other.rs", "pub fn other() { let _ = 3; }\n");

    const state = await checkCheckoutMatchesHead(repository.root, second, selected);

    assert.deepEqual(state, { kind: "matches" });
    repository.git("checkout", "--quiet", "--", "crates/alpha/src/other.rs");
  });

  test("refuses a different checked-out commit", async () => {
    const state = await checkCheckoutMatchesHead(repository.root, first, selected);

    assert.deepEqual(state, { kind: "different_commit", checkedOutObjectId: second });
  });

  test("refuses an unstaged edit to a selected file", async () => {
    repository.write("crates/alpha/src/lib.rs", "pub fn edited() {}\n");

    const state = await checkCheckoutMatchesHead(repository.root, second, selected);

    assert.deepEqual(state, { kind: "modified_files", repositoryRelativePaths: selected });
    repository.git("checkout", "--quiet", "--", "crates/alpha/src/lib.rs");
  });

  test("refuses a staged edit to a selected file", async () => {
    repository.write("crates/alpha/src/lib.rs", "pub fn staged() {}\n");
    repository.git("add", "crates/alpha/src/lib.rs");

    const state = await checkCheckoutMatchesHead(repository.root, second, selected);

    assert.deepEqual(state, { kind: "modified_files", repositoryRelativePaths: selected });
    repository.git("restore", "--staged", "--worktree", "--", "crates/alpha/src/lib.rs");
  });
});
