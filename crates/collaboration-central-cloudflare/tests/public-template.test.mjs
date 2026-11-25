import test from "node:test";
import assert from "node:assert/strict";
import template from "../cloudflare.config.ts";
import linkTemplate from "../cloudflare-link.config.ts";

test("the public deployment template binds each workspace store to its SQLite object class", () => {
  assert.deepEqual(template.worker.env.WORKSPACE_STORE, {
    type: "durable-object",
    worker: "replace-worker-name",
    exportName: "WorkspaceStoreObject",
  });
  assert.deepEqual(template.worker.exports.WorkspaceStoreObject, {
    type: "durable-object",
    storage: "sqlite",
  });
});

test("only the storage template binds the named Sign-in keys entrypoint", () => {
  assert.deepEqual(template.worker.env.SIGN_IN_KEYS, {
    type: "worker",
    worker: "replace-sign-in-worker-name",
    exportName: "SignInJwks",
  });
  assert.equal(Object.hasOwn(linkTemplate.worker.env, "SIGN_IN_KEYS"), false);
});
