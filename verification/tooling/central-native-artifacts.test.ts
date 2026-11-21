import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { checkCentralNativeArtifacts } from "./check-central-native-artifacts.ts";

const graph = [
  "central_engine_model",
  "collaboration_central_core",
  "collaboration_central_tokio",
  "fleet_engine",
  "fleet_mail",
  "fleet_runtime",
  "fleet_schedules",
  "fleet_storage_schema",
  "fleet_store_sqlx",
  "fleet_wakes",
];

test("the native guard reads the full graph and rejects cross-chunk fixture SQL and test support", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "native-central-guard-"));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const archive = join(root, "central.rlib");
  const cargoFile = join(root, "cargo.jsonl");
  const writeMessages = async (features: string[] = []): Promise<void> => {
    const messages = [
      { reason: "build-script-executed" },
      ...graph.map((name) => ({
        reason: "compiler-artifact",
        target: { name },
        filenames: [archive],
        features,
      })),
    ];
    await writeFile(
      cargoFile,
      messages.map((message) => JSON.stringify(message)).join("\n") + "\n",
    );
  };

  // The positive and negative controls have the same graph and archive paths.
  await writeFile(archive, Buffer.alloc(2 * 64 * 1024, 0x00));
  await writeMessages();
  assert.equal(await checkCentralNativeArtifacts(cargoFile), graph.length);

  await writeFile(
    archive,
    Buffer.concat([Buffer.alloc(64 * 1024 - 5, 0x00), Buffer.from("ENGINE_PROBE_DUE_ROWS")]),
  );
  await assert.rejects(
    checkCentralNativeArtifacts(cargoFile),
    /contains fixture SQL: engine_probe_due_rows/,
  );

  await writeMessages(["test-support"]);
  await assert.rejects(checkCentralNativeArtifacts(cargoFile), /enables test-support/);

  await writeMessages();
  await rm(archive);
  await assert.rejects(checkCentralNativeArtifacts(cargoFile), {
    message: `Cannot read native Central archive ${archive}`,
  });
});
