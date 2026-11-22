import { spawnSync } from "node:child_process";
import {
  closeSync,
  createReadStream,
  mkdtempSync,
  openSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { z } from "zod";
import { engineFixtureSqlMarkers, findEngineFixtureSql } from "./central-engine-fixture-markers.ts";
import { runChecked } from "./process-command.ts";

const centralProgramPackage = "codex-router-cli";
const centralProgramTarget = "codex-router";
const cargoMetadata = z.object({
  target_directory: z.string(),
  workspace_members: z.array(z.string()),
  packages: z.array(z.object({ id: z.string(), name: z.string() })),
  resolve: z.object({
    nodes: z.array(
      z.object({
        id: z.string(),
        deps: z.array(
          z.object({
            pkg: z.string(),
            dep_kinds: z.array(z.object({ kind: z.string().nullable() })),
          }),
        ),
      }),
    ),
  }),
});
const cargoMessageReason = z.object({ reason: z.string() });
const cargoArtifact = z.object({
  package_id: z.string(),
  target: z.object({ name: z.string(), kind: z.array(z.string()) }),
  filenames: z.array(z.string()),
  features: z.array(z.string()),
  executable: z.string().nullable(),
});
const cargoBuildFinished = z.object({ success: z.boolean() });

const artifactChunkBytes = 64 * 1024;
const markerOverlapBytes = Math.max(...engineFixtureSqlMarkers.map((marker) => marker.length)) - 1;

async function checkFixtureSql(artifact: string, description: string): Promise<void> {
  let overlap = Buffer.alloc(0);
  try {
    if (realpathSync(artifact) !== artifact)
      throw new Error("artifact redirects outside its reported build path");
    for await (const chunk of createReadStream(artifact, { highWaterMark: artifactChunkBytes })) {
      const window = Buffer.concat([overlap, chunk]);
      const marker = findEngineFixtureSql(window);
      if (marker !== undefined) throw new Error(`contains fixture SQL: ${marker}`);
      overlap = window.subarray(-markerOverlapBytes);
    }
  } catch (cause) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    throw new Error(`Native Central ${description} at ${artifact}: ${detail}`, { cause });
  }
}

function resolveNativeWorkspace(metadata: z.infer<typeof cargoMetadata>): {
  programPackage: string;
  workspacePackages: Map<string, string>;
} {
  const members = new Set(metadata.workspace_members);
  const program = metadata.packages.find(
    ({ id, name }) => members.has(id) && name === centralProgramPackage,
  );
  if (program === undefined) throw new Error(`Missing workspace package ${centralProgramPackage}`);

  const nodes = new Map(metadata.resolve.nodes.map((node) => [node.id, node.deps]));
  const reachable = new Set([program.id]);
  for (const id of reachable) {
    const dependencies = nodes.get(id);
    if (dependencies === undefined) throw new Error(`Cargo metadata has no resolve node for ${id}`);
    for (const dependency of dependencies) {
      if (dependency.dep_kinds.some(({ kind }) => kind === null)) reachable.add(dependency.pkg);
    }
  }
  return {
    programPackage: program.id,
    workspacePackages: new Map(
      metadata.packages
        .filter(({ id }) => members.has(id) && reachable.has(id))
        .map(({ id, name }) => [id, name]),
    ),
  };
}

export async function checkCentralNativeArtifacts(
  cargoMetadataFile: string,
  cargoMessagesFile: string,
  hostTarget: string,
): Promise<number> {
  const metadata = cargoMetadata.parse(JSON.parse(readFileSync(cargoMetadataFile, "utf8")));
  const { programPackage, workspacePackages } = resolveNativeWorkspace(metadata);
  const buildDirectory = join(metadata.target_directory, hostTarget, "debug");
  const archiveDirectory = join(buildDirectory, "deps");
  const expectedExecutable = join(buildDirectory, centralProgramTarget);
  const coveredPackages = new Set<string>();
  const checkedArchives = new Set<string>();
  let executableChecked = false;
  let buildFinished = false;
  const input = createReadStream(cargoMessagesFile, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  let lineNumber = 0;
  try {
    for await (const line of lines) {
      lineNumber++;
      if (line === "") continue;
      let message: unknown;
      try {
        message = JSON.parse(line);
      } catch (cause) {
        throw new Error(`Invalid Cargo JSON in ${cargoMessagesFile} at line ${lineNumber}`, {
          cause,
        });
      }
      const { reason } = cargoMessageReason.parse(message);
      if (buildFinished) throw new Error("Cargo emitted a message after build-finished");
      if (reason === "build-finished") {
        if (!cargoBuildFinished.parse(message).success)
          throw new Error("Cargo reported a failed build");
        buildFinished = true;
      }
      if (reason !== "compiler-artifact") continue;
      const artifact = cargoArtifact.parse(message);
      const packageName = workspacePackages.get(artifact.package_id);
      if (packageName === undefined) continue;
      if (artifact.features.includes("test-support"))
        throw new Error(`Native Central artifact ${packageName} enables test-support`);

      for (const archive of artifact.filenames.filter((filename) => filename.endsWith(".rlib"))) {
        if (dirname(archive) !== archiveDirectory)
          throw new Error(
            `Native Central archive for ${packageName} is outside this build: ${archive}`,
          );
        if (!checkedArchives.has(archive))
          await checkFixtureSql(archive, `archive for ${packageName}`);
        checkedArchives.add(archive);
        coveredPackages.add(artifact.package_id);
      }

      if (
        artifact.package_id === programPackage &&
        artifact.target.name === centralProgramTarget &&
        artifact.target.kind.includes("bin")
      ) {
        if (
          artifact.executable !== expectedExecutable ||
          !artifact.filenames.includes(expectedExecutable)
        )
          throw new Error(
            `Native Central executable is outside this build: ${artifact.executable}`,
          );
        if (executableChecked)
          throw new Error("Cargo reported the native Central executable twice");
        await checkFixtureSql(expectedExecutable, "executable");
        executableChecked = true;
      }
    }
  } finally {
    lines.close();
    input.destroy();
  }
  if (!buildFinished) throw new Error("Missing successful Cargo build-finished record");
  const missing = [...workspacePackages]
    .filter(([id]) => !coveredPackages.has(id))
    .map(([, name]) => name);
  if (missing.length > 0)
    throw new Error(`Native Central graph is missing archives: ${missing.join(", ")}`);
  if (!executableChecked)
    throw new Error(`Missing Cargo artifact for native Central executable ${centralProgramTarget}`);
  return checkedArchives.size;
}

async function checkCentralNativeBuild(evidenceParent: string): Promise<void> {
  // Owning the build prevents a caller from pairing archives with an executable from a different target directory.
  const evidence = mkdtempSync(join(realpathSync(evidenceParent), "central-native-guard-"));
  console.log(`Native Central build evidence in ${evidence}`);
  const host = /^host: ([\w-]+)$/m.exec(runChecked(["rustc", "-vV"]))?.[1];
  if (host === undefined) throw new Error("rustc did not report a host target");
  const env = { ...process.env, CARGO_TARGET_DIR: join(evidence, "target") };
  const metadataFile = join(evidence, "metadata.json");
  const messagesFile = join(evidence, "cargo.jsonl");
  const metadata = runChecked(
    [
      "cargo",
      "metadata",
      "--locked",
      "--format-version",
      "1",
      "--no-default-features",
      "--filter-platform",
      host,
    ],
    { env },
  );
  writeFileSync(metadataFile, metadata);
  if (cargoMetadata.parse(JSON.parse(metadata)).target_directory !== env.CARGO_TARGET_DIR)
    throw new Error("Cargo metadata points outside the guard's build directory");

  const messages = openSync(messagesFile, "wx");
  try {
    const result = spawnSync(
      "cargo",
      [
        "build",
        "--locked",
        "-p",
        centralProgramPackage,
        "--bin",
        centralProgramTarget,
        "--no-default-features",
        "--target",
        host,
        "--message-format=json-render-diagnostics",
      ],
      { env, stdio: ["ignore", messages, "inherit"] },
    );
    if (result.error !== undefined) throw result.error;
    if (result.status !== 0)
      throw new Error(`Native Central Cargo build failed: ${result.status ?? result.signal}`);
  } finally {
    closeSync(messages);
  }
  const count = await checkCentralNativeArtifacts(metadataFile, messagesFile, host);
  console.log(
    `Native Central checked ${count} workspace archives and the ${centralProgramTarget} executable for fixture SQL`,
  );
}

if (import.meta.main) {
  const [evidenceParent] = process.argv.slice(2);
  if (evidenceParent === undefined || process.argv.length !== 3) {
    console.error("usage: check-central-native-artifacts.ts <existing-evidence-directory>");
    process.exitCode = 2;
  } else {
    try {
      await checkCentralNativeBuild(evidenceParent);
    } catch (error) {
      console.error(error);
      process.exitCode = 1;
    }
  }
}
