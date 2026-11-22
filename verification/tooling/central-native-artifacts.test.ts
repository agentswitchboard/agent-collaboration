import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test, type TestContext } from "node:test";
import { checkCentralNativeArtifacts } from "./check-central-native-artifacts.ts";

const host = "aarch64-apple-darwin";
const program = "path+file:///workspace/cli#codex-router-cli@1.0.0";
const central = "path+file:///workspace/central#central-owner@1.0.0";
const newCentralDependency = "path+file:///workspace/new#fleet-added-later@1.0.0";
const development = "path+file:///workspace/development#development-only@1.0.0";
const build = "path+file:///workspace/build#build-only@1.0.0";
const external = "registry+https://registry.example/index#bridge@1.0.0";

function dependency(
  pkg: string,
  kind: string | null = null,
): {
  pkg: string;
  dep_kinds: { kind: string | null }[];
} {
  return { pkg, dep_kinds: [{ kind }] };
}

function compilerArtifact(
  packageId: string,
  targetName: string,
  filename: string,
  features: string[] = [],
  executable: string | null = null,
): object {
  return {
    reason: "compiler-artifact",
    package_id: packageId,
    target: { name: targetName, kind: [executable === null ? "lib" : "bin"] },
    filenames: [filename],
    features,
    executable,
  };
}

async function fixture(context: TestContext): Promise<{
  check: () => Promise<number>;
  writeBuild: (records: readonly object[]) => Promise<void>;
  records: object[];
  archive: string;
  executable: string;
  foreignExecutable: string;
}> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "native-central-guard-")));
  context.after(async () => rm(root, { recursive: true, force: true }));
  const targetDirectory = join(root, "target");
  const buildDirectory = join(targetDirectory, host, "debug");
  const archiveDirectory = join(buildDirectory, "deps");
  const metadataFile = join(root, "metadata.json");
  const messagesFile = join(root, "cargo.jsonl");
  const programArchive = join(archiveDirectory, "libcodex_router_cli.rlib");
  const ownerArchive = join(archiveDirectory, "libcentral_owner.rlib");
  const archive = join(archiveDirectory, "libfleet_added_later.rlib");
  const executable = join(buildDirectory, "codex-router");
  const foreignExecutable = join(root, "other-target", host, "debug", "codex-router");
  await mkdir(archiveDirectory, { recursive: true });
  await mkdir(dirname(foreignExecutable), { recursive: true });
  for (const filename of [programArchive, ownerArchive, archive, executable, foreignExecutable]) {
    await writeFile(filename, Buffer.alloc(2 * 64 * 1024, 0));
  }
  await writeFile(
    metadataFile,
    JSON.stringify({
      target_directory: targetDirectory,
      workspace_members: [program, central, newCentralDependency, development, build],
      packages: [
        { id: program, name: "codex-router-cli" },
        { id: central, name: "central-owner" },
        { id: newCentralDependency, name: "fleet-added-later" },
        { id: development, name: "development-only" },
        { id: build, name: "build-only" },
        { id: external, name: "bridge" },
      ],
      resolve: {
        nodes: [
          {
            id: program,
            deps: [dependency(central), dependency(development, "dev"), dependency(build, "build")],
          },
          { id: central, deps: [dependency(external)] },
          { id: external, deps: [dependency(newCentralDependency)] },
          { id: newCentralDependency, deps: [] },
          { id: development, deps: [] },
          { id: build, deps: [] },
        ],
      },
    }),
  );
  const records = [
    compilerArtifact(program, "codex_router_cli", programArchive),
    compilerArtifact(central, "central_owner", ownerArchive),
    compilerArtifact(newCentralDependency, "fleet_added_later", archive),
    compilerArtifact(program, "codex-router", executable, [], executable),
  ];
  const writeBuild = async (artifacts: readonly object[]): Promise<void> => {
    const messages = [
      { reason: "build-script-executed" },
      ...artifacts,
      { reason: "build-finished", success: true },
    ];
    await writeFile(
      messagesFile,
      messages.map((message) => JSON.stringify(message)).join("\n") + "\n",
    );
  };
  await writeBuild(records);
  return {
    check: () => checkCentralNativeArtifacts(metadataFile, messagesFile, host),
    writeBuild,
    records,
    archive,
    executable,
    foreignExecutable,
  };
}

test("the native guard scans the normal workspace graph and the shipping executable", async (context) => {
  const build = await fixture(context);
  assert.equal(await build.check(), 3);
});

test("the native guard rejects test-support on a newly reachable workspace package", async (context) => {
  const build = await fixture(context);
  await build.writeBuild(
    build.records.with(
      2,
      compilerArtifact(newCentralDependency, "fleet_added_later", build.archive, ["test-support"]),
    ),
  );
  await assert.rejects(build.check, /fleet-added-later enables test-support/);
});

test("the native guard rejects cross-chunk fixture SQL in a covered archive", async (context) => {
  const build = await fixture(context);
  await writeFile(
    build.archive,
    Buffer.concat([Buffer.alloc(64 * 1024 - 5, 0), Buffer.from("ENGINE_PROBE_DUE_ROWS")]),
  );
  await assert.rejects(
    build.check,
    /archive for fleet-added-later .*contains fixture SQL: engine_probe_due_rows/,
  );
});

test("the native guard rejects a missing workspace archive even when a different package reports its target name", async (context) => {
  const build = await fixture(context);
  await build.writeBuild(
    build.records.with(2, compilerArtifact(external, "fleet_added_later", build.archive)),
  );
  await assert.rejects(build.check, /missing archives: fleet-added-later/);
});

test("the native guard rejects a clean executable from a different build", async (context) => {
  const build = await fixture(context);
  await build.writeBuild(
    build.records.with(
      3,
      compilerArtifact(
        program,
        "codex-router",
        build.foreignExecutable,
        [],
        build.foreignExecutable,
      ),
    ),
  );
  await assert.rejects(build.check, /executable is outside this build/);
});

test("the native guard rejects redirecting the reported executable to a different build", async (context) => {
  const build = await fixture(context);
  await rm(build.executable);
  await symlink(build.foreignExecutable, build.executable);
  await assert.rejects(build.check, /artifact redirects outside its reported build path/);
});

test("the native guard rejects fixture SQL found only in the shipping executable", async (context) => {
  const build = await fixture(context);
  await writeFile(build.executable, "ENGINE_PROBE_DUE_ROWS");
  await assert.rejects(build.check, /executable .*contains fixture SQL: engine_probe_due_rows/);
});

test("the native guard fails when a reported archive or the shipping executable record is missing", async (context) => {
  const build = await fixture(context);
  await rm(build.archive);
  await assert.rejects(build.check, /archive for fleet-added-later/);
  await writeFile(build.archive, "");
  await build.writeBuild(build.records.slice(0, -1));
  await assert.rejects(build.check, /Missing Cargo artifact for native Central executable/);
});
