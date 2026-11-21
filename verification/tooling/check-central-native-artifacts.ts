import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { z } from "zod";
import { engineFixtureSqlMarkers, findEngineFixtureSql } from "./central-engine-fixture-markers.ts";

const cargoArtifact = z.object({
  reason: z.literal("compiler-artifact"),
  target: z.object({ name: z.string() }),
  filenames: z.array(z.string()),
  features: z.array(z.string()),
});

const centralGraph = [
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

const archiveChunkBytes = 64 * 1024;
const markerOverlapBytes = Math.max(...engineFixtureSqlMarkers.map((marker) => marker.length)) - 1;

async function findArchiveFixtureSql(archive: string): Promise<string | undefined> {
  let overlap = Buffer.alloc(0);
  try {
    for await (const chunk of createReadStream(archive, { highWaterMark: archiveChunkBytes })) {
      const window = Buffer.concat([overlap, chunk]);
      const marker = findEngineFixtureSql(window);
      if (marker !== undefined) return marker;
      overlap = window.subarray(-markerOverlapBytes);
    }
    return undefined;
  } catch (cause) {
    throw new Error(`Cannot read native Central archive ${archive}`, { cause });
  }
}

export async function checkCentralNativeArtifacts(cargoMessagesFile: string): Promise<number> {
  const artifacts = new Set<string>();
  const input = createReadStream(cargoMessagesFile, { encoding: "utf8" });
  // Use a regular file as the transport and retain only the current Cargo record.
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
      const parsed = cargoArtifact.safeParse(message);
      if (!parsed.success) continue;
      const { target, features, filenames } = parsed.data;
      if (!centralGraph.includes(target.name)) continue;
      for (const archive of filenames.filter((file) => file.endsWith(".rlib"))) {
        if (features.includes("test-support"))
          throw new Error(`Native Central artifact ${target.name} enables test-support`);
        const marker = await findArchiveFixtureSql(archive);
        if (marker !== undefined)
          throw new Error(
            `Native Central artifact ${target.name} contains fixture SQL: ${marker} (${archive})`,
          );
        artifacts.add(target.name);
      }
    }
  } finally {
    lines.close();
    input.destroy();
  }
  const missing = centralGraph.filter((name) => !artifacts.has(name));
  if (missing.length > 0) throw new Error(`Native Central graph is missing archives: ${missing}`);
  return artifacts.size;
}

if (import.meta.main) {
  const [cargoMessagesFile] = process.argv.slice(2);
  if (cargoMessagesFile === undefined || process.argv.length !== 3) {
    console.error("usage: check-central-native-artifacts.ts <cargo-messages.jsonl>");
    process.exitCode = 2;
  } else {
    try {
      const count = await checkCentralNativeArtifacts(cargoMessagesFile);
      console.log(
        `Native Central production graph: ${count} archives contain no Engine fixture SQL`,
      );
    } catch (error) {
      console.error(error);
      process.exitCode = 1;
    }
  }
}
