import { readFileSync } from "node:fs";
import { z } from "zod";
import { engineFixtureSqlMarkers } from "./central-engine-fixture-markers.ts";

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

export function checkCentralNativeArtifacts(messages: string): number {
  const artifacts = new Set<string>();
  for (const line of messages.split("\n").filter(Boolean)) {
    const parsed = cargoArtifact.safeParse(JSON.parse(line));
    if (!parsed.success) continue;
    const { target, features, filenames } = parsed.data;
    if (!centralGraph.includes(target.name)) continue;
    for (const archive of filenames.filter((file) => file.endsWith(".rlib"))) {
      if (features.includes("test-support"))
        throw new Error(`Native Central artifact ${target.name} enables test-support`);
      const content = readFileSync(archive);
      for (const marker of engineFixtureSqlMarkers)
        if (content.includes(marker))
          throw new Error(`Native Central artifact ${target.name} contains fixture SQL: ${marker}`);
      artifacts.add(target.name);
    }
  }
  const missing = centralGraph.filter((name) => !artifacts.has(name));
  if (missing.length > 0) throw new Error(`Native Central graph is missing archives: ${missing}`);
  return artifacts.size;
}

if (import.meta.main) {
  const count = checkCentralNativeArtifacts(readFileSync(0, "utf8"));
  console.log(`Native Central production graph: ${count} archives contain no Engine fixture SQL`);
}
