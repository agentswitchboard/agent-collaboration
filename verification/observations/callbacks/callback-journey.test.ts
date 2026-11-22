import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { field } from "../../exact-json.ts";
import { observedObject, publicObservationSchema } from "../shared/evidence-models.ts";
import { decodeEvidenceJson } from "../../evidence-json.ts";
import { driveCallbacks } from "./interaction-journey.ts";
import { runCallbackFixture } from "./callback-journey-fixture.ts";

test(
  "complete callback journey matches the original over HTTP and real SQLite readback",
  { timeout: 10_000 },
  async (context): Promise<void> => {
    const stdout: unknown[] = [];
    context.mock.method(console, "log", (text: string): void => {
      stdout.push(decodeEvidenceJson(text));
    });
    const result = await runCallbackFixture(
      async (plan): Promise<{ readonly exit: number; readonly stdout: readonly unknown[] }> => {
        await driveCallbacks(plan);
        return { exit: 0, stdout };
      },
    );
    assert.deepEqual(
      result,
      decodeEvidenceJson(
        readFileSync(new URL("./baselines/callback-journey.bin", import.meta.url)),
      ),
    );
  },
);

for (const fixture of ["marker", "bareRefusal", "numberedGap", "oversize"] as const) {
  test(
    "callback journey handles the provider observation " + fixture,
    { timeout: 10_000 },
    async (context): Promise<void> => {
      const stdout: unknown[] = [];
      context.mock.method(console, "log", (value: string): void => {
        stdout.push(decodeEvidenceJson(value));
      });
      const run = runCallbackFixture(async (plan) => {
        await driveCallbacks(plan);
        return { exit: 0, stdout };
      }, fixture);
      if (fixture === "marker") {
        const baseline = decodeEvidenceJson(
          readFileSync(new URL("./baselines/callback-journey.bin", import.meta.url)),
        );
        const retained = publicObservationSchema.parse(
          field(await run, "files", "callback-events.json"),
        );
        const original = publicObservationSchema.parse(
          field(baseline, "files", "callback-events.json"),
        );
        assert.deepEqual(retained.events, original.events);
        assert.equal(observedObject(retained["observation"])["epoch"], 7n);
      } else {
        await assert.rejects(run, /observer resync or oversized event gap/);
      }
    },
  );
}
