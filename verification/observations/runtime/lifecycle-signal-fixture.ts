import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { LifecycleFixture } from "./lifecycle-fixture.ts";

const root = process.argv[2];
assert.ok(root);
const fixture = new LifecycleFixture(root, "interrupt", {
  onInterruptReady: (): void => {
    process.send?.("ready");
  },
});
const error = await fixture.drive();
writeFileSync(
  join(root, "driver-finished.json"),
  JSON.stringify({ failure: error?.constructor.name ?? null }),
);
if (error?.constructor.name === "KeyboardInterrupt") process.exitCode = 130;
else {
  console.error(error ?? new Error("signal fixture completed without SIGINT"));
  process.exitCode = 1;
}
