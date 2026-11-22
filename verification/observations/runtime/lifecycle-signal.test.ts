import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFileSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { z } from "zod";
import { createFixtureDirectory } from "../../test-support/fixture-directory.ts";
import { readObservedJson, processIdentitySchema } from "../shared/evidence-models.ts";
import { lookupProcessIdentity, sameIdentity } from "./process-ownership.ts";

const ownedIdentitySchema = processIdentitySchema.omit({ parent: true });
const readySchema = z.object({
  identities: z.array(ownedIdentitySchema),
  profileBefore: z.string(),
});

test(
  "SIGINT awaits real owned Host/Router settlement and preserves disposable profile bytes",
  // Last-resort hang guard; readiness and settlement are decided by child events.
  { timeout: 180_000 },
  async (context): Promise<void> => {
    const root = createFixtureDirectory("lifecycle-sigint");
    const driver = spawn(
      process.execPath,
      [new URL("./lifecycle-signal-fixture.ts", import.meta.url).pathname, root],
      { stdio: ["ignore", "ignore", "pipe", "ipc"], detached: true },
    );
    const ready = join(root, "evidence/signal-ready.json");
    let diagnostics = "";
    const closed = once(driver, "close");
    let identities: z.infer<typeof ownedIdentitySchema>[] = [];
    try {
      assert.ok(driver.stderr, "signal driver diagnostics pipe is unavailable");
      driver.stderr.setEncoding("utf8").on("data", (chunk: string): void => {
        diagnostics += chunk;
      });
      const [message] = await Promise.race([
        once(driver, "message", { signal: context.signal }),
        closed.then((): never => {
          throw new Error(
            `signal driver exited before readiness (${driver.exitCode ?? driver.signalCode}): ${diagnostics.trim()}`,
          );
        }),
      ]);
      assert.equal(message, "ready");
      const before = readySchema.parse(readObservedJson(readFileSync(ready)));
      identities = before.identities;
      assert.equal(identities.length, 2);
      const original = Buffer.from(before.profileBefore, "base64");
      for (const identity of identities)
        assert.ok(sameIdentity(lookupProcessIdentity(identity.pid), identity));

      assert.equal(driver.kill("SIGINT"), true);
      await once(driver, "close", { signal: context.signal });

      assert.equal(driver.signalCode, null, "SIGINT must unwind instead of terminating directly");
      assert.equal(driver.exitCode, 130, diagnostics);
      const settlement = readObservedJson(
        readFileSync(join(root, "evidence/lifecycle-settlement.json")),
      );
      assert.equal(
        readObservedJson(readFileSync(join(root, "evidence/signal-owner-settlement.json")))
          .childrenReapedBeforeFixtureCleanup,
        true,
      );
      assert.equal(settlement.codexHomeUnchangedExactly, true);
      assert.deepEqual(readFileSync(join(root, "debug-profile")), original);
      for (const identity of identities) assert.equal(lookupProcessIdentity(identity.pid), null);
      assert.equal(
        readObservedJson(readFileSync(join(root, "driver-finished.json"))).failure,
        "KeyboardInterrupt",
      );
    } finally {
      if (driver.exitCode === null && driver.signalCode === null) driver.kill("SIGKILL");
      await closed;
      if (identities.length === 0 && existsSync(ready))
        identities = readySchema.parse(readObservedJson(readFileSync(ready))).identities;
      // Negative-control runs can leave only these exact recorded fixture children.
      for (const identity of identities)
        if (sameIdentity(lookupProcessIdentity(identity.pid), identity))
          process.kill(identity.pid, "SIGTERM");
      rmSync(root, { recursive: true, force: true });
    }
  },
);
