import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { after, before, test } from "node:test";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { z } from "zod";
import { seedHistory } from "./engine-due-views-fixture.ts";

const source = new URL("../../", import.meta.url);
const migrations = new URL("crates/fleet-storage-schema/migrations/central/", source);
const dueMigration = "0005_engine_due_views.sql";
const dueSchema = readFileSync(new URL(dueMigration, migrations), "utf8");
const oldSchema = readdirSync(migrations)
  .filter((name): boolean => name < dueMigration)
  .sort()
  .map((name): string => readFileSync(new URL(name, migrations), "utf8"));
const nextAlarm = readFileSync(
  new URL("crates/fleet-engine/src/queries/read_alarm_inputs.sql", source),
  "utf8",
);
const oldAlarm = readFileSync(
  new URL("crates/collaboration-central-core/tests/fixtures/main_engine_alarm_inputs.sql", source),
  "utf8",
);
const overlay = readFileSync(
  new URL(
    "crates/fleet-storage-schema/tests/fixtures/engine_migrations/0001_engine_probe.sql",
    source,
  ),
  "utf8",
);
const grid = "on_grid_due?: i64";
const legacy = "legacy_due?: i64";

const resultSchema = z.object({
  rows: z.array(z.record(z.string(), z.union([z.null(), z.string(), z.number()]))),
  rowsRead: z.number(),
  rowsWritten: z.number(),
});
type Result = z.infer<typeof resultSchema>;

// Only the owning fixture object receives SQL. The same production statement
// is loaded by the Engine's native compile witness and its DO transaction.
const workerSource = [
  "export class DueViewObject {",
  "  constructor(ctx) { this.ctx = ctx; this.sql = ctx.storage.sql; }",
  "  async fetch(request) {",
  "    const { sql, setup } = await request.json();",
  "    if (setup) {",
  "      this.ctx.storage.transactionSync(() => this.sql.exec(sql).toArray());",
  "      return Response.json({ rows: [], rowsRead: 0, rowsWritten: 0 });",
  "    }",
  "    const cursor = this.sql.exec(sql);",
  "    const rows = cursor.toArray();",
  "    return Response.json({ rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten });",
  "  }",
  "}",
  "export default { fetch(request, env) {",
  "  return env.STORE.getByName(new URL(request.url).pathname.slice(1)).fetch(request);",
  "} };",
].join("\n");

let runtime: Miniflare;
let origin: URL;
let scratch: string;
before(async (): Promise<void> => {
  scratch = mkdtempSync(join(tmpdir(), "engine-due-views-"));
  runtime = new Miniflare(
    convertV4MiniflareOptions({
      host: "127.0.0.1",
      port: 0,
      cf: false,
      resourcePersistencePath: scratch,
      workers: [
        {
          name: "due-view-verification",
          script: workerSource,
          modules: true,
          compatibilityDate: "2026-08-21",
          durableObjects: { STORE: { className: "DueViewObject", useSQLite: true } },
          outboundService: (): never => {
            throw new Error("unexpected outbound request");
          },
        },
      ],
    }),
  );
  origin = await runtime.ready;
});
after(async (): Promise<void> => {
  await runtime?.dispose();
  if (scratch !== undefined) rmSync(scratch, { recursive: true, force: true });
});

async function run(object: string, sql: string, setup = false): Promise<Result> {
  const response = await fetch(new URL("/" + object, origin), {
    method: "POST",
    headers: { "Content-Type": "application/json", Connection: "close" },
    body: JSON.stringify({ sql, setup }),
  });
  if (!response.ok) throw new Error(object + ": " + (await response.text()));
  return resultSchema.parse(await response.json());
}

async function prepare(object: string, steps: readonly string[]): Promise<void> {
  for (const sql of steps) await run(object, sql, true);
}

function pair(rows: Result["rows"], current: boolean): readonly unknown[] {
  assert.equal(rows.length, 1);
  const [row] = rows;
  assert.ok(row !== undefined);
  return current ? [row[grid], row[legacy]] : [row["on_grid_due"], row["legacy_due"]];
}

function replaceRequired(sql: string, from: string | RegExp, to: string): string {
  const result = sql.replace(from, to);
  assert.notEqual(result, sql, "the negative control must change the production DDL");
  return result;
}

async function plan(object: string): Promise<string> {
  const { rows } = await run(object, "EXPLAIN QUERY PLAN " + nextAlarm);
  return rows.map((row): string => z.string().parse(row["detail"])).join("\n");
}

test("the shipped Workerd alarm fits 330 reads and loses the bound without the expiry index", async (context): Promise<void> => {
  const fixtures = seedHistory(10_000, 50, 50);
  await prepare("indexed", [...oldSchema, dueSchema, ...fixtures]);
  const actual = await run("indexed", nextAlarm);
  assert.deepEqual(pair(actual.rows, true), [300_000, 300_000]);
  assert.equal(actual.rowsWritten, 0);
  assert.ok(actual.rowsRead <= 330, JSON.stringify(actual));

  const paths = await plan("indexed");
  for (const path of [
    /SCAN automation_wakes USING INDEX wakes_due\b/,
    /SCAN automation_wakes USING INDEX wakes_unknown_state_due\b/,
    /SEARCH automation_schedules USING INDEX automations_due\b/,
    /SCAN automation_schedules USING INDEX automations_unknown_state_due\b/,
    /SEARCH s USING INDEX messages_cancel_due\b/,
    /SEARCH s USING INDEX messages_system_expiry_due\b/,
    /SEARCH s USING INDEX workspace_drains_due\b/,
    /SEARCH engine_work_attempts USING INDEX central_work_attempts_subject\b/,
    /mail_notification_windows USING COVERING INDEX notification_windows_due\b/,
  ])
    assert.match(paths, path);
  assert.doesNotMatch(paths, /SCAN automation_(?:wakes|schedules)$/m);

  const noExpiry = replaceRequired(dueSchema, /CREATE INDEX messages_system_expiry_due [^;]+;/, "");
  await prepare("lost-expiry", [...oldSchema, noExpiry, ...fixtures]);
  const lostExpiry = await run("lost-expiry", nextAlarm);
  assert.deepEqual(lostExpiry.rows, actual.rows);
  assert.ok(lostExpiry.rowsRead > 330, JSON.stringify(lostExpiry));
  assert.doesNotMatch(await plan("lost-expiry"), /messages_system_expiry_due/);

  const noWake = replaceRequired(dueSchema, "INDEXED BY wakes_due", "NOT INDEXED");
  await prepare("lost-wake", [...oldSchema, noWake, ...fixtures]);
  const lostWake = await run("lost-wake", nextAlarm);
  assert.deepEqual(lostWake.rows, actual.rows);
  assert.ok(lostWake.rowsRead > 330, JSON.stringify(lostWake));
  context.diagnostic(
    JSON.stringify({
      indexed: actual.rowsRead,
      lostExpiry: lostExpiry.rowsRead,
      lostWake: lostWake.rowsRead,
    }),
  );
});

test("a null source ID still masks its wake, and the old ID-based rule is detected", async (): Promise<void> => {
  const badSchema = replaceRequired(
    dueSchema,
    /SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls/g,
    "SELECT CASE WHEN uses_subject_attempt THEN 1 END AS control_present, due_at_ms FROM engine_due_attempt_controls",
  );
  const fixtures = [
    "INSERT INTO signin_organization_identity VALUES ('fixture-stamp','fixture',1,1)",
    "INSERT INTO automation_wakes VALUES ('01890f2e-7b4c-7cc0-98c4-000000000022','target','1970-01-01T00:05:00.000Z','wake','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
    "INSERT INTO engine_work_attempts VALUES (NULL,'engine_source','wake_timer','retired',1,NULL,'statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
  ];
  await prepare("masked", [...oldSchema, dueSchema, ...fixtures]);
  await prepare("mask-regression", [...oldSchema, badSchema, ...fixtures]);
  assert.deepEqual(pair((await run("masked", oldAlarm)).rows, false), [null, null]);
  assert.deepEqual(pair((await run("masked", nextAlarm)).rows, true), [null, null]);
  assert.deepEqual(pair((await run("mask-regression", nextAlarm)).rows, true), [300_000, null]);
});

function timeStock(statement: StatementSync): number {
  for (let warm = 0; warm < 15; warm += 1) statement.all();
  const started = performance.now();
  for (let iteration = 0; iteration < 1_000; iteration += 1) statement.all();
  return performance.now() - started;
}

function stockCounters(database: string, sql: string): Record<string, number> | null {
  const result = spawnSync("sqlite3", ["-batch", database, ".stats stmt", sql], {
    encoding: "utf8",
  });
  if (result.status !== 0) return null;
  return Object.fromEntries(
    [...result.stdout.matchAll(/^(Fullscan Steps|Virtual Machine Steps):\s+(\d+)$/gm)].map(
      ([, name, count]): [string, number] => [name ?? "", Number(count)],
    ),
  );
}
const shortlists = [
  ["fleet-wakes", "read_due_wakes.sql", "main-wakes.sql"],
  ["fleet-schedules", "read_due_schedules.sql", "main-schedules.sql"],
  ["fleet-mail", "read_due_direct_messages.sql", "main-direct.sql"],
  ["fleet-mail", "read_due_system_expiries.sql", "main-system.sql"],
] as const;

async function dueLists(
  object: string,
  native: DatabaseSync,
  views: boolean,
): Promise<unknown[][]> {
  const lists: unknown[][] = [];
  for (const [owner, file, main] of shortlists) {
    const path = views
      ? "crates/" + owner + "/src/queries/" + file
      : "crates/collaboration-central-core/tests/fixtures/" + main;
    const sql = readFileSync(new URL(path, source), "utf8")
      .replace("?1", "450000")
      .replace("?2", "251");
    const rows = (await run(object, sql)).rows;
    assert.deepEqual(rows, resultSchema.shape.rows.parse(native.prepare(sql).all()));
    lists.push(
      rows.map((row): readonly unknown[] =>
        views
          ? [row["subject_key?: String"], row["occurrence_ms!: i64"], row["effective_due_ms!: i64"]]
          : [row["subject_key"], row["occurrence_ms"], row["effective_due_ms"]],
      ),
    );
  }
  return lists;
}
test("a populated masked job and fixed active history agree on both backends", async (context): Promise<void> => {
  const mask =
    "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000002200','legacy_notice_windows','all','failing',1,'1970-01-01T00:12:00.000Z','statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";
  const timerMask =
    "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000002201','engine_source','wake_timer','failing',1,'1970-01-01T00:03:00.000Z','statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";
  const cases = [
    { name: "sparse-short-history", seed: seedHistory(100, 50, 50) },
    { name: "sparse-long-history", seed: seedHistory(10_000, 50, 50) },
    { name: "all-finished", seed: seedHistory(10_000, 0, 0) },
    { name: "open-windows", seed: seedHistory(10_000, 50, 10_000) },
    {
      name: "held-jobs",
      seed: [
        ...seedHistory(10_000, 50, 10_000),
        "UPDATE mail_workspace_drains SET drain_due_at = '1970-01-01T00:14:00.000Z' WHERE drain_due_at IS NOT NULL",
        mask,
        timerMask,
      ],
    },
  ];
  const historyReads: number[] = [];
  const selections: unknown[][] = [];
  for (const { name, seed } of cases) {
    await prepare(name, [...oldSchema, ...seed]);
    const old = await run(name, oldAlarm);
    const database = join(scratch, name + ".sqlite");
    const native = new DatabaseSync(database);
    try {
      for (const sql of [...oldSchema, ...seed]) native.exec(sql);
      const previous = native.prepare(oldAlarm);
      const expected = pair(resultSchema.shape.rows.parse(previous.all()), false);
      assert.deepEqual(pair(old.rows, false), expected);
      const expectedAlarm = {
        "all-finished": [null, null],
        "held-jobs": [180_000, 720_000],
      }[name] ?? [300_000, 300_000];
      assert.deepEqual(expected, expectedAlarm);
      const mainLists = name.startsWith("sparse-") ? await dueLists(name, native, false) : null;
      const mainMs = timeStock(previous);
      const mainStock = stockCounters(database, oldAlarm);
      const indexBuilds: { sql: string; reads: number; writes: number }[] = [];
      if (name === "sparse-long-history") {
        for (const [indexSql] of dueSchema.matchAll(/CREATE INDEX[^;]+;/g)) {
          const built = await run(name, indexSql);
          indexBuilds.push({ sql: indexSql, reads: built.rowsRead, writes: built.rowsWritten });
        }
      }
      await run(
        name,
        name === "sparse-long-history" ? dueSchema.replace(/CREATE INDEX[^;]+;/g, "") : dueSchema,
        true,
      );
      const migrationStarted = performance.now();
      native.exec(dueSchema);
      const stockMigrationMs = performance.now() - migrationStarted;
      const actual = await run(name, nextAlarm);
      const notices =
        name === "open-windows" || name === "held-jobs"
          ? await run(name, "SELECT due_at_ms FROM notice_window_due_rows")
          : null;
      if (notices !== null) {
        assert.deepEqual(notices.rows, [{ due_at_ms: name === "held-jobs" ? 720_000 : 300_000 }]);
        assert.equal(notices.rowsWritten, 0);
        if (name === "held-jobs") assert.ok(notices.rowsRead <= 2, JSON.stringify(notices));
      }
      const current = native.prepare(nextAlarm);
      assert.deepEqual(pair(actual.rows, true), expected);
      assert.deepEqual(pair(resultSchema.shape.rows.parse(current.all()), true), expected);
      context.diagnostic(
        JSON.stringify({
          name,
          alarm: expected,
          mainRows: old.rowsRead,
          viewRows: actual.rowsRead,
          stockMsFor1000: [mainMs, timeStock(current)],
          stockCounters: [mainStock, stockCounters(database, nextAlarm)],
          noticeRows: notices?.rowsRead,
          indexBuilds,
          stockMigrationMs,
        }),
      );
      if (mainLists !== null) {
        historyReads.push(actual.rowsRead);
        const actualLists = await dueLists(name, native, true);
        assert.deepEqual(actualLists, mainLists);
        selections.push(actualLists);
      }
    } finally {
      native.close();
    }
  }
  assert.deepEqual(historyReads, [318, 318]);
  assert.deepEqual(selections[0], selections[1]);
});

test("the common checked SELECT runs with a fresh Workerd fixture overlay", async (): Promise<void> => {
  await prepare("overlay", [
    ...oldSchema,
    dueSchema,
    overlay,
    "INSERT INTO signin_organization_identity VALUES ('fixture-stamp','fixture',1,1)",
    "INSERT INTO engine_alarm_only_items VALUES ('01890f2e-7b4c-7cc0-98c4-000000000031','1970-01-01T00:15:00.000Z','waiting','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
  ]);
  assert.deepEqual(pair((await run("overlay", nextAlarm)).rows, true), [900_000, null]);
});
