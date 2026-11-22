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
const signinAlarmSchema = readFileSync(
  new URL("0006_signin_provisioned_for_engine.sql", migrations),
  "utf8",
);
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
  await prepare("indexed", [...oldSchema, dueSchema, signinAlarmSchema, ...fixtures]);
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
  await prepare("lost-expiry", [...oldSchema, noExpiry, signinAlarmSchema, ...fixtures]);
  const lostExpiry = await run("lost-expiry", nextAlarm);
  assert.deepEqual(lostExpiry.rows, actual.rows);
  assert.ok(lostExpiry.rowsRead > 330, JSON.stringify(lostExpiry));
  assert.doesNotMatch(await plan("lost-expiry"), /messages_system_expiry_due/);

  const noWake = replaceRequired(dueSchema, "INDEXED BY wakes_due", "NOT INDEXED");
  await prepare("lost-wake", [...oldSchema, noWake, signinAlarmSchema, ...fixtures]);
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
  await prepare("masked", [...oldSchema, dueSchema, signinAlarmSchema, ...fixtures]);
  await prepare("mask-regression", [...oldSchema, badSchema, signinAlarmSchema, ...fixtures]);
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
      await run(name, signinAlarmSchema, true);
      native.exec(dueSchema);
      native.exec(signinAlarmSchema);
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

test("the checked SELECT in a Workerd fixture overlay requires exactly one stamp", async (): Promise<void> => {
  await prepare("overlay", [
    ...oldSchema,
    dueSchema,
    signinAlarmSchema,
    overlay,
    "INSERT INTO engine_alarm_only_items VALUES ('01890f2e-7b4c-7cc0-98c4-000000000031','1970-01-01T00:15:00.000Z','waiting','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
  ]);
  assert.deepEqual(pair((await run("overlay", nextAlarm)).rows, true), [null, null]);
  await prepare("overlay", [
    "INSERT INTO signin_organization_identity VALUES ('fixture-stamp','fixture',1,1)",
  ]);
  assert.deepEqual(pair((await run("overlay", nextAlarm)).rows, true), [900_000, null]);
  await prepare("overlay", [
    "INSERT INTO signin_organization_identity VALUES ('other-stamp','fixture',1,1)",
  ]);
  assert.deepEqual(pair((await run("overlay", nextAlarm)).rows, true), [null, null]);
});

const sparseDirectReceiptHistory = [
  // Finished history uses the new terminal/marked tags. Delivered is unfinished.
  "UPDATE mail_messages SET state='seen',seen_completed_at='1970-01-01T00:01:00.000Z' WHERE message_kind='direct' AND state='delivered'",
  "UPDATE mail_messages SET state='pastTheMark' WHERE message_kind='direct' AND state='cancelled'",
  "UPDATE mail_messages SET state='delivered',delivered_arrived_at='1970-01-01T00:00:01.000Z' WHERE message_kind='direct' AND state='queued'",
];

test("the final Central schema keeps delivered direct marks within 330 sparse reads", async (context) => {
  const finalSchema = readFileSync(new URL("0007_direct_message_receipts.sql", migrations), "utf8");
  const fixtures = seedHistory(10_000, 50, 50);
  for (const [name, ddl, limit] of [
    ["mail2-indexed", finalSchema, true],
    [
      "mail2-no-direct-index",
      replaceRequired(finalSchema, "INDEXED BY messages_cancel_due", "NOT INDEXED"),
      false,
    ],
  ] as const) {
    await prepare(name, [
      ...oldSchema,
      dueSchema,
      signinAlarmSchema,
      ddl,
      ...fixtures,
      ...sparseDirectReceiptHistory,
    ]);
    const alarm = await run(name, nextAlarm);

    assert.deepEqual(pair(alarm.rows, true), [300_000, 300_000]);
    assert.equal(alarm.rowsWritten, 0);
    assert.equal(alarm.rowsRead <= 330, limit, JSON.stringify(alarm));
    context.diagnostic(JSON.stringify({ name, rowsRead: alarm.rowsRead }));
  }
});

test("the Workerd alarm stays within 330 reads with machine allowance rows", async (context) => {
  const receipts = readFileSync(new URL("0007_direct_message_receipts.sql", migrations), "utf8");
  const allowances = readFileSync(new URL("0008_machine_allowance_audit.sql", migrations), "utf8");
  const object = "machine-allowances-alarm-budget";
  await prepare(object, [
    ...oldSchema,
    dueSchema,
    signinAlarmSchema,
    receipts,
    allowances,
    ...seedHistory(10_000, 50, 50, "machine-allowances"),
    ...sparseDirectReceiptHistory,
    "INSERT INTO login_machine_allowances SELECT workspace_id, (SELECT machine_id FROM login_machines LIMIT 1), workspace_id, 'active', 'fixture', 'automatic', created_at, updated_at FROM directory_workspaces ORDER BY workspace_id LIMIT 1000",
  ]);
  const stored = await run(object, "SELECT count(*) AS allowances FROM login_machine_allowances");
  assert.deepEqual(stored.rows, [{ allowances: 1000 }]);
  const alarm = await run(object, nextAlarm);
  assert.deepEqual(pair(alarm.rows, true), [300_000, 300_000]);
  assert.equal(alarm.rowsWritten, 0);
  assert.ok(alarm.rowsRead <= 330, JSON.stringify(alarm));
  context.diagnostic(JSON.stringify({ allowanceRows: 1000, alarmRowsRead: alarm.rowsRead }));
});

test("system offer and expiry inputs preserve every stored shape from main", async (context) => {
  const object = "mail2-system-shapes";
  // Frozen 254caabce delivery candidate predicate. The probe selects only
  // IDs so it observes eligibility independently of the typed decoder.
  const oldQuery =
    "SELECT message_id FROM mail_messages WHERE target_machine_id=?1 AND state='queued' AND message_kind IN ('direct','notice','deliveryOutcome') AND delivery_window_ends_at>?2 AND (?3 IS NULL OR (created_at,message_id)>(?3,?4)) ORDER BY created_at,message_id LIMIT ?5";
  const version = readFileSync(new URL("0007_direct_message_receipts.sql", migrations), "utf8");
  const page = readFileSync(
    new URL("crates/fleet-mail/src/queries/read_windowed_page.sql", source),
    "utf8",
  );
  const previousExpiry = readFileSync(
    new URL("crates/collaboration-central-core/tests/fixtures/main-system.sql", source),
    "utf8",
  );
  const currentExpiry = readFileSync(
    new URL("crates/fleet-mail/src/queries/read_due_system_expiries.sql", source),
    "utf8",
  );
  const machine = "01890f2e-7b4c-7cc0-98c4-000000009002";
  const now = "1970-01-01T00:06:00.000Z";
  const created = "1970-01-01T00:00:00.000Z";
  const seed = seedHistory(1, 0, 0);
  const sqlText = (value: string | null): string => (value === null ? "NULL" : "'" + value + "'");
  const rows: string[] = [];
  // All predicate equivalence classes, with both present and NULL SQLite PKs.
  for (const kind of ["notice", "deliveryOutcome", "wake", "automation", "unrecognized"]) {
    for (const state of ["queued", "delivered", "cancelled", "failed", "unrecognized"]) {
      for (const expiry of [null, created, now, "1970-01-01T00:08:00.000Z", "unreadable"]) {
        for (const key of [null, "fixture-" + rows.length]) {
          rows.push(
            "INSERT INTO mail_messages (message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,delivery_window_ends_at) VALUES (" +
              [key, "central", machine, "fixture", kind, "body", state, created, created, expiry]
                .map(sqlText)
                .join(",") +
              ");",
          );
        }
      }
    }
  }
  await prepare(object, [
    ...oldSchema,
    dueSchema,
    version,
    ...seed,
    "DELETE FROM mail_messages",
    rows.join("\n"),
  ]);
  const native = new DatabaseSync(":memory:");
  try {
    for (const statement of [
      ...oldSchema,
      dueSchema,
      version,
      ...seed,
      "DELETE FROM mail_messages",
      ...rows,
    ])
      native.exec(statement);
    const substitute = (sql: string, binds: readonly string[]): string =>
      sql.replace(/\?(\d+)/g, (_, index: string) => binds[Number(index) - 1] ?? "NULL");
    const offerBinds = [sqlText(machine), sqlText(now), "NULL", "NULL", "250"];
    const identities = (items: Result["rows"], label: string): unknown[] =>
      items.map((row) => row[label]);
    const original = substitute(oldQuery, offerBinds);
    // On offer a NULL message id fails the checked decoder. The predicate
    // comparison retains it to verify selection before decoding.
    const current = substitute(
      "SELECT message_id FROM (" + page.replace('AS "message_id!: String"', "") + ")",
      offerBinds,
    );
    assert.deepEqual(
      identities((await run(object, current)).rows, "message_id"),
      identities((await run(object, original)).rows, "message_id"),
    );
    assert.deepEqual(native.prepare(current).all(), native.prepare(original).all());
    for (const horizon of ["0", "359999", "360000", "480000", "600000"]) {
      const oldSql = substitute(previousExpiry, [horizon, "250"]);
      const newSql = substitute(currentExpiry, [horizon, "250"]);
      const values = (result: readonly Record<string, unknown>[]): unknown[][] =>
        result.map(Object.values);
      assert.deepEqual(
        values((await run(object, newSql)).rows),
        values((await run(object, oldSql)).rows),
      );
      assert.deepEqual(values(native.prepare(newSql).all()), values(native.prepare(oldSql).all()));
    }
    context.diagnostic(
      JSON.stringify({ systemShapes: rows.length, backends: 2, expiryHorizons: 5 }),
    );
  } finally {
    native.close();
  }
});

function quotedLedgerBindings(sql: string, binds: readonly string[]): string {
  let position = 0;
  return sql.replaceAll("?", (): string => {
    const value = binds[position++];
    assert.ok(value !== undefined, "every ledger placeholder has a bound value");
    return "'" + value.replaceAll("'", "''") + "'";
  });
}

test("organization request replay and pruning keep main's strict cutoff on both stores", async (context): Promise<void> => {
  // The three frozen statements are from fleet-runtime at main 6cf6c254c.
  const original = {
    read: "SELECT caller,content_digest,answer FROM runtime_request_log WHERE request_id=? AND created_at>?",
    retain:
      "INSERT INTO runtime_request_log (request_id,caller,content_digest,answer,created_at,updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(request_id) DO UPDATE SET caller=excluded.caller,content_digest=excluded.content_digest,answer=excluded.answer,created_at=excluded.created_at,updated_at=excluded.updated_at",
    prune: "DELETE FROM runtime_request_log WHERE created_at<=?",
  };
  const directory = new URL("crates/fleet-request-log/src/queries/", source);
  const current = {
    read: readFileSync(new URL("read_central_request.sql", directory), "utf8"),
    retain: readFileSync(new URL("retain_central_request.sql", directory), "utf8"),
    prune: readFileSync(new URL("prune_central_requests.sql", directory), "utf8"),
  };
  const cutoff = "2030-01-02T00:00:00+00:00";
  const storedTimes = [
    "2029-12-31T23:59:59+00:00",
    "2030-01-01T23:59:59.999999999+00:00",
    cutoff,
    "2030-01-02T00:00:00.000000001+00:00",
    "2030-01-02T00:00:01+00:00",
    "2030-01-02T00:00:00Z",
    "2030-01-02T00:00:00.000Z",
    "bad time",
    "",
  ];
  const executeComparison = async (
    statements: typeof current,
    execute: (sql: string, binds: readonly string[]) => Promise<readonly unknown[]>,
  ): Promise<readonly unknown[]> => {
    for (const [index, createdAt] of storedTimes.entries()) {
      await execute(statements.retain, [
        String(index),
        "caller",
        "digest",
        "{}",
        createdAt,
        "2030-01-03T00:00:00+00:00",
      ]);
    }
    const retained = [];
    for (const [index] of storedTimes.entries()) {
      retained.push(await execute(statements.read, [String(index), cutoff]));
    }
    await execute(statements.prune, [cutoff]);
    const firstSurvivors = await execute(
      "SELECT * FROM runtime_request_log ORDER BY request_id",
      [],
    );
    await execute(statements.retain, ["2", "new caller", "digest", "{}", cutoff, cutoff]);
    await execute(statements.retain, [
      "3",
      "new caller",
      "new digest",
      '{"done":true}',
      cutoff,
      cutoff,
    ]);
    const replayAtEquality = await execute(statements.read, ["3", cutoff]);
    const updatedBeforePrune = await execute(
      "SELECT * FROM runtime_request_log ORDER BY request_id",
      [],
    );
    await execute(statements.prune, [cutoff]);
    const finalSurvivors = await execute(
      "SELECT * FROM runtime_request_log ORDER BY request_id",
      [],
    );
    return [retained, firstSurvivors, replayAtEquality, updatedBeforePrune, finalSurvivors];
  };

  const outcomes = [];
  for (const [name, statements] of [
    ["main", original],
    ["checked", current],
  ] as const) {
    const object = `request-cutoff-${name}`;
    await prepare(object, oldSchema);
    const worker = await executeComparison(
      statements,
      async (sql, binds): Promise<Result["rows"]> =>
        (await run(object, quotedLedgerBindings(sql, binds))).rows,
    );
    const native = new DatabaseSync(":memory:");
    try {
      for (const sql of oldSchema) native.exec(sql);
      const sqlite = await executeComparison(statements, async (sql, binds): Promise<unknown[]> =>
        native
          .prepare(sql)
          .all(...binds)
          .map((row) => ({ ...row })),
      );
      assert.deepEqual(worker, sqlite);
      outcomes.push(worker);
    } finally {
      native.close();
    }
  }
  assert.deepEqual(outcomes[1], outcomes[0]);
  context.diagnostic(JSON.stringify({ storedTimes: storedTimes.length, backends: 2 }));
});
