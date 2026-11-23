import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { test } from "node:test";
import { seedHistory } from "./engine-due-views-fixture.ts";
import {
  alarmTimePair,
  dueViewQueryPlan,
  dueViewQueryResultSchema,
  prepareDueViewStore,
  replaceRequiredSql,
  runDueViewQuery,
  dueSchema,
  migrationDirectory,
  nextAlarm,
  oldAlarm,
  oldSchema,
  overlay,
  repositoryDirectory,
  signinAlarmSchema,
  scratchDirectory,
  stockCounters,
  type DueViewQueryResult,
} from "./workerd-due-view-fixture.ts";
import {
  checkMachineAllowanceBudget,
  checkMachineDirectoryBudget,
} from "./machine-workspace-page-fixture.ts";

test("the shipped Workerd alarm fits 330 reads and loses the bound without the expiry index", async (context): Promise<void> => {
  const fixtures = seedHistory(10_000, 50, 50);
  await prepareDueViewStore("indexed", [...oldSchema, dueSchema, signinAlarmSchema, ...fixtures]);
  const actual = await runDueViewQuery("indexed", nextAlarm);
  assert.deepEqual(alarmTimePair(actual.rows, true), [300_000, 300_000]);
  assert.equal(actual.rowsWritten, 0);
  assert.ok(actual.rowsRead <= 330, JSON.stringify(actual));

  const paths = await dueViewQueryPlan("indexed", nextAlarm);
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

  const noExpiry = replaceRequiredSql(
    dueSchema,
    /CREATE INDEX messages_system_expiry_due [^;]+;/,
    "",
  );
  await prepareDueViewStore("lost-expiry", [
    ...oldSchema,
    noExpiry,
    signinAlarmSchema,
    ...fixtures,
  ]);
  const lostExpiry = await runDueViewQuery("lost-expiry", nextAlarm);
  assert.deepEqual(lostExpiry.rows, actual.rows);
  assert.ok(lostExpiry.rowsRead > 330, JSON.stringify(lostExpiry));
  assert.doesNotMatch(
    await dueViewQueryPlan("lost-expiry", nextAlarm),
    /messages_system_expiry_due/,
  );

  const noWake = replaceRequiredSql(dueSchema, "INDEXED BY wakes_due", "NOT INDEXED");
  await prepareDueViewStore("lost-wake", [...oldSchema, noWake, signinAlarmSchema, ...fixtures]);
  const lostWake = await runDueViewQuery("lost-wake", nextAlarm);
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
  const badSchema = replaceRequiredSql(
    dueSchema,
    /SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls/g,
    "SELECT CASE WHEN uses_subject_attempt THEN 1 END AS control_present, due_at_ms FROM engine_due_attempt_controls",
  );
  const fixtures = [
    "INSERT INTO signin_organization_identity VALUES ('fixture-stamp','fixture',1,1)",
    "INSERT INTO automation_wakes VALUES ('01890f2e-7b4c-7cc0-98c4-000000000022','target','1970-01-01T00:05:00.000Z','wake','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
    "INSERT INTO engine_work_attempts VALUES (NULL,'engine_source','wake_timer','retired',1,NULL,'statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
  ];
  await prepareDueViewStore("masked", [...oldSchema, dueSchema, signinAlarmSchema, ...fixtures]);
  await prepareDueViewStore("mask-regression", [
    ...oldSchema,
    badSchema,
    signinAlarmSchema,
    ...fixtures,
  ]);
  assert.deepEqual(alarmTimePair((await runDueViewQuery("masked", oldAlarm)).rows, false), [
    null,
    null,
  ]);
  assert.deepEqual(alarmTimePair((await runDueViewQuery("masked", nextAlarm)).rows, true), [
    null,
    null,
  ]);
  assert.deepEqual(
    alarmTimePair((await runDueViewQuery("mask-regression", nextAlarm)).rows, true),
    [300_000, null],
  );
});

function timeStock(statement: StatementSync): number {
  for (let warm = 0; warm < 15; warm += 1) statement.all();
  const started = performance.now();
  for (let iteration = 0; iteration < 1_000; iteration += 1) statement.all();
  return performance.now() - started;
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
    const sql = readFileSync(new URL(path, repositoryDirectory), "utf8")
      .replace("?1", "450000")
      .replace("?2", "251");
    const rows = (await runDueViewQuery(object, sql)).rows;
    assert.deepEqual(rows, dueViewQueryResultSchema.shape.rows.parse(native.prepare(sql).all()));
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
function populatedDueHistory(): { readonly name: string; readonly seed: readonly string[] }[] {
  const mask =
    "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000002200','legacy_notice_windows','all','failing',1,'1970-01-01T00:12:00.000Z','statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";
  const timerMask =
    "INSERT INTO engine_work_attempts VALUES ('01890f2e-7b4c-7cc0-98c4-000000002201','engine_source','wake_timer','failing',1,'1970-01-01T00:03:00.000Z','statement_rejected',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z',NULL,'1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";
  return [
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
}

test("a populated masked job and fixed active history agree on both backends", async (context): Promise<void> => {
  const historyReads: number[] = [];
  const selections: unknown[][] = [];
  for (const { name, seed } of populatedDueHistory()) {
    await prepareDueViewStore(name, [...oldSchema, ...seed]);
    const old = await runDueViewQuery(name, oldAlarm);
    const database = join(scratchDirectory, name + ".sqlite");
    const native = new DatabaseSync(database);
    try {
      for (const sql of [...oldSchema, ...seed]) native.exec(sql);
      const previous = native.prepare(oldAlarm);
      const expected = alarmTimePair(
        dueViewQueryResultSchema.shape.rows.parse(previous.all()),
        false,
      );
      assert.deepEqual(alarmTimePair(old.rows, false), expected);
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
          const built = await runDueViewQuery(name, indexSql);
          indexBuilds.push({ sql: indexSql, reads: built.rowsRead, writes: built.rowsWritten });
        }
      }
      await runDueViewQuery(
        name,
        name === "sparse-long-history" ? dueSchema.replace(/CREATE INDEX[^;]+;/g, "") : dueSchema,
        { setup: true },
      );
      const migrationStarted = performance.now();
      await runDueViewQuery(name, signinAlarmSchema, { setup: true });
      native.exec(dueSchema);
      native.exec(signinAlarmSchema);
      const stockMigrationMs = performance.now() - migrationStarted;
      const actual = await runDueViewQuery(name, nextAlarm);
      const notices =
        name === "open-windows" || name === "held-jobs"
          ? await runDueViewQuery(name, "SELECT due_at_ms FROM notice_window_due_rows")
          : null;
      if (notices !== null) {
        assert.deepEqual(notices.rows, [{ due_at_ms: name === "held-jobs" ? 720_000 : 300_000 }]);
        assert.equal(notices.rowsWritten, 0);
        if (name === "held-jobs") assert.ok(notices.rowsRead <= 2, JSON.stringify(notices));
      }
      const current = native.prepare(nextAlarm);
      assert.deepEqual(alarmTimePair(actual.rows, true), expected);
      assert.deepEqual(
        alarmTimePair(dueViewQueryResultSchema.shape.rows.parse(current.all()), true),
        expected,
      );
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
  await prepareDueViewStore("overlay", [
    ...oldSchema,
    dueSchema,
    signinAlarmSchema,
    overlay,
    "INSERT INTO engine_alarm_only_items VALUES ('01890f2e-7b4c-7cc0-98c4-000000000031','1970-01-01T00:15:00.000Z','waiting','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')",
  ]);
  assert.deepEqual(alarmTimePair((await runDueViewQuery("overlay", nextAlarm)).rows, true), [
    null,
    null,
  ]);
  await prepareDueViewStore("overlay", [
    "INSERT INTO signin_organization_identity VALUES ('fixture-stamp','fixture',1,1)",
  ]);
  assert.deepEqual(alarmTimePair((await runDueViewQuery("overlay", nextAlarm)).rows, true), [
    900_000,
    null,
  ]);
  await prepareDueViewStore("overlay", [
    "INSERT INTO signin_organization_identity VALUES ('other-stamp','fixture',1,1)",
  ]);
  assert.deepEqual(alarmTimePair((await runDueViewQuery("overlay", nextAlarm)).rows, true), [
    null,
    null,
  ]);
});

const sparseDirectReceiptHistory = [
  "UPDATE mail_messages SET delivery_window_ends_at=cancel_due_at WHERE message_kind='direct'",
  "UPDATE mail_messages SET state='seen',seen_completed_at='1970-01-01T00:01:00.000Z' WHERE message_kind='direct' AND state='delivered'",
  "UPDATE mail_messages SET state='queued',deadline_settled_at='1970-01-01T00:20:00.000Z' WHERE message_kind='direct' AND state='cancelled'",
  "UPDATE mail_messages SET state='delivered',delivered_arrived_at='1970-01-01T00:00:01.000Z' WHERE message_kind='direct' AND state='queued' AND deadline_settled_at IS NULL",
];

test("the final Central schema keeps delivered direct marks within 330 sparse reads", async (context) => {
  const finalSchema = readFileSync(
    new URL("0007_direct_message_receipts.sql", migrationDirectory),
    "utf8",
  );
  const fixtures = seedHistory(10_000, 50, 50);
  for (const [name, ddl, limit] of [
    ["mail2-indexed", finalSchema, true],
    ["mail2-no-direct-index", finalSchema + "\nDROP INDEX messages_cancel_due;", false],
  ] as const) {
    await prepareDueViewStore(name, [
      ...oldSchema,
      dueSchema,
      signinAlarmSchema,
      ddl,
      ...fixtures,
      ...sparseDirectReceiptHistory,
    ]);
    const alarm = await runDueViewQuery(name, nextAlarm);

    assert.deepEqual(alarmTimePair(alarm.rows, true), [300_000, 300_000]);
    assert.equal(alarm.rowsWritten, 0);
    assert.equal(alarm.rowsRead <= 330, limit, JSON.stringify(alarm));
    context.diagnostic(JSON.stringify({ name, rowsRead: alarm.rowsRead }));
  }
});

test("the Workerd alarm stays within 330 reads with machine allowance rows", async (context) => {
  const receipts = readFileSync(
    new URL("0007_direct_message_receipts.sql", migrationDirectory),
    "utf8",
  );
  const allowances = readFileSync(
    new URL("0008_machine_allowance_audit.sql", migrationDirectory),
    "utf8",
  );
  const object = "machine-allowances-alarm-budget";
  await prepareDueViewStore(object, [
    ...oldSchema,
    dueSchema,
    signinAlarmSchema,
    receipts,
    allowances,
    ...seedHistory(10_000, 50, 50, "machine-allowances"),
    ...sparseDirectReceiptHistory,
    "INSERT INTO login_machine_allowances SELECT workspace_id, (SELECT machine_id FROM login_machines LIMIT 1), workspace_id, 'active', 'fixture', 'automatic', created_at, updated_at FROM directory_workspaces ORDER BY workspace_id LIMIT 1000",
  ]);
  const stored = await runDueViewQuery(
    object,
    "SELECT count(*) AS allowances FROM login_machine_allowances",
  );
  assert.deepEqual(stored.rows, [{ allowances: 1000 }]);
  const alarm = await runDueViewQuery(object, nextAlarm);
  assert.deepEqual(alarmTimePair(alarm.rows, true), [300_000, 300_000]);
  assert.equal(alarm.rowsWritten, 0);
  assert.ok(alarm.rowsRead <= 330, JSON.stringify(alarm));
  context.diagnostic(JSON.stringify({ allowanceRows: 1000, alarmRowsRead: alarm.rowsRead }));
});

test("machine workspace pages read only permitted rows apart from numeric cursor rank", async (context): Promise<void> => {
  const schema = readdirSync(migrationDirectory)
    .sort()
    .map((name): string => readFileSync(new URL(name, migrationDirectory), "utf8"));
  await checkMachineDirectoryBudget({
    schema,
    scratch: scratchDirectory,
    prepare: prepareDueViewStore,
    run: (object, sql, bindings): Promise<DueViewQueryResult> =>
      runDueViewQuery(object, sql, { bindings }),
    stockCounters,
    diagnostic: (receipt): void => context.diagnostic(receipt),
  });
});

test("machine allowance pair, active list and guard ignore withdrawn history", async (context): Promise<void> => {
  const schema = readdirSync(migrationDirectory)
    .sort()
    .map((name): string => readFileSync(new URL(name, migrationDirectory), "utf8"));
  await checkMachineAllowanceBudget({
    schema,
    scratch: scratchDirectory,
    prepare: prepareDueViewStore,
    run: (object, sql, bindings): Promise<DueViewQueryResult> =>
      runDueViewQuery(object, sql, { bindings }),
    stockCounters,
    diagnostic: (receipt): void => context.diagnostic(receipt),
  });
});

test("system offer and expiry inputs preserve every stored shape from main", async (context) => {
  const object = "mail2-system-shapes";
  // Frozen 254caabce delivery candidate predicate. The probe selects only
  // IDs so it observes eligibility independently of the typed decoder.
  const oldQuery =
    "SELECT message_id FROM mail_messages WHERE target_machine_id=?1 AND state='queued' AND message_kind IN ('direct','notice','deliveryOutcome') AND delivery_window_ends_at>?2 AND (?3 IS NULL OR (created_at,message_id)>(?3,?4)) ORDER BY created_at,message_id LIMIT ?5";
  const version = readFileSync(
    new URL("0007_direct_message_receipts.sql", migrationDirectory),
    "utf8",
  );
  const page = readFileSync(
    new URL("crates/fleet-mail/src/queries/read_windowed_page.sql", repositoryDirectory),
    "utf8",
  );
  const previousExpiry = readFileSync(
    new URL(
      "crates/collaboration-central-core/tests/fixtures/main-system.sql",
      repositoryDirectory,
    ),
    "utf8",
  );
  const currentExpiry = readFileSync(
    new URL("crates/fleet-mail/src/queries/read_due_system_expiries.sql", repositoryDirectory),
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
  await prepareDueViewStore(object, [
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
    const identities = (items: DueViewQueryResult["rows"], label: string): unknown[] =>
      items.map((row) => row[label]);
    const original = substitute(oldQuery, offerBinds);
    // On offer a NULL message id fails the checked decoder. The predicate
    // comparison retains it to verify selection before decoding.
    const current = substitute(
      "SELECT message_id FROM (" + page.replace('AS "message_id!: String"', "") + ")",
      offerBinds,
    );
    assert.deepEqual(
      identities((await runDueViewQuery(object, current)).rows, "message_id"),
      identities((await runDueViewQuery(object, original)).rows, "message_id"),
    );
    assert.deepEqual(native.prepare(current).all(), native.prepare(original).all());
    for (const horizon of ["0", "359999", "360000", "480000", "600000"]) {
      const oldSql = substitute(previousExpiry, [horizon, "250"]);
      const newSql = substitute(currentExpiry, [horizon, "250"]);
      const values = (result: readonly Record<string, unknown>[]): unknown[][] =>
        result.map(Object.values);
      assert.deepEqual(
        values((await runDueViewQuery(object, newSql)).rows),
        values((await runDueViewQuery(object, oldSql)).rows),
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
  const directory = new URL("crates/fleet-request-log/src/queries/", repositoryDirectory);
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
    await prepareDueViewStore(object, oldSchema);
    const worker = await executeComparison(
      statements,
      async (sql, binds): Promise<DueViewQueryResult["rows"]> =>
        (await runDueViewQuery(object, quotedLedgerBindings(sql, binds))).rows,
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
