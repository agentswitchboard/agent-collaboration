import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

type Binding = string | number | null;
type Row = Readonly<Record<string, Binding>>;

interface DirectoryBudgetProbe {
  readonly schema: readonly string[];
  readonly scratch: string;
  prepare(object: string, statements: readonly string[]): Promise<void>;
  run(
    object: string,
    sql: string,
    bindings: readonly Binding[],
  ): Promise<{
    readonly rows: readonly Row[];
    readonly rowsRead: number;
    readonly rowsWritten: number;
  }>;
  stockCounters(
    database: string,
    sql: string,
    bindings: readonly Binding[],
  ): Record<string, number> | null;
  diagnostic(receipt: string): void;
}

const source = new URL("../../", import.meta.url);
function query(crate: string, file: string): string {
  return readFileSync(new URL("crates/" + crate + "/src/queries/" + file + ".sql", source), "utf8");
}

const pageSql = query("fleet-workspace-directory", "read_machine_workspace_page");
const boundarySql = query("fleet-workspace-directory", "read_workspace_id_at_ordinal");
const ordinalSql = query("fleet-workspace-directory", "read_workspace_ordinal");
const accessSql = query("fleet-workspace-access", "read_workspace_state");
const fullDirectorySql =
  "SELECT workspace_id,name,description,state,created_at,updated_at FROM directory_workspaces ORDER BY workspace_id";

function assertCoveringRankPlan(
  database: DatabaseSync,
  sql: string,
  bindings: readonly Binding[],
): void {
  const details = database
    .prepare(sql)
    .all(...bindings)
    .map((row): string => String(row["detail"]))
    .join("\n");
  assert.match(details, /COVERING INDEX sqlite_autoindex_directory_workspaces_1/);
}

function workspaceId(suffix: number): string {
  return "019f0000-0000-7000-8000-" + suffix.toString(16).padStart(12, "0");
}

const research = workspaceId(20_001);
const notes = workspaceId(20_002);
const archived = workspaceId(20_003);
const lowercaseAccent = workspaceId(20_004);
const unpermitted = workspaceId(20_005);
const permitted = JSON.stringify([lowercaseAccent, notes, archived, research]);
const stamp = "2030-03-17T18:46:40Z";

function seed(unrelated: number): string {
  return `
WITH RECURSIVE history(suffix) AS (
  SELECT 1 UNION ALL SELECT suffix + 1 FROM history WHERE suffix < ${unrelated}
)
INSERT INTO directory_workspaces (workspace_id, name, description, state, created_at, updated_at)
SELECT printf('019f0000-0000-7000-8000-%012x', suffix), 'Unrelated ' || suffix, '',
       CASE WHEN suffix % 2 = 0 THEN 'archived' ELSE 'active' END, '${stamp}', '${stamp}'
FROM history;
INSERT INTO directory_workspaces (workspace_id, name, description, state, created_at, updated_at)
VALUES
  ('${research}', 'É%_ Research', '', 'active', '${stamp}', '${stamp}'),
  ('${notes}', 'Notes', 'prefix' || char(0) || '%_ ÉNODE', 'active', '${stamp}', '${stamp}'),
  ('${archived}', 'Archived É%_', '', 'archived', '${stamp}', '${stamp}'),
  ('${lowercaseAccent}', 'Lowercase é%_', '', 'active', '${stamp}', '${stamp}'),
  ('${unpermitted}', 'Unpermitted É%_', '', 'active', '${stamp}', '${stamp}');
`;
}

export async function checkMachineDirectoryBudget(probe: DirectoryBudgetProbe): Promise<void> {
  const measurements = [];
  for (const unrelated of [10, 10_000]) {
    const object = "machine-directory-budget-" + String(unrelated);
    const steps = [...probe.schema, seed(unrelated)];
    await probe.prepare(object, steps);
    const database = join(probe.scratch, object + ".sqlite");
    const native = new DatabaseSync(database);
    try {
      for (const statement of steps) native.exec(statement);
      async function compare(sql: string, bindings: readonly Binding[]): Promise<number> {
        const worker = await probe.run(object, sql, bindings);
        const stock = native
          .prepare(sql)
          .all(...bindings)
          .map((row): object => ({ ...row }));
        assert.deepEqual(worker.rows, stock);
        return worker.rowsRead;
      }
      async function page(
        bindings: readonly Binding[],
        expected: readonly string[],
      ): Promise<number> {
        const worker = await probe.run(object, pageSql, bindings);
        const stock = native.prepare(pageSql).all(...bindings);
        assert.deepEqual(
          worker.rows.map((row): unknown => row["workspace_id"]),
          expected,
        );
        assert.deepEqual(
          stock.map((row): unknown => row["workspace_id"]),
          expected,
        );
        return worker.rowsRead;
      }

      const pageBinds = [permitted, 0, null, null, 1, 2];
      const directoryReads = await page(pageBinds, [research, notes]);
      const stateReads = await compare(accessSql, [notes]);
      const boundaryReads = await compare(boundarySql, [unrelated]);
      const ordinalReads = await compare(ordinalSql, [research]);
      for (const [rankSql, bindings] of [
        [boundarySql, [unrelated]],
        [ordinalSql, [research]],
      ] as const) {
        const sql = "EXPLAIN QUERY PLAN " + rankSql;
        await compare(sql, bindings);
        assertCoveringRankPlan(native, sql, bindings);
      }
      assert.equal(native.prepare(boundarySql).get(unrelated)?.["workspace_id"], research);
      assert.equal(native.prepare(ordinalSql).get(research)?.["ordinal!: i64"], unrelated + 1);
      await page([permitted, 0, null, research, 1, 2], [notes, lowercaseAccent]);
      await page([permitted, 1, null, null, 0, 3], [lowercaseAccent, archived, notes]);
      await page([permitted, 0, "É%_", null, 1, 2], [research]);
      await page([permitted, 1, "É%_", null, 1, 3], [research, archived]);
      await page([permitted, 0, "\0%_ ÉnOde", null, 1, 2], [notes]);
      assert.throws(() => native.prepare(pageSql).all(...pageBinds, 0));

      const stock = (sql: string, bindings: readonly Binding[]): number => {
        const sqliteSteps = probe.stockCounters(database, sql, bindings)?.["Virtual Machine Steps"];
        assert.ok(sqliteSteps !== undefined, "expected stock SQLite statement counters");
        return sqliteSteps;
      };
      const receipt = {
        unrelated,
        workerd: {
          directoryReads,
          stateReads,
          boundaryReads,
          ordinalReads,
          previousDirectoryReads: (await probe.run(object, fullDirectorySql, [])).rowsRead,
        },
        native: {
          directorySteps: stock(pageSql, pageBinds),
          stateSteps: stock(accessSql, [notes]),
          boundarySteps: stock(boundarySql, [unrelated]),
          ordinalSteps: stock(ordinalSql, [research]),
          previousDirectorySteps: stock(fullDirectorySql, []),
        },
      };
      measurements.push(receipt);
      probe.diagnostic(JSON.stringify(receipt));
    } finally {
      native.close();
    }
  }
  const [sparse, dense] = measurements;
  assert.ok(sparse !== undefined && dense !== undefined);
  assert.equal(dense.workerd.directoryReads, sparse.workerd.directoryReads);
  assert.equal(dense.workerd.stateReads, sparse.workerd.stateReads);
  assert.equal(dense.native.directorySteps, sparse.native.directorySteps);
  assert.equal(dense.native.stateSteps, sparse.native.stateSteps);
  assert.ok(dense.workerd.boundaryReads > sparse.workerd.boundaryReads);
  assert.ok(dense.workerd.ordinalReads > sparse.workerd.ordinalReads);
  assert.ok(dense.native.ordinalSteps > sparse.native.ordinalSteps);
  assert.ok(dense.native.boundarySteps > sparse.native.boundarySteps);
  assert.ok(dense.workerd.previousDirectoryReads > sparse.workerd.previousDirectoryReads);
  assert.ok(dense.native.previousDirectorySteps > sparse.native.previousDirectorySteps);
}

const machineId = workspaceId(30_000);
const changeRequest = workspaceId(30_001);

function seedAllowances(): string {
  return `
INSERT INTO login_machines (machine_id, label, state, created_at, updated_at)
VALUES ('${machineId}', 'machine', 'enrolled', '${stamp}', '${stamp}');
INSERT INTO login_machine_allowances (
  allowance_id, machine_id, workspace_id, state, change_request_id, change_origin,
  created_at, updated_at
)
SELECT workspace_id, '${machineId}', workspace_id,
       CASE WHEN workspace_id IN ('${research}', '${notes}') THEN 'active' ELSE 'withdrawn' END,
       '${changeRequest}', '{"kind":"automatic"}', '${stamp}', '${stamp}'
FROM directory_workspaces
WHERE workspace_id NOT IN ('${archived}', '${lowercaseAccent}', '${unpermitted}');
`;
}

export async function checkMachineAllowanceBudget(probe: DirectoryBudgetProbe): Promise<void> {
  const pairSql = query("fleet-machine-login", "read_machine_allowance_pair");
  const activeSql = query("fleet-machine-login", "read_machine_allowances");
  const guardSql = query("fleet-machine-login", "guard_active_allowance_set");
  const measurements = [];
  for (const withdrawn of [10, 10_000]) {
    const object = "machine-allowance-budget-" + String(withdrawn);
    const steps = [...probe.schema, seed(withdrawn), seedAllowances()];
    await probe.prepare(object, steps);
    const database = join(probe.scratch, object + ".sqlite");
    const native = new DatabaseSync(database);
    try {
      for (const statement of steps) native.exec(statement);
      const activePair = [machineId, research];
      const withdrawnPair = [machineId, workspaceId(1)];
      const activeSet = [machineId, JSON.stringify([notes, research])];
      const wrongSet = [machineId, JSON.stringify([notes, workspaceId(1)])];
      const active = await probe.run(object, activeSql, [machineId]);
      const allow = await probe.run(object, pairSql, activePair);
      const withdrawnMatch = await probe.run(object, pairSql, withdrawnPair);
      const guard = await probe.run(object, guardSql, activeSet);
      const staleGuard = await probe.run(object, guardSql, wrongSet);
      assert.deepEqual(
        active.rows.map((row): Binding | undefined => row["workspace_id"]),
        [research, notes],
      );
      assert.equal(allow.rows[0]?.["state"], "active");
      assert.equal(withdrawnMatch.rows[0]?.["state"], "withdrawn");
      assert.equal(guard.rowsWritten, 1);
      assert.equal(staleGuard.rowsWritten, 0);
      assert.deepEqual(
        native
          .prepare(activeSql)
          .all(machineId)
          .map((row): object => ({ ...row })),
        active.rows,
      );
      assert.equal(native.prepare(pairSql).get(...activePair)?.["state"], "active");
      assert.equal(native.prepare(pairSql).get(...withdrawnPair)?.["state"], "withdrawn");
      assert.equal(native.prepare(guardSql).run(...activeSet).changes, 1);
      assert.equal(native.prepare(guardSql).run(...wrongSet).changes, 0);
      const workerd = {
        pairReads: allow.rowsRead,
        activeReads: active.rowsRead,
        guardReads: guard.rowsRead,
      };
      const nativeCounters = {
        pairSteps: probe.stockCounters(database, pairSql, activePair)?.["Virtual Machine Steps"],
        activeSteps: probe.stockCounters(database, activeSql, [machineId])?.[
          "Virtual Machine Steps"
        ],
        guardSteps: probe.stockCounters(database, guardSql, activeSet)?.["Virtual Machine Steps"],
      };
      assert.ok(nativeCounters.pairSteps !== undefined);
      assert.ok(nativeCounters.activeSteps !== undefined);
      assert.ok(nativeCounters.guardSteps !== undefined);
      measurements.push({ workerd, native: nativeCounters });
      probe.diagnostic(JSON.stringify({ withdrawn, workerd, native: nativeCounters }));
      if (withdrawn === 10_000) {
        const dropIndex = "DROP INDEX login_machine_allowances_active";
        await probe.prepare(object, [dropIndex]);
        native.exec(dropIndex);
        const unindexedActive = await probe.run(object, activeSql, [machineId]);
        const unindexedGuard = await probe.run(object, guardSql, activeSet);
        assert.deepEqual(unindexedActive.rows, active.rows);
        assert.equal(unindexedGuard.rowsWritten, guard.rowsWritten);
        assert.ok(unindexedActive.rowsRead > workerd.activeReads);
        assert.ok(unindexedGuard.rowsRead > workerd.guardReads);
        const activeSteps = probe.stockCounters(database, activeSql, [machineId])?.[
          "Virtual Machine Steps"
        ];
        const guardSteps = probe.stockCounters(database, guardSql, activeSet)?.[
          "Virtual Machine Steps"
        ];
        assert.ok(activeSteps !== undefined && activeSteps > nativeCounters.activeSteps);
        assert.ok(guardSteps !== undefined && guardSteps > nativeCounters.guardSteps);
        probe.diagnostic(
          JSON.stringify({
            withdrawn,
            withoutActiveIndex: {
              workerd: {
                activeReads: unindexedActive.rowsRead,
                guardReads: unindexedGuard.rowsRead,
              },
              native: { activeSteps, guardSteps },
            },
          }),
        );
      }
    } finally {
      native.close();
    }
  }
  const [sparse, dense] = measurements;
  assert.ok(sparse !== undefined && dense !== undefined);
  assert.deepEqual(sparse, dense);
}
