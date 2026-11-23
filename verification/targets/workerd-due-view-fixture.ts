import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before } from "node:test";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { z } from "zod";

export const dueViewQueryResultSchema = z.object({
  rows: z.array(z.record(z.string(), z.union([z.null(), z.string(), z.number()]))),
  rowsRead: z.number(),
  rowsWritten: z.number(),
});
export type DueViewQueryResult = z.infer<typeof dueViewQueryResultSchema>;

// Only the owning fixture object receives SQL. The same production statement
// is loaded by the Engine's native compile witness and its DO transaction.
const workerSource = [
  "export class DueViewObject {",
  "  constructor(ctx) { this.ctx = ctx; this.sql = ctx.storage.sql; }",
  "  async fetch(request) {",
  "    const { sql, setup, bindings = [] } = await request.json();",
  "    if (setup) {",
  "      this.ctx.storage.transactionSync(() => this.sql.exec(sql).toArray());",
  "      return Response.json({ rows: [], rowsRead: 0, rowsWritten: 0 });",
  "    }",
  "    const cursor = this.sql.exec(sql, ...bindings);",
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
export let scratchDirectory: string;
before(async (): Promise<void> => {
  scratchDirectory = mkdtempSync(join(tmpdir(), "engine-due-views-"));
  runtime = new Miniflare(
    convertV4MiniflareOptions({
      host: "127.0.0.1",
      port: 0,
      cf: false,
      resourcePersistencePath: scratchDirectory,
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
  if (scratchDirectory !== undefined) rmSync(scratchDirectory, { recursive: true, force: true });
});

interface QueryOptions {
  readonly setup?: boolean;
  readonly bindings?: readonly (number | string | null)[];
}

export async function runDueViewQuery(
  object: string,
  sql: string,
  options: QueryOptions = {},
): Promise<DueViewQueryResult> {
  const response = await fetch(new URL("/" + object, origin), {
    method: "POST",
    headers: { "Content-Type": "application/json", Connection: "close" },
    body: JSON.stringify({ sql, ...options }),
  });
  if (!response.ok) throw new Error(object + ": " + (await response.text()));
  return dueViewQueryResultSchema.parse(await response.json());
}

export async function prepareDueViewStore(object: string, steps: readonly string[]): Promise<void> {
  for (const sql of steps) await runDueViewQuery(object, sql, { setup: true });
}
const grid = "on_grid_due?: i64";
const legacy = "legacy_due?: i64";

export function alarmTimePair(
  rows: DueViewQueryResult["rows"],
  current: boolean,
): readonly unknown[] {
  assert.equal(rows.length, 1);
  const [row] = rows;
  assert.ok(row !== undefined);
  return current ? [row[grid], row[legacy]] : [row["on_grid_due"], row["legacy_due"]];
}

export function replaceRequiredSql(sql: string, from: string | RegExp, to: string): string {
  const result = sql.replace(from, to);
  assert.notEqual(result, sql, "the negative control must change the production DDL");
  return result;
}

export async function dueViewQueryPlan(object: string, sql: string): Promise<string> {
  const { rows } = await runDueViewQuery(object, "EXPLAIN QUERY PLAN " + sql);
  return rows.map((row): string => z.string().parse(row["detail"])).join("\n");
}
export const repositoryDirectory = new URL("../../", import.meta.url);
export const migrationDirectory = new URL(
  "crates/fleet-storage-schema/migrations/central/",
  repositoryDirectory,
);
const dueMigration = "0005_engine_due_views.sql";
export const dueSchema = readFileSync(new URL(dueMigration, migrationDirectory), "utf8");
export const signinAlarmSchema = readFileSync(
  new URL("0006_signin_provisioned_for_engine.sql", migrationDirectory),
  "utf8",
);
export const oldSchema = readdirSync(migrationDirectory)
  .filter((name): boolean => name < dueMigration)
  .sort()
  .map((name): string => readFileSync(new URL(name, migrationDirectory), "utf8"));
export const nextAlarm = readFileSync(
  new URL("crates/fleet-engine/src/queries/read_alarm_inputs.sql", repositoryDirectory),
  "utf8",
);
export const oldAlarm = readFileSync(
  new URL(
    "crates/collaboration-central-core/tests/fixtures/main_engine_alarm_inputs.sql",
    repositoryDirectory,
  ),
  "utf8",
);
export const overlay = readFileSync(
  new URL(
    "crates/fleet-storage-schema/tests/fixtures/engine_migrations/0001_engine_probe.sql",
    repositoryDirectory,
  ),
  "utf8",
);

export function stockCounters(
  database: string,
  sql: string,
  bindings: readonly (number | string | null)[] = [],
): Record<string, number> | null {
  const parameters = bindings.map((value, index): string => {
    const encoded =
      typeof value === "string"
        ? "CAST(X'" + Buffer.from(value).toString("hex") + "' AS TEXT)"
        : String(value);
    return "('?" + String(index + 1) + "'," + encoded + ")";
  });
  const setup =
    parameters.length === 0
      ? []
      : [
          ".parameter init",
          "INSERT INTO temp.sqlite_parameters (key,value) VALUES " + parameters.join(",") + ";",
        ];
  const result = spawnSync("sqlite3", ["-batch", database, ...setup, ".stats stmt", sql], {
    encoding: "utf8",
  });
  if (result.status !== 0) return null;
  return Object.fromEntries(
    [...result.stdout.matchAll(/^(Fullscan Steps|Virtual Machine Steps):\s+(\d+)$/gm)].map(
      ([, name, count]): [string, number] => [name ?? "", Number(count)],
    ),
  );
}
