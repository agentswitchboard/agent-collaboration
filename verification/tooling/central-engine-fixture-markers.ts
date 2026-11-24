// Fixture tables, views, probe-kind SQL and frozen query oracles have no
// executable use in either production host.
export const engineFixtureSqlMarkers = [
  "engine_probe_due_rows",
  "engine_alarm_only_due_rows",
  "engine_probe_groups",
  "engine_probe_items",
  "engine_alarm_only_items",
  "engine_probe_completed_claim",
  "s.machine_kind='engine_probe'",
  "'owed_reply',?,NULL",
  "CASE WHEN (SELECT COUNT(*) FROM signin_organization_identity)=1 THEN NULLIF(",
  "SELECT subject_key,occurrence_ms,effective_due_ms FROM (SELECT",
  "checked Engine widget fixture",
] as const;

/** SQLite identifiers are ASCII case-insensitive. Latin-1 keeps the artifact bytes intact. */
export function findEngineFixtureSql(content: Buffer): string | undefined {
  const artifact = content.toString("latin1").toLowerCase();
  return engineFixtureSqlMarkers.find((marker) => artifact.includes(marker.toLowerCase()));
}
