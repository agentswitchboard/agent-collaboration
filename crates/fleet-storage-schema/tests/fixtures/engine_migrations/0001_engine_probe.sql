-- Test-support only. The host's fixture migrator appends this after production
-- migrations; no production database receives these machine subjects.
CREATE TABLE engine_probe_groups (
    group_id TEXT PRIMARY KEY,
    effect_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE engine_probe_items (
    item_id TEXT PRIMARY KEY,
    machine_kind TEXT NOT NULL,
    group_id TEXT NOT NULL REFERENCES engine_probe_groups(group_id),
    due_at TEXT,
    claim TEXT NOT NULL,
    state TEXT NOT NULL,
    completed_claim TEXT,
    send_id TEXT,
    connection_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX engine_probe_completed_claim ON engine_probe_items(completed_claim);
-- A timed fixture kind that supplies an alarm without running a due unit.
CREATE TABLE engine_alarm_only_items (
    item_id TEXT PRIMARY KEY,
    due_at TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Extend the shared query's facade without changing either host's statement.
CREATE VIEW engine_probe_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='engine_probe'
)
SELECT 'on_grid', 1, s.item_id, CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='waiting' THEN 0 ELSE CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM engine_probe_items s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='engine_probe' AND a.subject_key=s.item_id
LEFT JOIN source_attempt ON 1
WHERE s.state NOT IN ('fired','stopped') AND s.machine_kind='engine_probe' AND s.due_at IS NOT NULL AND COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE VIEW engine_alarm_only_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='engine_alarm_only'
)
SELECT 'on_grid', 1, s.item_id, CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='waiting' THEN 0 ELSE CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM engine_alarm_only_items s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='engine_alarm_only' AND a.subject_key=s.item_id
LEFT JOIN source_attempt ON 1
WHERE s.state NOT IN ('fired','stopped') AND s.due_at IS NOT NULL AND COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

DROP VIEW engine_due_rows;

CREATE VIEW engine_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_production_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_probe_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_alarm_only_due_rows;
