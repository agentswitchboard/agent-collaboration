WITH notice_window_minimum(due_at_ms) AS (
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM engine_due_entries
        WHERE work_kind = 'legacy_notice_windows'
            AND entry_role = 'source_control' AND subject_key = work_kind
    ) THEN (
        SELECT effective_due_ms FROM engine_due_entries
        WHERE work_kind = 'legacy_notice_windows'
            AND entry_role = 'source_control' AND subject_key = work_kind
    ) ELSE (
        SELECT CASE WHEN unixepoch(due_at) IS NULL
             OR abs(unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)) > 9223372036854
             THEN 0 ELSE unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER) END
        FROM mail_notification_windows INDEXED BY notification_windows_due
        WHERE due_at IS NOT NULL ORDER BY due_at LIMIT 1
    ) END
),
workspace_drain_minimum(due_at_ms) AS (
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM engine_due_entries
        WHERE work_kind = 'legacy_workspace_drains'
            AND entry_role = 'source_control' AND subject_key = work_kind
    ) THEN (
        SELECT effective_due_ms FROM engine_due_entries
        WHERE work_kind = 'legacy_workspace_drains'
            AND entry_role = 'source_control' AND subject_key = work_kind
    ) ELSE (
        SELECT CASE WHEN unixepoch(drain_due_at) IS NULL
             OR abs(unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER)) > 9223372036854
             THEN 0 ELSE unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER) END
        FROM mail_workspace_drains INDEXED BY workspace_drains_due
        WHERE drain_due_at IS NOT NULL ORDER BY drain_due_at LIMIT 1
    ) END
),
grid_kinds(work_kind) AS (
    SELECT value FROM json_each(?1)
),
grid_minima(due_at_ms) AS (
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM engine_due_entries d
        WHERE d.work_kind = k.work_kind
            AND d.entry_role = 'source_control' AND d.subject_key = k.work_kind
    ) THEN (
        SELECT effective_due_ms FROM engine_due_entries d
        WHERE d.work_kind = k.work_kind
            AND d.entry_role = 'source_control' AND d.subject_key = k.work_kind
    ) ELSE (
        SELECT effective_due_ms FROM engine_due_entries d
        WHERE d.work_kind = k.work_kind AND d.entry_role = 'item'
            AND d.effective_due_ms IS NOT NULL
        ORDER BY d.effective_due_ms, d.subject_key LIMIT 1
    ) END
    FROM grid_kinds k
)
SELECT
    (SELECT MIN(due_at_ms) FROM grid_minima) AS "on_grid_due?: i64",
    (SELECT due_at_ms FROM notice_window_minimum) AS "notice_window_due?: i64",
    (SELECT due_at_ms FROM workspace_drain_minimum) AS "workspace_drain_due?: i64"
WHERE EXISTS (SELECT 1 FROM signin_provisioned_for_engine)
