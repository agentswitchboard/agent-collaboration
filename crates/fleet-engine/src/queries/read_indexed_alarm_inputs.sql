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
        SELECT CASE WHEN substr(due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(due_at,5,1) <> '-' THEN 0 WHEN unixepoch(due_at) IS NULL
             OR abs(unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)) > 9223372036854
             THEN 0 ELSE unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER) END AS decoded_due_ms
        FROM mail_notification_windows INDEXED BY notice_window_decoded_due
        WHERE due_at IS NOT NULL ORDER BY decoded_due_ms LIMIT 1
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
        SELECT CASE WHEN substr(drain_due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(drain_due_at,5,1) <> '-' THEN 0 WHEN unixepoch(drain_due_at) IS NULL
             OR abs(unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER)) > 9223372036854
             THEN 0 ELSE unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER) END AS decoded_due_ms
        FROM mail_workspace_drains INDEXED BY workspace_drain_decoded_due
        WHERE drain_due_at IS NOT NULL ORDER BY decoded_due_ms LIMIT 1
    ) END
),
grid_inputs AS (
    SELECT value AS work_kind, source.entry_role IS NOT NULL AS source_controls,
           source.effective_due_ms AS source_due_ms,
           (
               SELECT item.effective_due_ms FROM engine_due_entries item
               WHERE item.work_kind = kinds.value AND item.entry_role = 'item'
                   AND item.effective_due_ms IS NOT NULL
               ORDER BY item.effective_due_ms, item.subject_key LIMIT 1
           ) AS item_due_ms
    FROM json_each(?1) kinds
    LEFT JOIN engine_due_entries source
        ON source.work_kind = kinds.value AND source.entry_role = 'source_control'
        AND source.subject_key = kinds.value
),
early_occurrences AS (
    SELECT (
        SELECT item.effective_due_ms FROM engine_due_entries item
        WHERE item.work_kind = input.work_kind AND item.entry_role = 'item'
            AND item.effective_due_ms IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM engine_work_attempts attempt
                WHERE attempt.machine_kind = item.work_kind AND attempt.subject_key = item.subject_key
            )
        ORDER BY item.effective_due_ms, item.subject_key LIMIT 1
    ) AS due_ms
    FROM grid_inputs input
    WHERE NOT input.source_controls AND input.work_kind IN ('wake_timer', 'automation_timer')
),
ordinary_retries AS (
    SELECT MIN(item.effective_due_ms) AS due_ms
    FROM grid_inputs input
    JOIN engine_work_attempts attempt ON attempt.machine_kind = input.work_kind
    JOIN engine_due_entries item
        ON item.work_kind = input.work_kind AND item.entry_role = 'item'
        AND item.subject_key = attempt.subject_key
    WHERE NOT input.source_controls AND input.work_kind IN ('wake_timer', 'automation_timer')
        AND item.effective_due_ms IS NOT NULL
),
exact_inputs(due_ms) AS (
    SELECT source_due_ms FROM grid_inputs WHERE source_controls
    UNION ALL
    SELECT item_due_ms FROM grid_inputs
    WHERE NOT source_controls AND work_kind IN ('direct_message', 'system_message_expiry')
    UNION ALL
    SELECT due_ms FROM ordinary_retries
)
SELECT
    (SELECT MIN(CASE WHEN source_controls THEN source_due_ms ELSE item_due_ms END)
        FROM grid_inputs) AS "on_grid_due?: i64",
    (SELECT due_at_ms FROM notice_window_minimum) AS "notice_window_due?: i64",
    (SELECT due_at_ms FROM workspace_drain_minimum) AS "workspace_drain_due?: i64",
    (SELECT MIN(due_ms) FROM early_occurrences) AS "look_ahead_due?: i64",
    (SELECT MIN(due_ms) FROM exact_inputs) AS "exact_due?: i64",
    (SELECT item_due_ms FROM grid_inputs WHERE work_kind = 'approval_request' AND NOT source_controls)
        AS "approval_due?: i64"
WHERE EXISTS (SELECT 1 FROM signin_provisioned_for_engine)
