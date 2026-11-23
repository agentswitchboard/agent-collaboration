SELECT
    (SELECT MIN(due_at_ms) FROM notice_window_due_rows WHERE contributes_to_alarm = 1)
        AS "notice_window_due?: i64",
    (SELECT MIN(due_at_ms) FROM workspace_drain_due_rows WHERE contributes_to_alarm = 1)
        AS "workspace_drain_due?: i64"
WHERE EXISTS (SELECT 1 FROM signin_provisioned_for_engine)
