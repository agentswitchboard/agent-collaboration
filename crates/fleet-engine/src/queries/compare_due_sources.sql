SELECT
    machine_kind AS "kind!: String",
    subject_key AS "key!: String",
    due_at_ms AS "effective_due_ms?: i64"
FROM engine_due_attempt_controls
WHERE machine_kind IN ('engine_source', 'legacy_notice_windows', 'legacy_workspace_drains')
