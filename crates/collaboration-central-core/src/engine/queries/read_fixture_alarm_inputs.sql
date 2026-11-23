SELECT (
    SELECT MIN(due_at_ms) FROM (
        SELECT due_at_ms FROM engine_probe_due_rows WHERE contributes_to_alarm = 1
        UNION ALL
        SELECT due_at_ms FROM engine_alarm_only_due_rows WHERE contributes_to_alarm = 1
    )
    WHERE EXISTS (SELECT 1 FROM signin_provisioned_for_engine)
) AS "on_grid_due?: i64",
NULL AS "legacy_due?: i64"
