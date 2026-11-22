SELECT
  MIN(CASE WHEN due_lane = 'on_grid' THEN due_at_ms END) AS "on_grid_due?: i64",
  MIN(CASE WHEN due_lane = 'legacy' THEN due_at_ms END) AS "legacy_due?: i64"
FROM engine_due_rows
WHERE contributes_to_alarm = 1
  AND EXISTS (SELECT 1 FROM signin_provisioned_for_engine);
