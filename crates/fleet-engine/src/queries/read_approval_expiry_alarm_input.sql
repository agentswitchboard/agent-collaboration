SELECT MIN(due_at_ms) AS "approval_due_ms?: i64"
FROM approval_request_expiry_due_rows
WHERE contributes_to_alarm = 1
  AND EXISTS (SELECT 1 FROM signin_provisioned_for_engine)
