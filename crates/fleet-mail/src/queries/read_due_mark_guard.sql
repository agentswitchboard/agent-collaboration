WITH message_mark(occurrence_ms) AS (
  SELECT CASE
    WHEN unixepoch(cancel_due_at) IS NULL OR abs(unixepoch(cancel_due_at)*1000+CAST(substr(strftime('%f',cancel_due_at),4,3) AS INTEGER))>9223372036854
    THEN 0
    ELSE unixepoch(cancel_due_at)*1000+CAST(substr(strftime('%f',cancel_due_at),4,3) AS INTEGER)
  END
  FROM mail_messages
  WHERE message_id=?1 AND message_kind='direct'
    AND state IN ('queued','delivered') AND cancel_due_at IS NOT NULL
)
SELECT occurrence_ms AS "occurrence_ms!: i64",
       CASE WHEN attempt.uses_subject_attempt=1 THEN attempt.due_at_ms ELSE occurrence_ms END AS "effective_due_ms!: i64"
FROM message_mark
LEFT JOIN engine_due_attempt_controls attempt
  ON attempt.machine_kind='direct_message' AND attempt.subject_key=?1
WHERE COALESCE(attempt.retired,0)=0
  AND NOT EXISTS (
    SELECT 1 FROM engine_due_attempt_controls
    WHERE machine_kind='engine_source' AND subject_key='direct_message'
  );
