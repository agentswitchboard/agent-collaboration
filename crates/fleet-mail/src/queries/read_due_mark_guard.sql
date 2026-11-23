WITH message_mark(occurrence_ms) AS MATERIALIZED (
  SELECT occurrence_ms FROM direct_message_due_candidates WHERE message_id=?1
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
