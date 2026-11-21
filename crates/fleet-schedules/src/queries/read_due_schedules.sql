SELECT subject_key AS "subject_key?: String",
       occurrence_ms AS "occurrence_ms!: i64",
       due_at_ms AS "effective_due_ms!: i64"
FROM schedule_due_rows
WHERE is_subject = 1 AND due_at_ms <= ?1
ORDER BY due_at_ms, subject_key
LIMIT ?2;
