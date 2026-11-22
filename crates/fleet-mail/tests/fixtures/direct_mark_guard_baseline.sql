SELECT occurrence_ms AS "occurrence_ms!: i64", due_at_ms AS "effective_due_ms!: i64"
FROM direct_message_due_rows
WHERE is_subject=1 AND subject_key=? AND contributes_to_alarm=1
