SELECT
    subject_key AS "subject_key?: String",
    occurrence_ms AS "occurrence_ms!: i64",
    effective_due_ms AS "effective_due_ms!: i64"
FROM engine_due_entries
WHERE work_kind = ?1 AND entry_role = 'item'
    AND effective_due_ms IS NOT NULL AND effective_due_ms <= ?2
ORDER BY effective_due_ms, subject_key
LIMIT ?3
