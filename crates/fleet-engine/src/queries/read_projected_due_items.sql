SELECT
    item.subject_key AS "subject_key?: String",
    occurrence_ms AS "occurrence_ms!: i64",
    effective_due_ms AS "effective_due_ms!: i64"
FROM engine_due_entries item
WHERE work_kind = ?1 AND entry_role = 'item'
    AND effective_due_ms IS NOT NULL AND effective_due_ms <= ?2
    AND (item.effective_due_ms <= ?4 OR NOT EXISTS (
        SELECT 1 FROM engine_work_attempts attempt
        WHERE attempt.machine_kind = item.work_kind AND attempt.subject_key = item.subject_key
    ))
ORDER BY effective_due_ms, item.subject_key
LIMIT ?3
