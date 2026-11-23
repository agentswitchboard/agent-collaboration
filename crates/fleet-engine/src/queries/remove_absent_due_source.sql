DELETE FROM engine_due_entries
WHERE work_kind = ?1 AND entry_role = 'source_control' AND subject_key = ?1
    AND NOT EXISTS (
        SELECT 1 FROM engine_work_attempts
        WHERE machine_kind = ?2 AND subject_key = ?3
    )
