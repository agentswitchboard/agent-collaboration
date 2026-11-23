DELETE FROM engine_due_entries
WHERE work_kind = ?1 AND entry_role = 'item' AND subject_key = ?2
