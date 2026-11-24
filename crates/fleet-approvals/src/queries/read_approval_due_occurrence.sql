SELECT subject_key FROM engine_due_entries
WHERE subject_key = ?1 AND work_kind = 'approval_request' AND entry_role = 'item'
  AND occurrence_ms = ?2
