INSERT INTO engine_due_entries (
    due_entry_id, work_kind, entry_role, subject_key, occurrence_ms, base_due_ms,
    effective_due_ms, created_at, updated_at
)
SELECT ?1, ?2, 'source_control', ?2, NULL, NULL, due_at_ms, ?4, ?4
FROM engine_due_attempt_controls
WHERE machine_kind = ?3 AND subject_key = ?5
ON CONFLICT (work_kind, entry_role, subject_key) DO UPDATE SET
    effective_due_ms = excluded.effective_due_ms,
    updated_at = excluded.updated_at
