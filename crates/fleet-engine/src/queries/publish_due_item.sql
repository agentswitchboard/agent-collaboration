INSERT INTO engine_due_entries (
    due_entry_id, work_kind, entry_role, subject_key, occurrence_ms, base_due_ms,
    effective_due_ms, created_at, updated_at
)
SELECT ?1, ?2, 'item', ?3, ?4, ?5,
    CASE
        WHEN attempt.retired THEN NULL
        WHEN attempt.uses_subject_attempt THEN attempt.due_at_ms
        ELSE ?5
    END, ?6, ?6
FROM (SELECT 1)
LEFT JOIN engine_due_attempt_controls attempt
    ON attempt.machine_kind = ?2 AND attempt.subject_key = ?3
WHERE ?7 = 0 OR changes() = 1
ON CONFLICT (work_kind, entry_role, subject_key) DO UPDATE SET
    occurrence_ms = excluded.occurrence_ms,
    base_due_ms = excluded.base_due_ms,
    effective_due_ms = excluded.effective_due_ms,
    updated_at = excluded.updated_at
