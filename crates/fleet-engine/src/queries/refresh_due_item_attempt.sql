UPDATE engine_due_entries AS due SET effective_due_ms = (
    SELECT CASE
        WHEN attempt.retired THEN NULL
        WHEN attempt.uses_subject_attempt THEN attempt.due_at_ms
        ELSE due.base_due_ms
    END
    FROM (SELECT 1)
    LEFT JOIN engine_due_attempt_controls attempt
        ON attempt.machine_kind = due.work_kind AND attempt.subject_key = due.subject_key
), updated_at = ?3
WHERE work_kind = ?1 AND entry_role = 'item' AND subject_key = ?2
