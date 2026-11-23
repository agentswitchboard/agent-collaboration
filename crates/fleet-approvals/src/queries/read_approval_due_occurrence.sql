SELECT approval.approval_row_id
FROM engine_due_entries AS due
JOIN approval_requests AS approval ON approval.approval_row_id = due.subject_key
WHERE approval.organization_id = ?1 AND due.subject_key = ?2
  AND due.work_kind = 'approval_request' AND due.entry_role = 'item'
  AND due.occurrence_ms = ?3
