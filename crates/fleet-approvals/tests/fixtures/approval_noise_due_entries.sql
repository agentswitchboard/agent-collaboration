INSERT INTO engine_due_entries (
    due_entry_id, work_kind, entry_role, subject_key, occurrence_ms, base_due_ms,
    effective_due_ms, created_at, updated_at
)
SELECT
    '019e0000-0000-7000-8000-' || substr(approval_row_id, 25),
    'approval_request', 'item', approval_row_id, deadline_ms, deadline_ms, deadline_ms,
    created_at, updated_at
FROM approval_requests
WHERE approval_row_id LIKE '019b0000-0000-7000-8000-%' AND record_state = 'waiting'
