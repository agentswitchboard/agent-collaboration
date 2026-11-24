UPDATE approval_request_destinations
SET first_read_at = ?1, updated_at = MAX(updated_at, ?1)
WHERE approval_row_id = ?2 AND organization_id = ?3 AND destination_kind = 'parent_session'
    AND session_machine_id = ?4 AND session_enrollment_id = ?5 AND session_id = ?6
    AND first_read_at IS NULL
