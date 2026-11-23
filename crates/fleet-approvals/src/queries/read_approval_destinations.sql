SELECT approval_destination_id, approval_row_id, organization_id, request_id, destination_kind,
       session_machine_id, session_enrollment_id, session_id, raised_notice_at,
       position, first_read_at, created_at, updated_at
FROM approval_request_destinations
WHERE approval_row_id = ?1
ORDER BY position
