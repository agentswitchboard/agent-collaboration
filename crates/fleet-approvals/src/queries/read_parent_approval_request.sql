SELECT requests.*
FROM approval_requests AS requests
WHERE requests.organization_id = ?1 AND requests.request_id = ?2
  AND EXISTS (
      SELECT 1 FROM approval_request_destinations AS destination
      WHERE destination.approval_row_id = requests.approval_row_id
        AND destination.organization_id = requests.organization_id
        AND destination.destination_kind = 'parent_session'
        AND destination.session_machine_id = ?3
        AND destination.session_enrollment_id = ?4
        AND destination.session_id = ?5
  )
