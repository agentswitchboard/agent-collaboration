SELECT requests.*
FROM approval_request_destinations AS destination INDEXED BY approval_destinations_session_reads
CROSS JOIN approval_requests AS requests
WHERE destination.organization_id = ?1
  AND destination.request_id = ?2
  AND destination.destination_kind = 'parent_session'
  AND destination.session_machine_id = ?3
  AND destination.session_enrollment_id = ?4
  AND destination.session_id = ?5
  AND requests.approval_row_id = destination.approval_row_id
  AND requests.organization_id = destination.organization_id
