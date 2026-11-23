SELECT destination.approval_destination_id, requests.approval_row_id
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND requests.organization_id = ?1
  AND destination.destination_kind = 'parent_session' AND destination.session_machine_id = ?2
  AND destination.session_enrollment_id = ?3
  AND requests.record_state = 'waiting'
  AND (?4 IS NULL OR destination.approval_destination_id > ?4)
ORDER BY destination.approval_destination_id
LIMIT ?5
