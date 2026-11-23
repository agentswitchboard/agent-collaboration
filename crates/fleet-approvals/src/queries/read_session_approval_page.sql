SELECT requests.*
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND requests.organization_id = ?1
  AND destination.destination_kind = 'parent_session' AND destination.session_machine_id = ?2
  AND destination.session_enrollment_id = ?3 AND destination.session_id = ?4
  AND (?5 IS NULL OR requests.request_id < ?5)
ORDER BY requests.request_id DESC
LIMIT ?6
