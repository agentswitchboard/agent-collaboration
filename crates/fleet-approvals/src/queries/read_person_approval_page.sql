SELECT requests.*
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND destination.destination_kind = 'people'
  AND destination.approval_row_id > ?2 AND requests.organization_id = ?1
ORDER BY destination.approval_row_id
LIMIT ?3
