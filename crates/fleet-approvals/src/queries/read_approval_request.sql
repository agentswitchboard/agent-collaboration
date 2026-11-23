SELECT requests.*
FROM approval_requests AS requests
WHERE organization_id = ?1 AND request_id = ?2
