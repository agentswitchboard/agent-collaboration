SELECT requests.*
FROM approval_requests AS requests
WHERE approval_row_id = ?1 AND organization_id = ?2
