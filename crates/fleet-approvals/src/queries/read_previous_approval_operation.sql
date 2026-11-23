SELECT requests.*
FROM approval_requests AS requests
WHERE organization_id = ?1 AND decided_operation_id = ?2
