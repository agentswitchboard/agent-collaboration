SELECT id, minimum_revision, state, permission_policy_digest, request_reject_through_ms
FROM signin_authorization_floors
WHERE id = ?
