UPDATE signin_authorization_floors
SET id = id
WHERE id = ?
  AND minimum_revision = ?
  AND state = ?
  AND permission_policy_digest = ?
  AND request_reject_through_ms = ?
