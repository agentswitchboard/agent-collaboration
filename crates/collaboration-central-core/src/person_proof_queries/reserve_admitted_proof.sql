INSERT INTO admin_used_requests
    (id, organization_id, request_time_ms, operation_id, actor_person_id,
     actor_session_id, intent_digest, proof_key_thumbprint, created_at, updated_at)
SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9
WHERE ?9 < ?10
  AND ?3 > ?9 - 65000
  AND ?3 <= ?9 + 5000
  AND ?11 > ?9 - 65000
  AND ?11 <= ?9 + 5000
  AND (?13 = 0 OR (?12 IS NOT NULL AND ?12 * 1000 <= ?9 AND ?9 - ?12 * 1000 <= 900000))
  AND ?3 > (SELECT request_reject_through_ms FROM signin_authorization_floors WHERE id = ?2)
  AND NOT EXISTS (SELECT 1 FROM admin_used_requests WHERE id = ?1)
