SELECT id, request_id, kind, origin, actor_person_id, permission_checked, target_id,
       created_at, workspace_id, allowance_state
FROM signin_change_records
WHERE kind = 'machineRevoked'
  AND request_id >= ?1 || ':machineRevoked:'
  AND request_id < ?1 || ':machineRevoked;'
  AND request_id = ?1 || ':machineRevoked:' || target_id
LIMIT 2
