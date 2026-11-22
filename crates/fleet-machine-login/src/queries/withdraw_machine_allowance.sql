UPDATE login_machine_allowances
SET state = 'withdrawn', change_request_id = ?3, change_origin = ?4, updated_at = ?5
WHERE machine_id = ?1 AND workspace_id = ?2 AND state = 'active'
