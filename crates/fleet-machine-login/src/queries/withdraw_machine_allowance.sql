UPDATE login_machine_allowances
SET state = 'withdrawn', change_request_id = ?4, change_origin = ?5, updated_at = ?6
WHERE machine_id = ?1 AND workspace_id = ?2 AND allowance_id = ?3 AND state = 'active'
