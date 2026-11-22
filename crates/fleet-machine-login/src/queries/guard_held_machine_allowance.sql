UPDATE login_machine_allowances
SET updated_at = updated_at
WHERE machine_id = ?1 AND workspace_id = ?2 AND allowance_id = ?3 AND state = ?4
