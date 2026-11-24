SELECT allowance.state AS allowance_state, workspace.state AS workspace_state
FROM login_machine_allowances AS allowance
LEFT JOIN directory_workspaces AS workspace ON workspace.workspace_id = allowance.workspace_id
WHERE allowance.machine_id = ? AND allowance.workspace_id = ?
