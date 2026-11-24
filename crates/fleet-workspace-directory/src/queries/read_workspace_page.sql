SELECT workspace_id, name, description, state, created_at, updated_at
FROM directory_workspaces
WHERE workspace_id > ?
ORDER BY workspace_id
LIMIT ?
