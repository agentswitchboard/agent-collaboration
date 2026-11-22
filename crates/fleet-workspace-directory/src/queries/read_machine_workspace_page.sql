SELECT workspace.workspace_id, workspace.name, workspace.description,
       workspace.state, workspace.created_at, workspace.updated_at
FROM json_each(?1) AS permitted
CROSS JOIN directory_workspaces AS workspace
  ON workspace.workspace_id = permitted.value
WHERE (?2 = 1 OR workspace.state <> 'archived')
  AND (?3 IS NULL
       OR instr(lower(workspace.name), lower(?3)) > 0
       OR instr(lower(workspace.description), lower(?3)) > 0)
  AND (?4 IS NULL
       OR (?5 = 1 AND workspace.workspace_id > ?4)
       OR (?5 = 0 AND workspace.workspace_id < ?4))
ORDER BY CASE WHEN ?5 = 1 THEN workspace.workspace_id END ASC,
         CASE WHEN ?5 = 0 THEN workspace.workspace_id END DESC
LIMIT ?6
