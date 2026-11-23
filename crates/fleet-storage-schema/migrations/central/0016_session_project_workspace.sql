-- Cutover starts empty. Existing rows in a test store have no project to infer
-- and cannot be decoded as registered sessions until the fixture supplies one.
ALTER TABLE actor_sessions ADD COLUMN project_id TEXT;
ALTER TABLE actor_sessions ADD COLUMN workspace_id TEXT
    REFERENCES directory_workspaces(workspace_id);
CREATE INDEX actor_sessions_by_workspace ON actor_sessions(workspace_id);
