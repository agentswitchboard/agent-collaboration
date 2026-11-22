ALTER TABLE signin_change_records ADD COLUMN workspace_id TEXT;
ALTER TABLE signin_change_records ADD COLUMN allowance_state TEXT;

ALTER TABLE login_machines DROP COLUMN allowed_workspaces;

CREATE TABLE login_machine_allowances (
    allowance_id TEXT NOT NULL PRIMARY KEY,
    machine_id TEXT NOT NULL REFERENCES login_machines(machine_id),
    workspace_id TEXT NOT NULL REFERENCES directory_workspaces(workspace_id),
    state TEXT NOT NULL,
    change_request_id TEXT NOT NULL,
    change_origin TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (machine_id, workspace_id)
);
