//! Seed real organization allowances and project rows for session link scenarios.

use central_storage::{
    AlarmUpdate, GuardedStatement, RowExpectation, SqlValue, Statement, StatementBatch,
};
use fleet_identity_protocol::ProjectRef;

use crate::ConformanceResult;

pub const FIRST_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000000004";
pub const FIRST_PROJECT: &str = "019a3dce-2000-7000-8000-000000000005";
pub const SECOND_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000000006";
pub const SECOND_PROJECT: &str = "019a3dce-2000-7000-8000-000000000007";
const FIRST_ALLOWANCE: &str = "019a3dce-2000-7000-8000-000000000008";
const COMPETING_ALLOWANCE: &str = "019a3dce-2000-7000-8000-00000000000c";
const SECOND_ALLOWANCE: &str = "019a3dce-2000-7000-8000-000000000009";
pub const SIBLING_PROJECT: &str = "019a3dce-2000-7000-8000-00000000000b";
pub const UNALLOWED_WORKSPACE: &str = "019a3dce-2000-7000-8000-00000000000d";
pub const FOREIGN_WORKSPACE: &str = "019a3dce-2000-7000-8000-00000000000e";
const PROJECT_AREA: &str = "019a3dce-2000-7000-8000-00000000000a";

pub fn project_at(workspace: &str, project: &str) -> ConformanceResult<ProjectRef> {
    Ok(ProjectRef {
        workspace: workspace.parse()?,
        project: project.parse()?,
    })
}

pub fn session_project_allowances(machine: &str) -> StatementBatch {
    let mut statements = Vec::new();
    for (workspace, allowance) in [
        (FIRST_WORKSPACE, FIRST_ALLOWANCE),
        (SECOND_WORKSPACE, SECOND_ALLOWANCE),
    ] {
        statements.push(insert_workspace(workspace));
        statements.push(allow_machine_session_start(machine, workspace, allowance));
    }
    batch(statements)
}

pub fn directory_entry(workspace: &str) -> StatementBatch {
    batch(vec![insert_workspace(workspace)])
}

fn insert_workspace(workspace: &str) -> GuardedStatement {
    write(
        "INSERT INTO directory_workspaces (workspace_id,name,state,created_at,updated_at) VALUES (?,?,'active','2026-11-22T10:00:00.000Z','2026-11-22T10:00:00.000Z')",
        &[workspace, workspace],
    )
}

pub fn archive_session_workspace(workspace: &str) -> StatementBatch {
    batch(vec![write(
        "UPDATE directory_workspaces SET state='archived' WHERE workspace_id=?",
        &[workspace],
    )])
}

pub fn restore_session_allowance(machine: &str, workspace: &str) -> StatementBatch {
    batch(vec![write(
        "UPDATE login_machine_allowances SET state='active' WHERE machine_id=? AND workspace_id=?",
        &[machine, workspace],
    )])
}

pub fn seed_session_project(project: &str) -> StatementBatch {
    let mut statements = vec![
        write(
            "INSERT INTO workspace_areas (area_id,name,description,state,created_at,updated_at) VALUES (?,'Session fixture area','Session projects','open','2026-11-22T10:00:00.000Z','2026-11-22T10:00:00.000Z')",
            &[PROJECT_AREA],
        ),
        insert_project(project),
    ];
    if project == FIRST_PROJECT {
        statements.push(insert_project(SIBLING_PROJECT));
    }
    batch(statements)
}

fn insert_project(project: &str) -> GuardedStatement {
    write(
        "INSERT INTO workspace_projects (project_id,area_id,title,description,status,created_at,updated_at) VALUES (?,?,'Session fixture project','Session starts','open','2026-11-22T10:00:00.000Z','2026-11-22T10:00:00.000Z')",
        &[project, PROJECT_AREA],
    )
}

pub fn allow_competing_session_machine(machine: &str) -> StatementBatch {
    batch(vec![allow_machine_session_start(
        machine,
        FIRST_WORKSPACE,
        COMPETING_ALLOWANCE,
    )])
}

fn allow_machine_session_start(
    machine: &str,
    workspace: &str,
    allowance: &str,
) -> GuardedStatement {
    write(
        "INSERT INTO login_machine_allowances (allowance_id,machine_id,workspace_id,state,change_request_id,change_origin,created_at,updated_at) VALUES (?,?,?,'active',?,'automatic','2026-11-22T10:00:00.000Z','2026-11-22T10:00:00.000Z')",
        &[allowance, machine, workspace, allowance],
    )
}

pub fn withdraw_session_allowance(machine: &str, workspace: &str) -> StatementBatch {
    batch(vec![write(
        "UPDATE login_machine_allowances SET state='withdrawn' WHERE machine_id=? AND workspace_id=?",
        &[machine, workspace],
    )])
}

fn write(sql: &'static str, parameters: &[&str]) -> GuardedStatement {
    GuardedStatement {
        statement: Statement {
            sql,
            args: parameters
                .iter()
                .map(|value| SqlValue::Text((*value).to_owned()))
                .collect(),
        },
        expect: RowExpectation::Exactly(1),
    }
}

fn batch(statements: Vec<GuardedStatement>) -> StatementBatch {
    StatementBatch {
        statements,
        alarm: AlarmUpdate::Unchanged,
    }
}
