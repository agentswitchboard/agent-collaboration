//! Drive C6 through an authenticated session link on either host.

use central_link_protocol::{
    CorrelationId,
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc},
};
use central_storage::{ReadOutcome, SqlValue, Statement};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, CreateSessionRefusal, OrganizationId, PresentedSessionSecret, ProjectRef,
    SessionCreator, StartProject,
};
use reqwest_websocket::WebSocket;

use crate::{
    ConformanceResult as Result,
    session_link_scenarios::{answer, call, create, read_reply, send_call},
    session_project_fixture::{
        FIRST_PROJECT, FIRST_WORKSPACE, SECOND_PROJECT, SECOND_WORKSPACE, SIBLING_PROJECT,
        project_at,
    },
};

const UNKNOWN_PROJECT: &str = "019a3dce-2000-7000-8000-000000005311";
const UNKNOWN_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000005312";
const CHILD_OPERATION: &str = "019a3dce-2000-7000-8000-000000005313";
const OUTSIDE_OPERATION: &str = "019a3dce-2000-7000-8000-000000005314";
const INHERITED_OPERATION: &str = "019a3dce-2000-7000-8000-000000005315";
const WITHDRAWN_OPERATION: &str = "019a3dce-2000-7000-8000-000000005316";

/// The parent Create response supplies both the session and the admitted workspace.
pub async fn admit_session_projects(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    mut read_sessions: impl AsyncFnMut() -> Result<Vec<Vec<SqlValue>>>,
) -> Result<Vec<Vec<SqlValue>>> {
    let mut request = create()?;
    let first_project = project_at(FIRST_WORKSPACE, FIRST_PROJECT)?;
    let second_project = project_at(SECOND_WORKSPACE, SECOND_PROJECT)?;
    for (project, reason) in [
        (
            StartProject::SameAsParent {},
            CreateSessionRefusal::ProjectRequired {},
        ),
        (
            named(FIRST_WORKSPACE, UNKNOWN_PROJECT)?,
            CreateSessionRefusal::ProjectNotInWorkspace {},
        ),
        (
            named(SECOND_WORKSPACE, FIRST_PROJECT)?,
            CreateSessionRefusal::ProjectNotInWorkspace {},
        ),
        (
            named(UNKNOWN_WORKSPACE, FIRST_PROJECT)?,
            CreateSessionRefusal::MachineNotAllowed {},
        ),
    ] {
        request.project = project;
        assert_eq!(
            create_reply(socket, organization, request.clone()).await?,
            CreateFleetSessionReply::Refused { reason }
        );
    }

    assert!(read_sessions().await?.is_empty());
    request.project = StartProject::Named {
        project_ref: first_project.clone(),
    };
    let CreateFleetSessionReply::Reserved {
        session_id,
        secret,
        workspace,
        ..
    } = create_reply(socket, organization, request.clone()).await?
    else {
        return Err("the correctly named project did not reserve a session".into());
    };
    assert_eq!(workspace, first_project.workspace);
    let first_write = read_sessions().await?;
    assert_eq!(
        create_reply(socket, organization, request.clone()).await?,
        CreateFleetSessionReply::Superseded {},
    );
    let mut changed = request.clone();
    changed.project = StartProject::Named {
        project_ref: second_project.clone(),
    };
    assert_eq!(
        create_reply(socket, organization, changed.clone()).await?,
        CreateFleetSessionReply::Conflict {},
    );
    assert_eq!(read_sessions().await?, first_write);
    let bound = call::<BindFleetSessionRpc>(
        socket,
        organization,
        BindFleetSession {
            session_id: session_id.clone(),
            harness_session_id: "project-parent".parse()?,
            secret: secret.clone(),
        },
    )
    .await?;
    let BindFleetSessionReply::Active { address } =
        answer::<BindFleetSessionRpc>(bound, organization)?
    else {
        return Err("the project parent did not bind".into());
    };
    assert_eq!(
        create_reply(socket, organization, request.clone()).await?,
        CreateFleetSessionReply::Active {
            session_id: session_id.clone(),
            address
        },
    );
    assert_eq!(
        create_reply(socket, organization, changed).await?,
        CreateFleetSessionReply::Conflict {},
    );

    request.creator = SessionCreator::Session {
        session_id: session_id.clone(),
        presented: PresentedSessionSecret::Secret { secret },
    };
    request.session_kind = CreateSessionKind::Delegate {
        parent: session_id.clone(),
    };
    request.operation_id = CHILD_OPERATION.parse()?;
    let sibling_project = project_at(FIRST_WORKSPACE, SIBLING_PROJECT)?;
    request.project = StartProject::Named {
        project_ref: sibling_project.clone(),
    };
    let named_child =
        expect_reserved(socket, organization, request.clone(), &sibling_project).await?;
    request.project = StartProject::SameAsParent {};
    request.operation_id = INHERITED_OPERATION.parse()?;
    let inherited_child =
        expect_reserved(socket, organization, request.clone(), &first_project).await?;
    request.operation_id = OUTSIDE_OPERATION.parse()?;
    request.project = StartProject::Named {
        project_ref: second_project,
    };
    assert_eq!(
        create_reply(socket, organization, request).await?,
        CreateFleetSessionReply::Refused {
            reason: CreateSessionRefusal::HelperOutsideWorkspace {}
        },
    );

    let mut sessions = [
        (session_id, FIRST_PROJECT),
        (named_child, SIBLING_PROJECT),
        (inherited_child, FIRST_PROJECT),
    ];
    sessions.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(sessions
        .into_iter()
        .map(|(session_id, project_id)| {
            vec![
                SqlValue::Text(session_id.as_str().to_owned()),
                SqlValue::Text(project_id.to_owned()),
                SqlValue::Text(FIRST_WORKSPACE.to_owned()),
            ]
        })
        .collect())
}

pub async fn send_project_create_before_withdrawal(
    socket: &mut WebSocket,
    organization: &OrganizationId,
) -> Result<CorrelationId> {
    let mut request = create()?;
    request.operation_id = WITHDRAWN_OPERATION.parse()?;
    send_call::<CreateFleetSessionRpc>(socket, organization, request).await
}

pub async fn expect_allowance_withdrawal_refusal(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    correlation: &CorrelationId,
) -> Result<()> {
    let reply = read_reply(socket, correlation).await?;
    assert_eq!(
        answer::<CreateFleetSessionRpc>(reply, organization)?,
        CreateFleetSessionReply::Refused {
            reason: CreateSessionRefusal::MachineNotAllowed {}
        },
    );
    Ok(())
}

pub fn record_session_projects() -> Statement {
    Statement {
        sql: "SELECT actor_id,project_id,workspace_id FROM actor_sessions ORDER BY actor_id",
        args: vec![],
    }
}

pub fn session_project_migration() -> Statement {
    Statement {
        sql: "SELECT description, CAST(success AS TEXT) FROM _sqlx_migrations WHERE version=16",
        args: vec![],
    }
}

pub fn assert_session_project_migration(outcome: ReadOutcome) {
    assert_eq!(
        outcome.rows,
        vec![vec![
            SqlValue::Text("session project workspace".into()),
            SqlValue::Text("1".into()),
        ]],
    );
}

fn named(workspace: &str, project: &str) -> Result<StartProject> {
    Ok(StartProject::Named {
        project_ref: project_at(workspace, project)?,
    })
}

async fn create_reply(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    request: CreateFleetSession,
) -> Result<CreateFleetSessionReply> {
    answer::<CreateFleetSessionRpc>(
        call::<CreateFleetSessionRpc>(socket, organization, request).await?,
        organization,
    )
}

async fn expect_reserved(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    request: CreateFleetSession,
    project: &ProjectRef,
) -> Result<workspace_ids::CentralSessionId> {
    let CreateFleetSessionReply::Reserved {
        session_id,
        workspace,
        ..
    } = create_reply(socket, organization, request).await?
    else {
        return Err("an in-workspace delegate did not reserve a session".into());
    };
    assert_eq!(workspace, project.workspace);
    Ok(session_id)
}
