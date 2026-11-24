//! Shared real-link scenarios for the session start review corrections.

use central_link_protocol::{CorrelationId, session_lifecycle_rpc::CreateFleetSessionRpc};
use central_storage::SqlValue;
use fleet_identity_protocol::{
    CreateFleetSession, CreateFleetSessionReply, CreateSessionRefusal, OrganizationId, StartProject,
};
use reqwest_websocket::WebSocket;

use crate::{
    ConformanceResult as Result,
    session_link_scenarios::{create, send_call},
    session_project_fixture::{
        FIRST_PROJECT, FIRST_WORKSPACE, FOREIGN_WORKSPACE, SECOND_PROJECT, SECOND_WORKSPACE,
        UNALLOWED_WORKSPACE, project_at,
    },
    session_project_scenarios::{create_reply, expect_reserved},
};

const UNKNOWN_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000005319";
const ROTATION_OPERATION: &str = "019a3dce-2000-7000-8000-00000000531a";
const ARCHIVE_OPERATION: &str = "019a3dce-2000-7000-8000-00000000531b";

pub async fn reserve_for_rotation(
    socket: &mut WebSocket,
    organization: &OrganizationId,
) -> Result<(CreateFleetSession, CreateFleetSession)> {
    let mut initial = create()?;
    initial.operation_id = ROTATION_OPERATION.parse()?;
    expect_reserved(
        socket,
        organization,
        initial.clone(),
        &project_at(FIRST_WORKSPACE, FIRST_PROJECT)?,
    )
    .await?;
    let mut higher_attempt = initial.clone();
    higher_attempt.attempt = 2_u64.try_into()?;
    Ok((initial, higher_attempt))
}

pub fn archive_candidate() -> Result<CreateFleetSession> {
    let mut request = create()?;
    request.operation_id = ARCHIVE_OPERATION.parse()?;
    request.project = StartProject::Named {
        project_ref: project_at(SECOND_WORKSPACE, SECOND_PROJECT)?,
    };
    Ok(request)
}

pub async fn verify_denials_without_workspace_reads(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    withdrawn_attempt: CreateFleetSession,
    mut workspace_calls: impl AsyncFnMut() -> Result<u64>,
    mut session_rows: impl AsyncFnMut() -> Result<Vec<Vec<SqlValue>>>,
) -> Result<()> {
    let mut unexpected_reads = Vec::new();
    let mut outside = create()?;
    for (case, workspace) in [
        ("foreign", FOREIGN_WORKSPACE),
        ("unknown", UNKNOWN_WORKSPACE),
        ("unallowed", UNALLOWED_WORKSPACE),
    ] {
        outside.project = StartProject::Named {
            project_ref: project_at(workspace, FIRST_PROJECT)?,
        };
        let before_calls = workspace_calls().await?;
        let before_rows = session_rows().await?;
        require_machine_not_allowed(socket, organization, outside.clone()).await?;
        if workspace_calls().await? != before_calls {
            unexpected_reads.push(case);
        }
        assert!(
            session_rows().await? == before_rows,
            "{case} workspace changed a session row"
        );
    }
    let before_calls = workspace_calls().await?;
    let before_rows = session_rows().await?;
    require_machine_not_allowed(socket, organization, withdrawn_attempt).await?;
    if workspace_calls().await? != before_calls {
        unexpected_reads.push("withdrawn");
    }
    assert!(
        unexpected_reads.is_empty(),
        "unexpected owner-store reads: {unexpected_reads:?}"
    );
    assert!(
        session_rows().await? == before_rows,
        "withdrawal changed the attempt or its stored secret"
    );
    Ok(())
}

pub async fn verify_replays_without_allowance(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    equal_attempt: CreateFleetSession,
) -> Result<()> {
    assert_eq!(
        create_reply(socket, organization, equal_attempt.clone()).await?,
        CreateFleetSessionReply::Superseded {},
    );
    let mut different_contents = equal_attempt;
    different_contents.project = StartProject::Named {
        project_ref: project_at(SECOND_WORKSPACE, SECOND_PROJECT)?,
    };
    assert_eq!(
        create_reply(socket, organization, different_contents).await?,
        CreateFleetSessionReply::Conflict {},
    );
    Ok(())
}

pub async fn require_machine_not_allowed(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    request: CreateFleetSession,
) -> Result<()> {
    assert_eq!(
        create_reply(socket, organization, request).await?,
        CreateFleetSessionReply::Refused {
            reason: CreateSessionRefusal::MachineNotAllowed {}
        },
    );
    Ok(())
}

pub async fn send_create_for_review_race(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    request: CreateFleetSession,
) -> Result<CorrelationId> {
    send_call::<CreateFleetSessionRpc>(socket, organization, request).await
}
