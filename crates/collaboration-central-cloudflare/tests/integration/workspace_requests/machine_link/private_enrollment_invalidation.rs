//! A private sign-in commit invalidates only sockets for the ended enrollment.
use super::fixture::{
    ConnectedLink, Fixture, MACHINE_ID, ORGANIZATION, approve, grant, receive, send,
};
use central_link_protocol::{CorrelationId, DataFrame, MachineLinkCloseCode, TokenRenewalResult};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fleet_contracts::{MachineTokenDecision, MachineTokenRefusal};
use fleet_identity_protocol::EnrollmentId;
use futures_util::{FutureExt, SinkExt, StreamExt};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};

#[tokio::test]
async fn private_superseding_grant_closes_the_old_socket_before_a_replacement_dials() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = superseding_grant(&fixture).await;
    fixture.close().await?;
    result
}

async fn superseding_grant(fixture: &Fixture) -> Result<()> {
    let mut socket = ConnectedLink::open(fixture).await?.socket;
    supersede_and_require_old_close(fixture, &mut socket).await
}

pub(super) async fn supersede_and_require_old_close(
    fixture: &Fixture,
    old: &mut WebSocket,
) -> Result<()> {
    let replacement = approve_replacement(fixture).await?;
    if old.next().now_or_never().is_some() {
        return Err(
            "approving a replacement disturbed the original link before its first grant".into(),
        );
    }
    let decision = request_token(fixture, &replacement).await?;
    if !matches!(
        &decision,
        MachineTokenDecision::Grant { machine_id, enrollment_id, superseded: Some(ended), .. }
            if machine_id.as_str() == MACHINE_ID
                && enrollment_id == &replacement
                && ended.enrollment_id == fixture.enrollment
                && ended.client_id.as_str() == fixture.enrollment.as_str()
    ) {
        return Err(format!(
            "replacement grant must identify the ended enrollment and client: {decision:?}"
        )
        .into());
    }
    require_ending(fixture, "superseded", "machineRevoked").await?;
    require_private_inactive_close(old).await
}

#[tokio::test]
async fn private_idle_mint_closes_the_committed_ended_enrollment_without_a_link_request()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let result = idle_mint(&fixture).await;
    fixture.close().await?;
    result
}

async fn idle_mint(fixture: &Fixture) -> Result<()> {
    let mut socket = ConnectedLink::open(fixture).await?.socket;
    set_last_grant_at_idle_boundary(fixture).await?;
    require_idle_refusal(request_token(fixture, &fixture.enrollment).await?)?;
    require_ending(fixture, "idle", "machineExpired").await?;
    require_private_inactive_close(&mut socket).await
}

#[tokio::test]
async fn delayed_private_idle_invalidation_spares_a_replacement_on_the_same_machine() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = delayed_invalidation(&fixture).await;
    fixture.close().await?;
    result
}

async fn delayed_invalidation(fixture: &Fixture) -> Result<()> {
    let mut original = ConnectedLink::open(fixture).await?.socket;
    set_last_grant_at_idle_boundary(fixture).await?;
    let runtime = fixture.runtime();
    let pause = "beforePrivateEnrollmentInvalidation";
    if !runtime.control::<bool>("hold-central", &pause).await? {
        return Err("could not hold private invalidation".into());
    }
    let mut mint = std::pin::pin!(request_token(fixture, &fixture.enrollment));
    tokio::select! {
        held = runtime.control::<bool>("until-held-central", &pause) => {
            if !held? {
                return Err("private idle mint did not commit".into());
            }
        }
        answer = &mut mint => return Err(format!("private idle mint answered before the invalidation hold: {answer:?}").into()),
        frame = original.next() => return Err(format!("original socket produced a frame before invalidation: {frame:?}").into()),
    }
    let replacement = dial_before_dispatch(fixture, &mut original).await;
    if !runtime.control::<bool>("release-central", &pause).await? {
        return Err("could not release private invalidation".into());
    }
    require_idle_refusal(mint.await?)?;
    let (mut socket, bearer) = replacement?;
    require_renewed(&mut socket, bearer).await
}

async fn dial_before_dispatch(
    fixture: &Fixture,
    original: &mut WebSocket,
) -> Result<(WebSocket, String)> {
    require_ending(fixture, "idle", "machineExpired").await?;
    if original.next().now_or_never().is_some() {
        return Err(
            "idle commit published a frame before the private invalidation was released".into(),
        );
    }
    let replacement = approve_replacement(fixture).await?;
    let MachineTokenDecision::Grant {
        machine_id,
        enrollment_id,
        grant_expires_at,
        superseded: None,
        ..
    } = request_token(fixture, &replacement).await?
    else {
        return Err("the replacement grant must succeed after the original idle commit".into());
    };
    if enrollment_id != replacement || machine_id.as_str() != MACHINE_ID {
        return Err("replacement grant names the wrong enrollment or machine".into());
    }
    let bearer = signed::machine_token(
        ORGANIZATION,
        MACHINE_ID,
        replacement.as_str(),
        signed::NOW_SECONDS,
        grant_expires_at.0,
    )?;
    let mut socket = fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(&bearer)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    require_welcome(&mut socket).await?;
    Ok((socket, bearer))
}

async fn require_welcome(socket: &mut WebSocket) -> Result<()> {
    let frame = receive(socket).await?;
    if !matches!(frame, DataFrame::Welcome { .. }) {
        return Err(format!("expected Welcome, got {frame:?}").into());
    }
    Ok(())
}

async fn require_renewed(socket: &mut WebSocket, bearer: String) -> Result<()> {
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000cd12".parse()?;
    let renewal = DataFrame::RenewMachineToken {
        correlation_id: correlation.clone(),
        token: bearer.try_into()?,
    };
    send(socket, &renewal).await?;
    let response = receive(socket).await?;
    if !matches!(response,
        DataFrame::MachineTokenRenewal {
            ref correlation_id,
            result: TokenRenewalResult::Renewed { .. },
        } if correlation_id == &correlation
    ) {
        return Err(format!(
            "delayed invalidation disturbed the replacement's real renewal: {response:?}"
        )
        .into());
    }
    Ok(())
}

async fn require_private_inactive_close(socket: &mut WebSocket) -> Result<()> {
    let frame = match socket.next().now_or_never() {
        Some(frame) => frame,
        None => {
            // A literal heartbeat bypasses authority restoration on both hosts.
            // If the invalidation is missing, the old link will answer pong.
            let _ = socket.send(Message::Text("ping".into())).await;
            socket.next().await
        }
    };
    if !matches!(frame, Some(Ok(Message::Close { code, .. }))
        if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Inactive))
    {
        return Err(format!(
            "the private commit must send Inactive to the original link: {frame:?}"
        )
        .into());
    }
    Ok(())
}

fn require_idle_refusal(decision: MachineTokenDecision) -> Result<()> {
    if decision
        != (MachineTokenDecision::Refuse {
            reason: MachineTokenRefusal::IdleExpired {},
        })
    {
        return Err(format!("private idle mint must report IdleExpired: {decision:?}").into());
    }
    Ok(())
}

pub(super) async fn set_last_grant_at_idle_boundary(fixture: &Fixture) -> Result<()> {
    let store = fixture.runtime();
    let idle_at = chrono::DateTime::from_timestamp(
        signed::NOW_SECONDS - signed::MACHINE_IDLE_LIMIT_SECONDS,
        0,
    )
    .ok_or("invalid idle fixture clock")?
    .to_rfc3339();
    let outcome = store
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "UPDATE login_machine_approvals SET last_grant_at=? WHERE approval_id=?",
                    args: vec![
                        SqlValue::Text(idle_at),
                        SqlValue::Text(fixture.enrollment.as_str().to_owned()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await?;
    if !matches!(outcome, BatchOutcome::Applied { .. }) {
        return Err(format!("could not set the idle fixture deadline: {outcome:?}").into());
    }
    Ok(())
}

async fn require_ending(fixture: &Fixture, reason: &str, audit_kind: &str) -> Result<()> {
    let store = fixture.runtime();
    let rows = store.read(Statement {
        sql: "SELECT state,end_reason,(SELECT COUNT(*) FROM signin_change_records WHERE kind=? AND target_id=approval_id) FROM login_machine_approvals WHERE approval_id=?",
        args: vec![SqlValue::Text(audit_kind.to_owned()), SqlValue::Text(fixture.enrollment.as_str().to_owned())],
    }).await?.rows;
    if rows
        != [vec![
            SqlValue::Text("ended".into()),
            SqlValue::Text(reason.into()),
            SqlValue::Integer(1_i64.try_into()?),
        ]]
    {
        return Err(format!(
            "private decision must commit one {reason} ending and its {audit_kind} audit: {rows:?}"
        )
        .into());
    }
    Ok(())
}

pub(super) async fn approve_replacement(fixture: &Fixture) -> Result<EnrollmentId> {
    approve(
        &fixture.host,
        &fixture.client,
        "019f0000-0000-7000-8000-00000000cd11",
        signed::NOW_SECONDS,
    )
    .await
}

pub(super) async fn request_token(
    fixture: &Fixture,
    enrollment: &EnrollmentId,
) -> Result<MachineTokenDecision> {
    grant(&fixture.host, &fixture.client, enrollment).await
}
