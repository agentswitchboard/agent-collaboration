use super::fixture::{
    ConnectedLink, Fixture, MACHINE_ID, ORGANIZATION, receive, send, session_caller,
};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_link_protocol::{
    CorrelationId, DataFrame, LinkRequest, MachineLinkCloseCode, ReplyPayload, RequestPayload,
    RequestScope, RequestStatusQuery, TokenRenewalRefusal, TokenRenewalResult,
};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::ConformanceResult as Result;
use futures_util::StreamExt;
use reqwest_websocket::{Message, RequestBuilderExt};
use workspace_model::{RefusalReason, WorkspaceOperation, WorkspaceRequest, WorkspaceResult};

#[tokio::test]
async fn a_request_reply_agrees_with_each_authority_close_cause() -> Result<()> {
    for state in [
        EnrollmentAtApply::StillEnrolled,
        EnrollmentAtApply::OwnerRevoked,
        EnrollmentAtApply::EndedForIdle,
    ] {
        let fixture = Fixture::new().await?;
        let result = check_request_refusal(&fixture, state).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

async fn check_request_refusal(fixture: &Fixture, state: EnrollmentAtApply) -> Result<()> {
    let mut socket = ConnectedLink::open(fixture).await?.socket;
    let central = fixture.runtime();
    super::renewal::advance(fixture, 601).await?;
    if let Some(reason) = state.stored_end_reason() {
        // Stage the committed ending without its socket observer, to exercise the live check.
        let batch = stored_ending_batch(MACHINE_ID, fixture.enrollment.as_str(), reason)?;
        if !matches!(
            central.run_batch(batch).await?,
            BatchOutcome::Applied { .. }
        ) {
            return Err("failed to stage the stored ending".into());
        }
    }
    let expected_reply = match state {
        EnrollmentAtApply::StillEnrolled => WorkspaceResult::CentralUnavailable,
        EnrollmentAtApply::OwnerRevoked | EnrollmentAtApply::EndedForIdle => {
            WorkspaceResult::Refused(RefusalReason::MachineRevoked)
        }
    };
    let expected_close = state.expected_close();
    let correlation: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-00000000aa01".parse()?;
    let request = LinkRequest::new(
        correlation.clone(),
        session_caller()?,
        RequestPayload::Workspace(WorkspaceRequest {
            request_id: Some("019f0000-0000-7000-8000-00000000aa02".parse()?),
            workspace_id: None,
            operation: WorkspaceOperation::CreateWorkspace {
                name: "Lapsed link must not write".to_owned().try_into()?,
            },
        }),
    )?;
    send(
        &mut socket,
        &DataFrame::Request {
            request: Box::new(request),
        },
    )
    .await?;
    let reply = receive(&mut socket).await?;
    if !matches!(&reply, DataFrame::Reply { correlation_id, result }
        if correlation_id == &correlation
            && matches!(result.as_ref(), ReplyPayload::Workspace(reply) if reply.result == expected_reply))
    {
        return Err(
            format!("{expected_close:?} requires {expected_reply:?}, got {reply:?}").into(),
        );
    }
    let closed = socket.next().await;
    if !matches!(closed, Some(Ok(Message::Close { code, .. }))
        if MachineLinkCloseCode::from_code(code.into()) == Some(expected_close))
    {
        return Err(format!("expected {expected_close:?} after reply, got {closed:?}").into());
    }
    let rows = central.read(Statement {
        sql: "SELECT count(*) FROM directory_workspaces WHERE name='Lapsed link must not write'",
        args: vec![],
    }).await?.rows;
    if rows != [vec![SqlValue::Integer(0.try_into()?)]] {
        return Err("refused request created a workspace".into());
    }
    Ok(())
}

#[tokio::test]
async fn inconsistent_generation_refuses_the_real_link_until_repaired() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = check_generation_on_link(&fixture).await;
    fixture.close().await?;
    result
}

async fn check_generation_on_link(fixture: &Fixture) -> Result<()> {
    let mut socket = ConnectedLink::open(fixture).await?.socket;
    let healthy = login_rows(fixture).await?;
    change_generation(fixture, 1, 2).await?;
    let inconsistent = login_rows(fixture).await?;

    let dial = || {
        fixture
            .client
            .get(fixture.link_url(ORGANIZATION))
            .bearer_auth(&fixture.token)
            .upgrade()
            .send()
    };
    let refused = dial().await?;
    if refused.status().as_u16() != 503 {
        return Err(format!(
            "inconsistent generation must refuse connect with HTTP 503, got {}",
            refused.status()
        )
        .into());
    }
    ensure_login_rows(fixture, &inconsistent, "connect refusal").await?;

    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000aa03".parse()?;
    send(
        &mut socket,
        &DataFrame::RenewMachineToken {
            correlation_id: correlation.clone(),
            token: fixture.token.clone().try_into()?,
        },
    )
    .await?;
    let reply = receive(&mut socket).await?;
    if reply
        != (DataFrame::MachineTokenRenewal {
            correlation_id: correlation.clone(),
            result: TokenRenewalResult::Refused {
                reason: TokenRenewalRefusal::CentralUnavailable,
            },
        })
    {
        return Err(format!(
            "inconsistent generation must refuse renewal as unavailable: {reply:?}"
        )
        .into());
    }
    ensure_login_rows(fixture, &inconsistent, "renewal refusal").await?;

    send(
        &mut socket,
        &DataFrame::RequestStatus {
            correlation_id: correlation,
            caller: session_caller()?,
            query: RequestStatusQuery {
                request_id: "019f0000-0000-7000-8000-00000000aa04".parse()?,
                scope: RequestScope::Organization,
            },
        },
    )
    .await?;
    let closed = socket.next().await;
    if !matches!(closed, Some(Ok(Message::Close { code, .. })) if u16::from(code) == 1011) {
        return Err(format!("guarded restore must close with 1011, got {closed:?}").into());
    }
    ensure_login_rows(fixture, &inconsistent, "guarded restore refusal").await?;

    change_generation(fixture, 2, 1).await?;
    let repaired = dial().await?;
    if repaired.status().as_u16() != 101 {
        return Err(format!("repair must allow a 101 upgrade, got {}", repaired.status()).into());
    }
    let mut repaired = repaired.into_websocket().await?;
    if !matches!(receive(&mut repaired).await?, DataFrame::Welcome { .. }) {
        return Err("repair must restore link admission".into());
    }
    ensure_login_rows(fixture, &healthy, "repair and connect").await
}

async fn change_generation(fixture: &Fixture, expected: i64, replacement: i64) -> Result<()> {
    let outcome = fixture.runtime().run_batch(StatementBatch {
        statements: vec![GuardedStatement {
            statement: Statement {
                sql: "UPDATE login_machines SET enrollment_generation=? WHERE machine_id=? AND state='enrolled' AND enrollment_generation=?",
                args: vec![
                    SqlValue::Integer(replacement.try_into()?),
                    SqlValue::Text(MACHINE_ID.to_owned()),
                    SqlValue::Integer(expected.try_into()?),
                ],
            },
            expect: RowExpectation::Exactly(1),
        }],
        alarm: AlarmUpdate::Unchanged,
    }).await?;
    if !matches!(outcome, BatchOutcome::Applied { .. }) {
        return Err(format!("fixture generation update did not apply: {outcome:?}").into());
    }
    Ok(())
}

async fn login_rows(fixture: &Fixture) -> Result<Vec<Vec<Vec<SqlValue>>>> {
    let central = fixture.runtime();
    let mut rows = Vec::new();
    for sql in [
        "SELECT * FROM login_machines ORDER BY machine_id",
        "SELECT * FROM login_machine_approvals ORDER BY approval_id",
        "SELECT * FROM login_machine_allowances ORDER BY allowance_id",
        "SELECT * FROM signin_change_records ORDER BY id",
    ] {
        rows.push(central.read(Statement { sql, args: vec![] }).await?.rows);
    }
    Ok(rows)
}

async fn ensure_login_rows(
    fixture: &Fixture,
    expected: &[Vec<Vec<SqlValue>>],
    operation: &str,
) -> Result<()> {
    if login_rows(fixture).await? != expected {
        return Err(format!("{operation} changed login rows").into());
    }
    Ok(())
}
