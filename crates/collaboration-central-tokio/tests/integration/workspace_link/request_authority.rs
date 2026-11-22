use super::fixture::{Fixture, INSTALLATION, receive, send};
use super::link_support::{caller, create_workspace};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_link_protocol::{
    DataFrame, LinkFrame, LinkRequest, MachineLinkCloseCode, ReplyPayload,
};
use central_storage::{BatchOutcome, SqlValue, Statement};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use futures_util::StreamExt;
use reqwest_websocket::Message;
use std::time::Duration;
use workspace_model::{RefusalReason, WorkspaceResult};

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
    let mut socket = fixture.connect().await?;
    if !matches!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("expected an admitted link".into());
    }
    send(&mut socket, &LinkFrame::Ping).await?;
    if receive(&mut socket).await? != LinkFrame::Pong {
        return Err("link did not become ready".into());
    }
    let central = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let store = central.runtime().statement_host();
    fixture.clock.advance(Duration::from_secs(601))?;
    if let Some(reason) = state.stored_end_reason() {
        // Stage the committed ending without its socket observer, to exercise the live check.
        let _turn = central.runtime().take_central_turn().await;
        let batch = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
        if !matches!(store.run_batch(batch).await?, BatchOutcome::Applied { .. }) {
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
        caller()?,
        create_workspace(
            "019f0000-0000-7000-8000-00000000aa02",
            "Lapsed link must not write",
        )?,
    )?;
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    let reply = receive(&mut socket).await?;
    if !matches!(&reply, LinkFrame::Data(DataFrame::Reply { correlation_id, result })
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
    let rows = store.read(Statement {
        sql: "SELECT count(*) FROM directory_workspaces WHERE name='Lapsed link must not write'",
        args: vec![],
    }).await?.rows;
    if rows != [vec![SqlValue::Integer(0.try_into()?)]] {
        return Err("refused request created a workspace".into());
    }
    Ok(())
}
