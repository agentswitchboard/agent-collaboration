use super::fixture::{Fixture, INSTALLATION, receive, send, start_fixture};
use super::link_support::{caller, create_workspace};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_link_protocol::{
    DataFrame, LinkFrame, LinkRequest, MachineLinkCloseCode, ReplyPayload,
};
use central_storage::{BatchOutcome, SqlValue, Statement};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fleet_runtime::PausePoint;
use futures_util::{FutureExt, StreamExt};
use reqwest_websocket::Message;
use std::time::Duration;
use workspace_model::{RefusalReason, WorkspaceResult};

#[tokio::test]
async fn a_request_reply_agrees_with_each_authority_close_cause() -> Result<()> {
    check_request_authority(AuthorityCheck::FrameEntry).await
}

#[tokio::test]
async fn a_workspace_request_uses_the_authority_cause_found_after_frame_entry() -> Result<()> {
    check_request_authority(AuthorityCheck::Applying).await
}

#[derive(Clone, Copy)]
enum AuthorityCheck {
    FrameEntry,
    Applying,
}

async fn check_request_authority(check: AuthorityCheck) -> Result<()> {
    for state in [
        EnrollmentAtApply::StillEnrolled,
        EnrollmentAtApply::OwnerRevoked,
        EnrollmentAtApply::EndedForIdle,
    ] {
        let fixture = start_fixture().await?;
        let result = check_request_refusal(&fixture, state, check).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

async fn check_request_refusal(
    fixture: &Fixture,
    state: EnrollmentAtApply,
    check: AuthorityCheck,
) -> Result<()> {
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
    let lapse_authority = || async {
        fixture.clock.advance(Duration::from_secs(601))?;
        if let Some(reason) = state.stored_end_reason() {
            // Leave the old socket registered so the active authority check supplies the cause.
            let _turn = central.runtime().take_central_turn().await;
            let batch = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
            if !matches!(store.run_batch(batch).await?, BatchOutcome::Applied { .. }) {
                return Err("failed to stage the stored ending".into());
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    let expected_reply = match state {
        EnrollmentAtApply::StillEnrolled => WorkspaceResult::CentralUnavailable,
        EnrollmentAtApply::OwnerRevoked | EnrollmentAtApply::EndedForIdle => {
            WorkspaceResult::Refused(RefusalReason::MachineRevoked)
        }
    };
    let expected_close = state.expected_close();
    let correlation: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-00000000aa01".parse()?;
    const REQUEST_ID: &str = "019f0000-0000-7000-8000-00000000aa02";
    const WORKSPACE_NAME: &str = "Lapsed link must not write";
    let request = LinkRequest::new(
        correlation.clone(),
        caller()?,
        create_workspace(REQUEST_ID, WORKSPACE_NAME)?,
    )?;
    let message = LinkFrame::Data(DataFrame::Request {
        request: Box::new(request),
    });
    match check {
        AuthorityCheck::FrameEntry => {
            lapse_authority().await?;
            send(&mut socket, &message).await?;
        }
        AuthorityCheck::Applying => {
            let mut hold = central
                .runtime()
                .pause_points()
                .hold_central(PausePoint::BeforeWorkspaceRequestApplying);
            send(&mut socket, &message).await?;
            tokio::select! {
                () = hold.until_held() => {}
                early = socket.next() => return Err(format!("request returned before the applying barrier: {early:?}").into()),
            }
            lapse_authority().await?;
            if socket.next().now_or_never().is_some() {
                return Err("request emitted a frame before applying authority was checked".into());
            }
            hold.release();
        }
    }
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
        sql: "SELECT (SELECT count(*) FROM directory_workspaces WHERE name=?),(SELECT count(*) FROM runtime_request_log WHERE request_id=?)",
        args: vec![SqlValue::Text(WORKSPACE_NAME.to_owned()), SqlValue::Text(REQUEST_ID.to_owned())],
    }).await?.rows;
    let zero = SqlValue::Integer(0.try_into()?);
    if rows != [vec![zero.clone(), zero]] {
        return Err(format!(
            "refused request created a workspace or retained a request answer: {rows:?}"
        )
        .into());
    }
    Ok(())
}
