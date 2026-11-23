//! Real socket controls for the shared Central link boundary cases.

use central_link_protocol::{
    DataFrame, FrameDirection, HandledMacCalls, LINK_MAX_TEXT_BYTES, LinkFrame, MacCapabilities,
    MachineLinkCloseCode, encode_text,
};
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, WebSocket};

use crate::{ConformanceResult as Result, approval_link_scenarios::receive_frame};

pub async fn expect_pong(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text("ping".into())).await?;
    assert_eq!(receive_frame(socket).await?, LinkFrame::Pong);
    Ok(())
}

pub async fn check_expired_heartbeat(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text("ping".into())).await?;
    expect_close(socket, MachineLinkCloseCode::Expired.code()).await
}

pub async fn check_input_sizes(socket: &mut WebSocket, other: &mut WebSocket) -> Result<()> {
    let announcement = LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
        handled_calls: HandledMacCalls::try_from(Vec::new())?,
    }));
    let mut text = encode_text(&announcement, FrameDirection::ToCentral)?;
    let padding = LINK_MAX_TEXT_BYTES
        .checked_sub(text.len())
        .ok_or("fixture does not fit")?;
    text.extend(std::iter::repeat_n(' ', padding));
    assert_eq!(text.len(), 1_048_576);
    socket.send(Message::Text(text.clone())).await?;
    expect_pong(socket).await?;
    text.push(' ');
    assert_eq!(text.len(), 1_048_577);
    socket.send(Message::Text(text)).await?;
    expect_close(socket, 1009).await?;
    expect_pong(other).await
}

async fn expect_close(socket: &mut WebSocket, expected: u16) -> Result<()> {
    let frame = socket.next().await.transpose()?;
    assert!(
        matches!(frame, Some(Message::Close { code, .. }) if u16::from(code) == expected),
        "expected close {expected}, got {frame:?}"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub enum WelcomeInterleaving {
    BeforeWrite,
    AfterWrite,
}

pub async fn register_mail_session(
    socket: &mut WebSocket,
    session: &workspace_ids::SessionRef,
    key: u32,
) -> Result<()> {
    let reply = mail_call(
        socket,
        session,
        key,
        serde_json::json!({"kind":"setNoticeCadence","cadence":"m15"}),
    )
    .await?;
    assert!(matches!(
        reply,
        workspace_model::central_mail::CentralMailResult::Done(
            workspace_model::central_mail::CentralMailAnswer::NoticeCadenceSet { .. }
        )
    ));
    Ok(())
}

pub async fn queue_direct_mail(
    socket: &mut WebSocket,
    sender: &workspace_ids::SessionRef,
    recipient: &workspace_ids::SessionRef,
    key: u32,
    sent_at: chrono::DateTime<chrono::Utc>,
) -> Result<workspace_ids::MessageId> {
    let reply = mail_call(
        socket,
        sender,
        key,
        serde_json::json!({
            "kind":"sendMessage", "to":recipient, "body":"offer during Welcome",
            "delivery":"queue", "replyTo":null, "sentAt":sent_at,
        }),
    )
    .await?;
    let workspace_model::central_mail::CentralMailResult::Done(
        workspace_model::central_mail::CentralMailAnswer::Queued { message_id },
    ) = reply
    else {
        return Err(format!("direct mail was not queued: {reply:?}").into());
    };
    Ok(message_id)
}

async fn mail_call(
    socket: &mut WebSocket,
    sender: &workspace_ids::SessionRef,
    key: u32,
    operation: serde_json::Value,
) -> Result<workspace_model::central_mail::CentralMailResult> {
    use central_link_protocol::{CorrelationId, LinkRequest, ReplyPayload, RequestPayload};
    let correlation: CorrelationId = format!("019f0000-0000-7000-8000-{key:012x}").parse()?;
    let request = LinkRequest::new(
        correlation.clone(),
        workspace_model::Caller::Session(sender.clone()),
        RequestPayload::Mail(serde_json::from_value(serde_json::json!({
            "requestId":format!("019f0000-0000-7000-8000-{:012x}", u64::from(key)+100),
            "operation":operation,
        }))?),
    )?;
    crate::approval_link_scenarios::send_frame(
        socket,
        LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::Reply {
        correlation_id,
        result,
    }) = receive_frame(socket).await?
    else {
        return Err("expected the sender's mail reply".into());
    };
    assert_eq!(correlation_id, correlation);
    let ReplyPayload::Mail(reply) = *result else {
        return Err("expected a Central mail reply".into());
    };
    Ok(reply)
}

pub async fn assert_welcome(socket: &mut WebSocket) -> Result<()> {
    let frame = receive_frame(socket).await?;
    assert!(
        matches!(frame, LinkFrame::Data(DataFrame::Welcome { .. })),
        "first frame was {frame:?}"
    );
    Ok(())
}

pub async fn offered_message(socket: &mut WebSocket) -> Result<workspace_ids::MessageId> {
    let frame = receive_frame(socket).await?;
    let LinkFrame::Data(DataFrame::Deliver { message, .. }) = frame else {
        return Err(format!("expected direct delivery, got {frame:?}").into());
    };
    Ok(message.message_id().clone())
}

pub async fn assert_replacement_backlog(
    socket: &mut WebSocket,
    message: workspace_ids::MessageId,
    sentinel: workspace_ids::MessageId,
) -> Result<()> {
    assert_welcome(socket).await?;
    let observed = std::collections::BTreeSet::from([
        offered_message(socket).await?,
        offered_message(socket).await?,
    ]);
    assert_eq!(
        observed,
        std::collections::BTreeSet::from([message, sentinel])
    );
    expect_pong(socket).await
}
