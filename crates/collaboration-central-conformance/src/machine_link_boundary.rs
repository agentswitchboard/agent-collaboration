//! Real socket controls for the shared Central link boundary cases.

use central_link_protocol::{
    CorrelationId, DataFrame, FrameDirection, LINK_MAX_TEXT_BYTES, LinkFrame, LinkRequest,
    MachineLinkCloseCode, RequestPayload, encode_text,
};
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, WebSocket};

use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{receive_expected_text, receive_frame},
};

pub async fn expect_pong(socket: &mut WebSocket) -> Result<()> {
    send_pong_checkpoint(socket).await?;
    // A following RFC control response bounds the application observation.
    send_transport_checkpoint(socket).await?;
    read_pong_checkpoint(socket).await?;
    read_transport_checkpoint(socket).await
}

/// Check socket progress while the test holds the organization or store lock.
pub async fn expect_transport_pong(socket: &mut WebSocket) -> Result<()> {
    send_transport_checkpoint(socket).await?;
    read_transport_checkpoint(socket).await
}

const TRANSPORT_CHECKPOINT: &[u8] = b"central-link-fixture";

async fn send_transport_checkpoint(socket: &mut WebSocket) -> Result<()> {
    socket
        .send(Message::Ping(TRANSPORT_CHECKPOINT.into()))
        .await?;
    Ok(())
}

async fn read_transport_checkpoint(socket: &mut WebSocket) -> Result<()> {
    let observed = socket.next().await.transpose()?;
    if !matches!(&observed, Some(Message::Pong(marker)) if marker.as_ref() == TRANSPORT_CHECKPOINT)
    {
        return Err(format!("expected the transport Pong checkpoint, got {observed:?}").into());
    }
    Ok(())
}

pub async fn send_pong_checkpoint(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text("ping".into())).await?;
    Ok(())
}

pub async fn read_pong_checkpoint(socket: &mut WebSocket) -> Result<()> {
    assert_eq!(
        receive_expected_text(socket, "application Pong").await?,
        LinkFrame::Pong
    );
    Ok(())
}

pub async fn assert_empty_welcome(socket: &mut WebSocket) -> Result<()> {
    send_pong_checkpoint(socket).await?;
    assert_welcome(socket).await?;
    read_pong_checkpoint(socket).await
}

pub async fn check_expired_heartbeat(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text("ping".into())).await?;
    expect_close(socket, MachineLinkCloseCode::Expired.code()).await
}

pub fn text_at_input_limit(machine: &str) -> Result<(CorrelationId, String)> {
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ccb0".parse()?;
    let request = LinkRequest::new(
        correlation.clone(),
        workspace_model::Caller::Session(workspace_ids::SessionRef {
            machine_id: machine.parse()?,
            session_id: "frame-boundary-é".parse()?,
        }),
        RequestPayload::Workspace(workspace_model::WorkspaceRequest {
            request_id: None,
            workspace_id: None,
            operation: workspace_model::WorkspaceOperation::Inbox {
                after: None,
                limit: 1u16.try_into()?,
            },
        }),
    )?;
    let frame = LinkFrame::Data(DataFrame::Request {
        request: Box::new(request),
    });
    let mut text = encode_text(&frame, FrameDirection::ToCentral)?;
    let padding = LINK_MAX_TEXT_BYTES
        .checked_sub(text.len())
        .ok_or("fixture does not fit")?;
    text.extend(std::iter::repeat_n(' ', padding));
    if text.len() != LINK_MAX_TEXT_BYTES || text.chars().count() == text.len() {
        return Err("expected a multibyte frame at the exact ingress limit".into());
    }
    Ok((correlation, text))
}

pub async fn check_input_sizes(
    socket: &mut WebSocket,
    other: &mut WebSocket,
    machine: &str,
) -> Result<()> {
    let (correlation, mut text) = text_at_input_limit(machine)?;
    socket.send(Message::Text(text.clone())).await?;
    assert!(
        matches!(receive_frame(socket).await?, LinkFrame::Data(DataFrame::Reply {
        correlation_id, ..
    }) if correlation_id == correlation)
    );
    text.push(' ');
    assert_eq!(text.len(), 1_048_577);
    assert!(text.chars().count() <= LINK_MAX_TEXT_BYTES);
    // A parser can close upon the frame header before the sender has
    // finished its payload. Observe the close independently of that write.
    let (mut write, mut read) = (&mut *socket).split();
    let frame = match futures_util::future::select(
        std::pin::pin!(read.next()),
        std::pin::pin!(write.send(Message::Text(text))),
    )
    .await
    {
        futures_util::future::Either::Left((frame, _)) => frame,
        futures_util::future::Either::Right((_, pending_close)) => pending_close.await,
    };
    assert_close(frame.transpose()?, 1009);
    expect_pong(other).await
}

pub async fn expect_close(socket: &mut WebSocket, expected: u16) -> Result<()> {
    assert_close(socket.next().await.transpose()?, expected);
    Ok(())
}

fn assert_close(frame: Option<Message>, expected: u16) {
    assert!(
        matches!(frame, Some(Message::Close { code, .. }) if u16::from(code) == expected),
        "expected close {expected}, got {frame:?}"
    );
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

pub fn mail_fixture_correlation(key: u32) -> Result<CorrelationId> {
    Ok(format!("019f0000-0000-7000-8000-{key:012x}").parse()?)
}

async fn mail_call(
    socket: &mut WebSocket,
    sender: &workspace_ids::SessionRef,
    key: u32,
    operation: serde_json::Value,
) -> Result<workspace_model::central_mail::CentralMailResult> {
    use central_link_protocol::ReplyPayload;
    let correlation = mail_fixture_correlation(key)?;
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
    let frame = receive_expected_text(socket, "Welcome").await?;
    assert!(
        matches!(frame, LinkFrame::Data(DataFrame::Welcome { .. })),
        "first frame was {frame:?}"
    );
    Ok(())
}

pub async fn offered_message(socket: &mut WebSocket) -> Result<workspace_ids::MessageId> {
    let frame = receive_expected_text(socket, "direct delivery").await?;
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
    send_pong_checkpoint(socket).await?;
    assert_welcome(socket).await?;
    let observed = std::collections::BTreeSet::from([
        offered_message(socket).await?,
        offered_message(socket).await?,
    ]);
    assert_eq!(
        observed,
        std::collections::BTreeSet::from([message, sentinel])
    );
    read_pong_checkpoint(socket).await
}

pub async fn queue_backlog(
    source: &mut WebSocket,
    sender: &workspace_ids::SessionRef,
    recipient: &workspace_ids::SessionRef,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<std::collections::BTreeSet<workspace_ids::MessageId>> {
    // The durable welcome page holds 250 records.
    let mut queued = std::collections::BTreeSet::new();
    for index in 0..257 {
        queued.insert(queue_direct_mail(source, sender, recipient, 0x10000 + index, now).await?);
    }
    assert_eq!(queued.len(), 257);
    Ok(queued)
}

pub enum ReceiptCheckpoint {
    Arm,
    Wait,
}

pub async fn drain_bounded_backlog(
    socket: &mut WebSocket,
    mut queued: std::collections::BTreeSet<workspace_ids::MessageId>,
    now: chrono::DateTime<chrono::Utc>,
    on_saturated: impl AsyncFnOnce() -> Result<()>,
    mut checkpoint: impl AsyncFnMut(&CorrelationId, ReceiptCheckpoint) -> Result<()>,
) -> Result<()> {
    use central_link_protocol::SOCKET_DIRECT_MAIL_LIMIT;
    send_pong_checkpoint(socket).await?;
    assert_welcome(socket).await?;
    let mut on_saturated = Some(on_saturated);
    let mut next_correlation = 0x20000_u32;
    while !queued.is_empty() {
        let mut receipt_ids = Vec::new();
        for _ in 0..queued.len().min(SOCKET_DIRECT_MAIL_LIMIT) {
            let message_id = offered_message(socket).await?;
            assert!(
                queued.remove(&message_id),
                "duplicate or unknown mail offer {message_id}"
            );
            receipt_ids.push(message_id);
        }
        // The Pong follows the Welcome walk or the last completed refill.
        read_pong_checkpoint(socket).await?;
        if let Some(on_saturated) = on_saturated.take() {
            on_saturated().await?;
        }
        let correlation_id: CorrelationId =
            format!("019f0000-0000-7000-8000-{next_correlation:012x}").parse()?;
        next_correlation += 1;
        checkpoint(&correlation_id, ReceiptCheckpoint::Arm).await?;
        acknowledge_delivered_mail(socket, correlation_id.clone(), receipt_ids, now).await?;
        checkpoint(&correlation_id, ReceiptCheckpoint::Wait).await?;
        send_pong_checkpoint(socket).await?;
    }
    read_pong_checkpoint(socket).await
}

pub async fn acknowledge_delivered_mail(
    socket: &mut WebSocket,
    correlation_id: CorrelationId,
    message_ids: Vec<workspace_ids::MessageId>,
    arrived_at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    use central_link_protocol::{Receipt, ReceiptBatch, ReceiptKind, ReceiptResult};
    let count = message_ids.len();
    let receipts = message_ids
        .into_iter()
        .map(|message_id| Receipt {
            message_id,
            kind: ReceiptKind::Delivered { arrived_at },
        })
        .collect::<Vec<_>>();
    crate::approval_link_scenarios::send_frame(
        socket,
        LinkFrame::Data(DataFrame::Receipts {
            correlation_id: correlation_id.clone(),
            receipts: ReceiptBatch::try_from(receipts)?,
        }),
    )
    .await?;
    let expected = LinkFrame::Data(DataFrame::ReceiptResults {
        correlation_id,
        results: vec![ReceiptResult::Recorded; count],
    });
    let actual = receive_frame(socket).await?;
    if actual != expected {
        return Err(format!("expected recorded delivery receipts, got {actual:?}").into());
    }
    Ok(())
}
