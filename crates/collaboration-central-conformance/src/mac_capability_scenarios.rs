//! Real socket witnesses for the Central capability gate.

use super::{
    ApprovalLinkFixture, answer_ending, heartbeat, held_start, next_ending, observe_received,
    raise, receive_frame, register_run, require_owed, require_receipt, send_frame,
};
use crate::ConformanceResult as Result;
use approval_request_protocol::{DeliverEndingReply, RegisterRunReply};
use central_link_protocol::{
    DataFrame, HandledMacCalls, LinkFrame, LinkRequest, MacCallKind, MacCapabilities, Receipt,
    ReceiptBatch, ReceiptKind, ReceiptResult, ReplyPayload, RequestPayload,
};
use futures_util::StreamExt;
use reqwest_websocket::{CloseCode, Message, WebSocket};
use serde_json::json;
use workspace_ids::{MachineId, SessionRef};
use workspace_model::{
    Caller,
    central_mail::{CentralMailAnswer, CentralMailResult},
};

pub async fn announce_mac_calls(socket: &mut WebSocket, kinds: Vec<MacCallKind>) -> Result<()> {
    let handled_calls = HandledMacCalls::try_from(kinds)?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
            handled_calls,
        })),
    )
    .await
}

pub async fn require_duplicate_capabilities_close(socket: &mut WebSocket, code: u16) -> Result<()> {
    announce_mac_calls(socket, vec![]).await?;
    match socket.next().await {
        Some(Ok(Message::Close { code: observed, .. })) if u16::from(observed) == code => Ok(()),
        response => {
            Err(format!("expected duplicate announcement close {code}, got {response:?}").into())
        }
    }
}

#[derive(Clone, Copy)]
pub enum HolderRecoveryOrder {
    RunBeforeCapabilities,
    CapabilitiesBeforeRun,
}

pub async fn capability_ending_recovery(
    fixture: &impl ApprovalLinkFixture,
    order: HolderRecoveryOrder,
    duplicate_close: u16,
) -> Result<()> {
    let target = fixture.target()?;
    let mut seed = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut seed, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let approval = held_start(&target.router_run, 0)?;
    let receipt =
        require_receipt(raise(&mut seed, &target.organization_id, approval.clone()).await?)?;
    seed.close(CloseCode::Normal, None).await?;
    let deadline = receipt.window.deadline().timestamp();
    let mark = deadline + (-deadline).rem_euclid(300);
    fixture
        .advance(u64::try_from(mark - fixture.now_seconds())?)
        .await?;
    fixture.observe_due_alarm().await?;
    require_owed(fixture, &approval).await?;

    let mut empty = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut empty, vec![]).await?;
    assert_eq!(
        register_run(&mut empty, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    exchange_direct_mail(fixture, &mut empty, &target.machine_id, 1).await?;
    require_owed(fixture, &approval).await?;
    empty.close(CloseCode::Normal, None).await?;

    let mut socket = fixture.connect_without_capabilities().await?;
    match order {
        HolderRecoveryOrder::RunBeforeCapabilities => {
            assert_eq!(
                register_run(&mut socket, &target.organization_id, &target.router_run).await?,
                RegisterRunReply::Registered {}
            );
            exchange_direct_mail(fixture, &mut socket, &target.machine_id, 2).await?;
            require_owed(fixture, &approval).await?;
            announce_mac_calls(&mut socket, vec![MacCallKind::DeliverEnding]).await?;
        }
        HolderRecoveryOrder::CapabilitiesBeforeRun => {
            exchange_direct_mail(fixture, &mut socket, &target.machine_id, 2).await?;
            announce_mac_calls(&mut socket, vec![MacCallKind::DeliverEnding]).await?;
            exchange_direct_mail(fixture, &mut socket, &target.machine_id, 3).await?;
            require_owed(fixture, &approval).await?;
            assert_eq!(
                register_run(&mut socket, &target.organization_id, &target.router_run).await?,
                RegisterRunReply::Registered {}
            );
        }
    }
    let (correlation, ending) = next_ending(&mut socket, &target).await?;
    assert_eq!(ending.request_id(), approval.request_id());
    answer_ending(
        &mut socket,
        target.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    observe_received(fixture, &approval).await?;
    assert_eq!(
        register_run(&mut socket, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    heartbeat(&mut socket).await?;
    require_duplicate_capabilities_close(&mut socket, duplicate_close).await
}

pub async fn exchange_direct_mail(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    machine: &MachineId,
    ordinal: u32,
) -> Result<()> {
    let session = SessionRef {
        machine_id: machine.clone(),
        session_id: "capability-mail-session".to_owned().try_into()?,
    };
    let now = chrono::DateTime::from_timestamp(fixture.now_seconds(), 0).ok_or("fixture clock")?;
    let request_id = format!("019f0000-0000-7000-8000-{:012x}", 0x6000 + ordinal);
    let correlation = format!("019f0000-0000-7000-8000-{:012x}", 0x6100 + ordinal).parse()?;
    let request = LinkRequest::new(
        correlation,
        Caller::Session(session.clone()),
        RequestPayload::Mail(serde_json::from_value(json!({
            "requestId":request_id,
            "operation":{"kind":"sendMessage","to":session,"body":"capability proof",
                "delivery":"auto","replyTo":null,"sentAt":now}
        }))?),
    )?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::Reply { result, .. }) = receive_frame(socket).await? else {
        return Err("expected direct mail reply without an intervening call".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        *result
    else {
        return Err("direct mail was not queued".into());
    };
    assert!(
        matches!(receive_frame(socket).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == &message_id)
    );
    let correlation_id = format!("019f0000-0000-7000-8000-{:012x}", 0x6200 + ordinal).parse()?;
    let receipts = ReceiptBatch::new(vec![
        Receipt {
            message_id: message_id.clone(),
            kind: ReceiptKind::Delivered { arrived_at: now },
        },
        Receipt {
            message_id,
            kind: ReceiptKind::Seen { completed_at: now },
        },
    ])?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::Receipts {
            correlation_id,
            receipts,
        }),
    )
    .await?;
    assert!(
        matches!(receive_frame(socket).await?, LinkFrame::Data(DataFrame::ReceiptResults { results, .. }) if results == [ReceiptResult::Recorded, ReceiptResult::Recorded])
    );
    heartbeat(socket).await
}
