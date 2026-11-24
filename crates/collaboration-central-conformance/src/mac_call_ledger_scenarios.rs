//! Force live and recovered offers to compete for the same physical socket.

use super::{
    ApprovalLinkFixture, ApprovalNoticeReply, ApprovalNoticeRpc, ApprovalRequestId, CorrelationId,
    DataFrame, Envelope, LinkFrame, MacCall, MacLinkRpc, ProviderAsk, ReadApprovalForSessionReply,
    ReadApprovalForSessionRequest, ReadApprovalForSessionRpc, RegisterRunReply, Result,
    SessionProof, TypedCallResult, WebSocket, answered, call, create_bound, first_read, heartbeat,
    provider_raise, raise, receive_frame, register_run, require_receipt, send_frame,
};
use crate::approval_link_scenarios::{
    announce_mac_calls, answer_ending, exchange_direct_mail, next_ending, observe_received,
    require_owed,
};
use crate::fixture_link_reader::read_fixture_frame;
use approval_request_protocol::{DeliverEndingReply, Raise};
use central_link_protocol::{
    MAX_SOCKET_MAC_CALL_ITEMS, MacCallKind,
    approval_rpc::{ApprovalHolderTarget, ApprovalNoticeEnvelope},
};
use central_storage::SqlValue;
use futures_util::future::{Either, select};
use reqwest_websocket::{CloseCode, Message};
use std::{future::Future, pin::pin};

#[derive(Clone, Copy)]
pub enum MacDeliveryEvent {
    BeforeEndingOffer,
    AfterEndingOffer,
    BeforeNoticeOffer,
    AfterNoticeOffer,
    EndingRecoveryFinished,
    RegistrationRecoveryScheduled,
}

impl MacDeliveryEvent {
    pub const fn control(self) -> &'static str {
        match self {
            Self::BeforeEndingOffer => "beforeMacEndingOffer",
            Self::AfterEndingOffer => "afterMacEndingOffer",
            Self::BeforeNoticeOffer => "beforeMacNoticeOffer",
            Self::AfterNoticeOffer => "afterMacNoticeOffer",
            Self::EndingRecoveryFinished => "macEndingRecoveryFinished",
            Self::RegistrationRecoveryScheduled => "macEndingRegistrationRecoveryScheduled",
        }
    }
}

pub trait LedgerFixture: ApprovalLinkFixture {
    fn hold_mac_event(&self, event: MacDeliveryEvent) -> impl Future<Output = Result<()>>;
    fn wait_mac_event_hold(&self, event: MacDeliveryEvent) -> impl Future<Output = Result<()>>;
    fn release_mac_event(&self, event: MacDeliveryEvent) -> impl Future<Output = Result<()>>;
    fn mac_event_count(&self, event: MacDeliveryEvent) -> impl Future<Output = Result<u64>>;
    fn wait_mac_event_count(
        &self,
        event: MacDeliveryEvent,
        count: u64,
    ) -> impl Future<Output = Result<()>>;
    fn hibernate_ledger_socket(&self) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }
    fn mac_timer_waits(&self) -> impl Future<Output = Result<u64>>;
}

async fn wait_without_frame(
    socket: &mut WebSocket,
    barrier: impl Future<Output = Result<()>>,
) -> Result<()> {
    match select(pin!(receive_frame(socket)), pin!(barrier)).await {
        Either::Left((frame, _)) => {
            Err(format!("unexpected frame before the delivery barrier: {:?}", frame?).into())
        }
        Either::Right((result, _)) => result,
    }
}

async fn register(socket: &mut WebSocket, holder: &ApprovalHolderTarget) -> Result<()> {
    assert_eq!(
        register_run(socket, &holder.organization_id, &holder.router_run).await?,
        RegisterRunReply::Registered {},
    );
    Ok(())
}

pub async fn both_live_and_recovered_items_have_one_attempt(
    fixture: &impl LedgerFixture,
    duplicate_close: u16,
) -> Result<()> {
    let holder = fixture.target()?;
    let mut socket = fixture.connect_without_capabilities().await?;
    register(&mut socket, &holder).await?;
    let (requester, _) = create_bound(&mut socket, &holder.organization_id, 91, None).await?;
    let ending = provider_raise(&holder, &requester, 92, ProviderAsk::Approval)?;
    require_receipt(raise(&mut socket, &holder.organization_id, ending.clone()).await?)?;

    fixture
        .hold_mac_event(MacDeliveryEvent::BeforeEndingOffer)
        .await?;
    fixture
        .decide_provider_approval(ending.request_id())
        .await?;
    fixture
        .wait_mac_event_hold(MacDeliveryEvent::BeforeEndingOffer)
        .await?;
    require_owed(fixture, &ending).await?;
    announce_mac_calls(&mut socket, vec![MacCallKind::DeliverEnding]).await?;
    let (correlation, offered) = next_ending(&mut socket, &holder).await?;
    assert_eq!(offered.request_id(), ending.request_id());

    let before = fixture
        .mac_event_count(MacDeliveryEvent::AfterEndingOffer)
        .await?;
    fixture
        .release_mac_event(MacDeliveryEvent::BeforeEndingOffer)
        .await?;
    wait_without_frame(
        &mut socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::AfterEndingOffer, before + 1),
    )
    .await?;
    answer_ending(
        &mut socket,
        holder.clone(),
        correlation,
        DeliverEndingReply::HolderNotReady {},
    )
    .await?;
    wait_without_frame(
        &mut socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::EndingRecoveryFinished, 1),
    )
    .await?;
    require_owed(fixture, &ending).await?;

    let planned = fixture
        .mac_event_count(MacDeliveryEvent::RegistrationRecoveryScheduled)
        .await?;
    register(&mut socket, &holder).await?;
    assert_eq!(
        fixture
            .mac_event_count(MacDeliveryEvent::RegistrationRecoveryScheduled)
            .await?,
        planned
    );
    assert_eq!(
        fixture
            .mac_event_count(MacDeliveryEvent::EndingRecoveryFinished)
            .await?,
        1
    );
    heartbeat(&mut socket).await?;
    socket.close(CloseCode::Normal, None).await?;

    let mut empty = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut empty, vec![]).await?;
    register(&mut empty, &holder).await?;
    heartbeat(&mut empty).await?;
    announce_mac_calls(&mut empty, vec![MacCallKind::DeliverEnding]).await?;
    match read_fixture_frame(&mut empty, "capability close").await? {
        Some(Ok(Message::Close { code, .. })) if u16::from(code) == duplicate_close => {}
        frame => {
            return Err(
                format!("empty then populated must close before any call: {frame:?}").into(),
            );
        }
    }
    require_owed(fixture, &ending).await?;

    let mut replacement = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut replacement, vec![MacCallKind::DeliverEnding]).await?;
    register(&mut replacement, &holder).await?;
    let (correlation, offered) = next_ending(&mut replacement, &holder).await?;
    assert_eq!(offered.request_id(), ending.request_id());
    answer_ending(
        &mut replacement,
        holder.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    observe_received(fixture, &ending).await?;
    replacement.close(CloseCode::Normal, None).await?;

    let mut notice_socket = fixture.connect_without_capabilities().await?;
    register(&mut notice_socket, &holder).await?;
    let (parent, child) = parent_and_child(&mut notice_socket, &holder, 93).await?;
    let notice = provider_raise(&holder, &child, 95, ProviderAsk::Approval)?;
    fixture
        .hold_mac_event(MacDeliveryEvent::BeforeNoticeOffer)
        .await?;
    require_receipt(raise(&mut notice_socket, &holder.organization_id, notice.clone()).await?)?;
    fixture
        .wait_mac_event_hold(MacDeliveryEvent::BeforeNoticeOffer)
        .await?;
    announce_mac_calls(&mut notice_socket, vec![MacCallKind::ApprovalNotice]).await?;
    let (correlation, offered) = next_notice(&mut notice_socket, notice.request_id()).await?;
    assert_eq!(offered.target.session_id, parent.session_id);
    let before = fixture
        .mac_event_count(MacDeliveryEvent::AfterNoticeOffer)
        .await?;
    fixture
        .release_mac_event(MacDeliveryEvent::BeforeNoticeOffer)
        .await?;
    wait_without_frame(
        &mut notice_socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::AfterNoticeOffer, before + 1),
    )
    .await?;
    answer_notice(&mut notice_socket, correlation, offered).await?;
    wait_without_frame(
        &mut notice_socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::AfterNoticeOffer, before + 2),
    )
    .await?;
    assert_eq!(
        first_read(fixture, &parent.session_id, notice.request_id()).await?,
        [[SqlValue::Null]]
    );
    register(&mut notice_socket, &holder).await?;
    heartbeat(&mut notice_socket).await?;
    notice_socket.close(CloseCode::Normal, None).await?;

    let mut reconnect = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut reconnect, vec![MacCallKind::ApprovalNotice]).await?;
    let (correlation, offered) = next_notice(&mut reconnect, notice.request_id()).await?;
    answer_notice(&mut reconnect, correlation, offered).await?;
    commit_parent_read(&mut reconnect, &holder, &parent, &notice).await?;
    reconnect.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn unsupported_mac_call_registers_no_wait(fixture: &impl LedgerFixture) -> Result<()> {
    let holder = fixture.target()?;
    let mut socket = fixture.connect_without_capabilities().await?;
    register(&mut socket, &holder).await?;
    let (_, child) = parent_and_child(&mut socket, &holder, 15).await?;
    let notice = provider_raise(&holder, &child, 19, ProviderAsk::Approval)?;
    let before = fixture.mac_timer_waits().await?;
    let offers = fixture
        .mac_event_count(MacDeliveryEvent::AfterNoticeOffer)
        .await?;

    require_receipt(raise(&mut socket, &holder.organization_id, notice.clone()).await?)?;

    wait_without_frame(
        &mut socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::AfterNoticeOffer, offers + 1),
    )
    .await?;
    assert_eq!(fixture.mac_timer_waits().await?, before);
    heartbeat(&mut socket).await?;
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn full_mac_call_ledger_recovers_on_committed_read(
    fixture: &impl LedgerFixture,
) -> Result<()> {
    let holder = fixture.target()?;
    let mut socket = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut socket, vec![MacCallKind::ApprovalNotice]).await?;
    register(&mut socket, &holder).await?;
    let (parent, child) = parent_and_child(&mut socket, &holder, 101).await?;
    let mut first = None;
    for index in 0..MAX_SOCKET_MAC_CALL_ITEMS {
        let request = provider_raise(
            &holder,
            &child,
            200 + u16::try_from(index)?,
            ProviderAsk::Approval,
        )?;
        if first.is_none() {
            first = Some(request.clone());
        }
        require_receipt(raise(&mut socket, &holder.organization_id, request.clone()).await?)?;
        let (correlation, notice) = next_notice(&mut socket, request.request_id()).await?;
        answer_notice(&mut socket, correlation, notice).await?;
        wait_without_frame(
            &mut socket,
            fixture.wait_mac_event_count(
                MacDeliveryEvent::AfterNoticeOffer,
                u64::try_from(index)? + 1,
            ),
        )
        .await?;
    }
    fixture.hibernate_ledger_socket().await?;
    let previous_offers = fixture
        .mac_event_count(MacDeliveryEvent::AfterNoticeOffer)
        .await?;
    let deferred = provider_raise(&holder, &child, 400, ProviderAsk::Approval)?;
    require_receipt(raise(&mut socket, &holder.organization_id, deferred.clone()).await?)?;
    wait_without_frame(
        &mut socket,
        fixture.wait_mac_event_count(MacDeliveryEvent::AfterNoticeOffer, previous_offers + 1),
    )
    .await?;
    assert_eq!(
        first_read(fixture, &parent.session_id, deferred.request_id()).await?,
        [[SqlValue::Null]]
    );
    exchange_direct_mail(fixture, &mut socket, &holder.machine_id, 61).await?;

    commit_parent_read(
        &mut socket,
        &holder,
        &parent,
        &first.ok_or("full ledger has a first row")?,
    )
    .await?;

    let (correlation, notice) = next_notice(&mut socket, deferred.request_id()).await?;
    answer_notice(&mut socket, correlation, notice).await?;
    heartbeat(&mut socket).await?;
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

async fn parent_and_child(
    socket: &mut WebSocket,
    holder: &ApprovalHolderTarget,
    ordinal: u16,
) -> Result<(SessionProof, workspace_ids::CentralSessionId)> {
    let (session_id, secret) = create_bound(socket, &holder.organization_id, ordinal, None).await?;
    let parent = SessionProof { session_id, secret };
    let (child, _) = create_bound(
        socket,
        &holder.organization_id,
        ordinal + 1,
        Some((&parent.session_id, parent.secret.clone())),
    )
    .await?;
    Ok((parent, child))
}

async fn commit_parent_read(
    socket: &mut WebSocket,
    holder: &ApprovalHolderTarget,
    parent: &SessionProof,
    request: &Raise,
) -> Result<()> {
    let read = answered(
        call::<ReadApprovalForSessionRpc>(
            socket,
            &holder.organization_id,
            parent.request(ReadApprovalForSessionRequest {
                request_id: request.request_id().clone(),
            }),
        )
        .await?,
    )?;
    assert!(matches!(read, ReadApprovalForSessionReply::Request { .. }));
    Ok(())
}

async fn next_notice(
    socket: &mut WebSocket,
    expected: &ApprovalRequestId,
) -> Result<(CorrelationId, ApprovalNoticeEnvelope)> {
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = receive_frame(socket).await?
    else {
        return Err("expected a recovered notice frame".into());
    };
    let MacCall::ApprovalNotice(notice) = *request else {
        return Err("expected a parent notice".into());
    };
    assert_eq!(&notice.body.request_id, expected);
    Ok((correlation_id, notice))
}

async fn answer_notice(
    socket: &mut WebSocket,
    correlation_id: CorrelationId,
    offered: ApprovalNoticeEnvelope,
) -> Result<()> {
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: ApprovalNoticeRpc::reply(Envelope {
                    target: offered.target,
                    body: ApprovalNoticeReply::Received {},
                }),
            }),
        }),
    )
    .await
}
