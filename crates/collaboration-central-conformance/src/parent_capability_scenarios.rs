//! Keep both owed call kinds durable while the real socket handles ordinary mail.

use super::{
    ApprovalLinkFixture, ApprovalNoticeReply, DecideApprovalAsSessionReply,
    DecideApprovalAsSessionRequest, DecideApprovalAsSessionRpc, ProviderAsk,
    ReadApprovalForSessionReply, ReadApprovalForSessionRequest, ReadApprovalForSessionRpc,
    RegisterRunReply, Result, SessionProof, acknowledge_notice, answered, call, create_bound,
    first_read, heartbeat, option, provider_raise, raise, register_run, require_receipt,
};
use crate::approval_link_scenarios::{
    announce_mac_calls, answer_ending, exchange_direct_mail, next_ending, observe_received,
    require_owed,
};
use approval_request_protocol::DeliverEndingReply;
use central_link_protocol::MacCallKind;
use central_storage::SqlValue;
use reqwest_websocket::CloseCode;

pub async fn both_owed_kinds_follow_the_socket_capabilities(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut seed = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut seed, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (parent_session, parent_secret) = create_bound(&mut seed, organization, 61, None).await?;
    let parent = SessionProof {
        session_id: parent_session,
        secret: parent_secret,
    };
    let (child, _) = create_bound(
        &mut seed,
        organization,
        62,
        Some((&parent.session_id, parent.secret.clone())),
    )
    .await?;
    let ending_approval = provider_raise(&target, &child, 63, ProviderAsk::Approval)?;
    let notice_approval = provider_raise(&target, &child, 64, ProviderAsk::Approval)?;
    for approval in [&ending_approval, &notice_approval] {
        require_receipt(raise(&mut seed, organization, approval.clone()).await?)?;
    }
    let decided = answered(
        call::<DecideApprovalAsSessionRpc>(
            &mut seed,
            organization,
            parent.request(DecideApprovalAsSessionRequest {
                request_id: ending_approval.request_id().clone(),
                operation_id: super::typed_id(0x61c5)?,
                value: option(1)?,
            }),
        )
        .await?,
    )?;
    assert!(matches!(
        decided,
        DecideApprovalAsSessionReply::SavedDecision { .. }
    ));
    require_owed(fixture, &ending_approval).await?;
    heartbeat(&mut seed).await?;
    seed.close(CloseCode::Normal, None).await?;

    for (mail_ordinal, announced) in [(41, false), (42, true)] {
        let mut socket = fixture.connect_without_capabilities().await?;
        if announced {
            announce_mac_calls(&mut socket, vec![]).await?;
        }
        assert_eq!(
            register_run(&mut socket, organization, &target.router_run).await?,
            RegisterRunReply::Registered {}
        );
        exchange_direct_mail(fixture, &mut socket, &target.machine_id, mail_ordinal).await?;
        require_owed(fixture, &ending_approval).await?;
        assert_eq!(
            first_read(fixture, &parent.session_id, notice_approval.request_id()).await?,
            [[SqlValue::Null]],
        );
        socket.close(CloseCode::Normal, None).await?;
    }

    // The notice-only socket may read without a run registration. Its notice
    // reply alone leaves the notice owed, so the next connection must also gate it.
    let mut notice_socket = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut notice_socket, vec![MacCallKind::ApprovalNotice]).await?;
    acknowledge_notice(
        &mut notice_socket,
        &target,
        &parent.session_id,
        notice_approval.request_id(),
        ApprovalNoticeReply::Received {},
    )
    .await?;
    assert_eq!(
        register_run(&mut notice_socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    heartbeat(&mut notice_socket).await?;
    require_owed(fixture, &ending_approval).await?;
    assert_eq!(
        first_read(fixture, &parent.session_id, notice_approval.request_id()).await?,
        [[SqlValue::Null]],
    );
    notice_socket.close(CloseCode::Normal, None).await?;

    let mut ending_socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut ending_socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    heartbeat(&mut ending_socket).await?;
    announce_mac_calls(&mut ending_socket, vec![MacCallKind::DeliverEnding]).await?;
    let (correlation, ending) = next_ending(&mut ending_socket, &target).await?;
    assert_eq!(ending.request_id(), ending_approval.request_id());
    answer_ending(
        &mut ending_socket,
        target.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    observe_received(fixture, &ending_approval).await?;
    heartbeat(&mut ending_socket).await?;
    assert_eq!(
        first_read(fixture, &parent.session_id, notice_approval.request_id()).await?,
        [[SqlValue::Null]],
    );
    ending_socket.close(CloseCode::Normal, None).await?;

    let mut final_notice = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut final_notice, vec![MacCallKind::ApprovalNotice]).await?;
    acknowledge_notice(
        &mut final_notice,
        &target,
        &parent.session_id,
        notice_approval.request_id(),
        ApprovalNoticeReply::Received {},
    )
    .await?;
    let read = answered(
        call::<ReadApprovalForSessionRpc>(
            &mut final_notice,
            organization,
            parent.request(ReadApprovalForSessionRequest {
                request_id: notice_approval.request_id().clone(),
            }),
        )
        .await?,
    )?;
    assert!(matches!(read, ReadApprovalForSessionReply::Request { .. }));
    let first_read_at =
        first_read(fixture, &parent.session_id, notice_approval.request_id()).await?;
    assert!(
        matches!(first_read_at.as_slice(), [row] if matches!(row.as_slice(), [SqlValue::Text(_)]))
    );
    heartbeat(&mut final_notice).await?;
    final_notice.close(CloseCode::Normal, None).await?;

    fixture.arm_notice_recovery().await?;
    let mut settled = fixture.connect_without_capabilities().await?;
    announce_mac_calls(&mut settled, vec![MacCallKind::ApprovalNotice]).await?;
    super::delivery_cases::observe_no_recovered_notice(fixture, &mut settled).await?;
    settled.close(CloseCode::Normal, None).await?;
    Ok(())
}
