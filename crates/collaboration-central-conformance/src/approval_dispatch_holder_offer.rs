//! Observe the original committed person ending on the holder link.
use approval_request_protocol::{Ending, RegisterRunReply};
use central_link_protocol::MacCallKind;
use reqwest_websocket::{CloseCode, WebSocket};
use std::num::NonZeroUsize;

use crate::approval_link_scenarios::{announce_mac_calls, heartbeat, next_ending, register_run};

use super::{ApprovalDispatchCase, ApprovalDispatchRollbackHost, Prepared, Result, apply};

pub(super) async fn apply_cut(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
    ordinal: NonZeroUsize,
) -> Result<()> {
    if !matches!(
        prepared.case,
        ApprovalDispatchCase::FirstPersonSaved
            | ApprovalDispatchCase::FirstPersonLate
            | ApprovalDispatchCase::FirstPersonInvalid
    ) {
        return apply(host, prepared, Some(Some(ordinal)), true).await;
    }

    let mut holder = registered_before_decision(host, prepared.at).await?;
    let recovered = host.ending_recovery_count().await?;
    announce_mac_calls(&mut holder, vec![MacCallKind::DeliverEnding]).await?;
    host.wait_ending_recovery(recovered + 1).await?;
    heartbeat(&mut holder).await?;
    let scheduled = host.scheduled_delivery_count().await?;

    apply(host, prepared, Some(Some(ordinal)), true).await?;

    // The correlated refusal comes after the person dispatcher returns.
    // Count synchronous handoffs so a later offer task cannot escape this check.
    assert_eq!(
        host.scheduled_delivery_count().await?,
        scheduled,
        "{:?} cut {ordinal} scheduled a holder offer",
        prepared.case,
    );
    heartbeat(&mut holder).await?;
    holder.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub(super) async fn apply_unarmed(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
) -> Result<()> {
    if !matches!(
        prepared.case,
        ApprovalDispatchCase::FirstPersonSaved | ApprovalDispatchCase::FirstPersonLate
    ) {
        return apply(host, prepared, None, false).await;
    }
    let mut holder = registered_before_decision(host, prepared.at).await?;
    announce_mac_calls(&mut holder, vec![MacCallKind::DeliverEnding]).await?;
    heartbeat(&mut holder).await?;

    apply(host, prepared, None, false).await?;

    let (_, offer) = next_ending(&mut holder, &host.target()?).await?;
    let request = prepared.request()?;
    assert_eq!(offer.request_id(), request.request_id());
    assert_eq!(offer.contents_digest(), &request.contents_digest()?);
    assert!(matches!(
        (prepared.case, offer.into_ending()),
        (
            ApprovalDispatchCase::FirstPersonSaved,
            Ending::Decided { .. }
        ) | (ApprovalDispatchCase::FirstPersonLate, Ending::TimedOut {})
    ));
    holder.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub(super) async fn registered_before_decision(
    host: &impl ApprovalDispatchRollbackHost,
    decision_at: i64,
) -> Result<WebSocket> {
    // A new socket must register its run. Open it before the decision's
    // applying sample so the registration cannot consume that floor advance.
    host.set_time(decision_at - 1_000).await?;
    let mut holder = host.connect_machine().await?;
    let target = host.target()?;
    assert_eq!(
        register_run(&mut holder, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {},
    );
    Ok(holder)
}
