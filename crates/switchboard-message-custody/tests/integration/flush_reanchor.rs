use router_clock::{ClockInstant, RuntimeClock};
use std::time::Duration;
use switchboard_machine_core_store::OutboxStorage;
use workspace_model::{Caller, Timestamp};

use super::{
    batch_replay::new_admission,
    faulted_overlay::{MARK_MS, WorkerFaultCampaign},
    outbox_timing::{open_outbox, thread_post},
};

#[tokio::test]
async fn a_welcome_that_never_registers_still_anchors_offline_receipts() {
    let mut campaign = WorkerFaultCampaign::start().await;
    let session = super::inbox_custody::recipient_session();
    let mut replacement = new_admission(session.machine_id, campaign.clock.clone());
    replacement.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(MARK_MS - 145_000).unwrap();
    let before_welcome = campaign.worker.clock.checkpoint();
    campaign.peer.set_admitted(Some(replacement));
    campaign.worker.wait_after(before_welcome).await;
    let before_withdrawal = campaign.worker.clock.checkpoint();
    campaign.peer.set_connected(None);
    campaign.worker.wait_after(before_withdrawal).await;

    let write = campaign.submit(None).await;
    assert!(campaign.pending().await.iter().any(|stored| {
        stored.request_id == write.request && stored.flush_at_ms == MARK_MS - 120_000
    }));
    campaign.stop().await;
}

#[tokio::test]
async fn waiting_writes_are_not_claimed_until_the_connection_registers() {
    for (hold, send_at) in [(20, 20), (24, 80)] {
        let mut campaign = WorkerFaultCampaign::start().await;
        let admission = campaign.peer.admitted.borrow().clone().unwrap();
        campaign.peer.set_admitted(Some(admission));
        let write = campaign.submit(None).await;

        let held = campaign
            .worker
            .advance(Duration::from_secs(hold))
            .await
            .unwrap();
        assert!(held.answered.is_empty() && held.issues.is_empty() && held.ended.is_empty());
        assert_eq!(
            campaign.faults.claim_calls(),
            0,
            "due work must stay unclaimed"
        );
        assert!(campaign.peer.batch_commands().is_empty());

        campaign.peer.register_admitted();
        let report = campaign
            .worker
            .advance(Duration::from_secs(send_at - hold))
            .await
            .unwrap();
        assert!(matches!(report.answered.as_slice(), [answer] if answer.request == write.request));
        assert_eq!(campaign.peer.batch_commands().len(), 1);
        assert_eq!(
            campaign.peer.command_instants(),
            [ClockInstant::ZERO.after(Duration::from_secs(send_at))]
        );
        campaign.stop().await;
    }
}

#[tokio::test]
async fn worker_readmission_after_a_missed_mark_arms_the_next_window_without_spinning() {
    let mut campaign = WorkerFaultCampaign::start().await;
    campaign.disconnect();
    let write = campaign.submit(None).await;
    let peer = campaign.peer.clone();
    let clock = campaign.clock.clone();
    let Caller::Session(owner) = &write.caller else {
        panic!("fixture must have a session");
    };
    clock.advance(Duration::from_secs(30)).unwrap();
    let waiting_since = clock.wait_checkpoint();
    let before_admission = campaign.worker.clock.checkpoint();
    peer.set_connected(Some(new_admission(owner.machine_id.clone(), clock.clone())));
    let next = ClockInstant::ZERO.after(Duration::from_secs(80));
    campaign.worker.wait_for_timer(before_admission, next).await;
    let disconnected_gap = clock.monotonic_now();
    assert_eq!(
        clock
            .wait_registrations_since(waiting_since)
            .iter()
            .filter(|wait| wait.deadline == next
                && !wait.dropped
                && wait.first_polled_at == Some(disconnected_gap))
            .count(),
        2,
        "the first still-open mark must arm both writes and receipts"
    );
    assert!(peer.batch_commands().is_empty());
    campaign
        .worker
        .advance(Duration::from_secs(49))
        .await
        .unwrap();
    assert!(peer.batch_commands().is_empty());
    let report = campaign
        .worker
        .advance(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(report.answered.as_slice(), [answer] if answer.request == write.request));
    assert_eq!(peer.batch_commands().len(), 1);
    campaign.stop().await;
}

#[tokio::test]
async fn a_backwards_welcome_rebases_waiting_receipts_but_preserves_used_windows() {
    let mut campaign = WorkerFaultCampaign::start().await;
    let first = campaign.submit(None).await;
    let flushed = campaign
        .worker
        .advance(Duration::from_secs(20))
        .await
        .unwrap();
    assert!(matches!(flushed.answered.as_slice(), [answer] if answer.request == first.request));
    campaign.clock.advance(Duration::from_secs(5)).unwrap();
    let old_receipt = campaign.submit(None).await;
    assert!(
        campaign
            .pending()
            .await
            .iter()
            .any(|stored| stored.request_id == old_receipt.request
                && stored.flush_at_ms == MARK_MS + 60_000)
    );

    let Caller::Session(owner) = &first.caller else {
        panic!("fixture must have a session");
    };
    let mut backwards = new_admission(owner.machine_id.clone(), campaign.clock.clone());
    backwards.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(MARK_MS - 1_190_000).unwrap();
    let before_rebase = campaign.worker.clock.checkpoint();
    campaign.peer.set_connected(Some(backwards));
    let available = ClockInstant::ZERO.after(Duration::from_secs(75));
    campaign
        .worker
        .wait_for_timer(before_rebase, available)
        .await;
    assert!(
        campaign.clock.has_not_due_check_now(available),
        "the waiting receipt must be due in the new clock's next unused window"
    );
    let rebased = campaign
        .worker
        .advance(Duration::from_secs(50))
        .await
        .unwrap();
    assert!(
        matches!(rebased.answered.as_slice(), [answer] if answer.request == old_receipt.request)
    );

    let mut earlier_receipt = thread_post(630);
    earlier_receipt.flush_at_ms = MARK_MS - 1_140_000;
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    store
        .insert(
            &earlier_receipt,
            campaign.clock.utc_now().timestamp_millis(),
        )
        .await
        .unwrap();
    let mut returned = new_admission(owner.machine_id.clone(), campaign.clock.clone());
    returned.clock_anchor.central_time = Timestamp::from_timestamp_millis(MARK_MS).unwrap();
    let before_return = campaign.worker.clock.checkpoint();
    campaign.peer.set_connected(Some(returned));
    campaign
        .worker
        .wait_for_timer(
            before_return,
            ClockInstant::ZERO.after(Duration::from_secs(135)),
        )
        .await;
    assert!(
        campaign.worker.take_reports().unwrap().answered.is_empty(),
        "the previously consumed numeric window cannot be reopened"
    );
    assert_eq!(campaign.peer.batch_commands().len(), 2);
    let current = campaign
        .worker
        .advance(Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        matches!(current.answered.as_slice(), [answer] if answer.request == earlier_receipt.request)
    );
    assert_eq!(campaign.peer.batch_commands().len(), 3);
    campaign.stop().await;
}
