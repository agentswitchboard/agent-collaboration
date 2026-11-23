use router_clock::{ClockInstant, RuntimeClock};
use std::{future::Future, sync::Arc, time::Duration};
use switchboard_machine_core_store::OutboxStorage;
use switchboard_message_custody::custody_task::{CustodyTaskEvent, custody_task};
use tokio::sync::{mpsc, watch};
use workspace_model::{Caller, Timestamp};

use super::{
    batch_replay::new_admission,
    custody_task::ObserveTask,
    faulted_overlay::{FaultCampaign, MARK_MS},
    outbox_timing::{open_outbox, thread_post},
};

async fn observe_before_spin<Observed>(
    expected: impl Future<Output = Observed>,
    name: &str,
) -> Observed {
    tokio::select! {
        biased;
        observed = expected => observed,
        () = async { for _ in 0..50_000 { tokio::task::yield_now().await; } } => panic!("custody did not reach {name}"),
    }
}

#[tokio::test]
async fn worker_readmission_after_a_missed_mark_arms_the_next_window_without_spinning() {
    let mut campaign = FaultCampaign::start().await;
    campaign.disconnect();
    let write = campaign.submit(None).await;
    let peer = campaign.peer.clone();
    let clock = campaign.clock.clone();
    let Caller::Session(owner) = &write.caller else {
        panic!("fixture must have a session");
    };
    let (stop, stopped) = watch::channel(false);
    let (sender, mut events) = mpsc::unbounded_channel();
    let (_calls, worker) = custody_task(campaign.custody, stopped, Arc::new(ObserveTask(sender)));
    let running = tokio::spawn(worker);
    clock.advance(Duration::from_secs(30)).unwrap();
    let waiting_since = clock.wait_checkpoint();
    peer.admitted
        .send_replace(Some(new_admission(owner.machine_id.clone(), clock.clone())));
    let next = ClockInstant::ZERO.after(Duration::from_secs(80));
    let disconnected_gap = clock.monotonic_now();
    observe_before_spin(
        async {
            loop {
                let at_next = clock
                    .wait_registrations_since(waiting_since)
                    .into_iter()
                    .filter(|wait| {
                        wait.deadline == next
                            && !wait.dropped
                            && wait.first_polled_at == Some(disconnected_gap)
                    })
                    .count();
                // Two separate futures wait: one for writes and one for receipts.
                if at_next == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        "the first still-open write window",
    )
    .await;
    assert!(peer.batch_commands().is_empty());
    clock.advance(Duration::from_secs(49)).unwrap();
    assert!(peer.batch_commands().is_empty());
    clock.advance(Duration::from_secs(1)).unwrap();
    let report = observe_before_spin(events.recv(), "the next mark's write outcome")
        .await
        .unwrap();
    assert!(matches!(report, CustodyTaskEvent::Flush(report)
        if matches!(report.answered.as_slice(), [answer] if answer.request == write.request)));
    assert_eq!(peer.batch_commands().len(), 1);
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_backwards_welcome_rebases_waiting_receipts_but_preserves_used_windows() {
    let mut campaign = FaultCampaign::start().await;
    let first = campaign.submit(None).await;
    campaign.clock.advance(Duration::from_secs(20)).unwrap();
    let flushed = campaign.custody.flush_writes().await.unwrap();
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
    campaign.peer.admitted.send_replace(Some(backwards));
    let available = ClockInstant::ZERO.after(Duration::from_secs(75));
    assert_eq!(
        campaign.custody.refresh_flush_wake().await.unwrap(),
        Some(available),
        "the waiting receipt must be due in the new clock's next unused window"
    );
    campaign.clock.advance(Duration::from_secs(50)).unwrap();
    let rebased = campaign.custody.flush_writes().await.unwrap();
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
    campaign.peer.admitted.send_replace(Some(returned));
    assert!(
        campaign
            .custody
            .flush_writes()
            .await
            .unwrap()
            .answered
            .is_empty(),
        "the previously consumed numeric window cannot be reopened"
    );
    assert_eq!(campaign.peer.batch_commands().len(), 2);
    campaign.clock.advance(Duration::from_secs(60)).unwrap();
    let current = campaign.custody.flush_writes().await.unwrap();
    assert!(
        matches!(current.answered.as_slice(), [answer] if answer.request == earlier_receipt.request)
    );
    assert_eq!(campaign.peer.batch_commands().len(), 3);
}
