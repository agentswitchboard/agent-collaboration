use approval_request_protocol::{Raise, RaiseReply, StopReceiptEnding, StopReply};
use fleet_approvals::{ApprovalHolderAdmission, ApprovalHolderScope, ApprovalSession};

use super::{
    approval_store_atomicity::assert_rollback_each_write,
    approval_store_cases::{fixture_with_owner, raise_and_commit},
    fixtures::{HOUR, key, provider, provider_stop},
    native_approval_store::NativeApprovalStoreFixture,
};

pub(super) async fn waiting_approval(
    label: &'static str,
) -> (
    NativeApprovalStoreFixture,
    ApprovalHolderScope,
    ApprovalSession,
    Raise,
    RaiseReply,
) {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let raise = provider(&key(&holder.run, 1), &requester, label);
    let (reply, _) = raise_and_commit(&fixture, &holder, &raise).await;
    assert!(matches!(reply, RaiseReply::Receipt { .. }));
    (fixture, holder, requester, raise, reply)
}

#[tokio::test]
async fn people_raise_rolls_back_the_request_destination_and_deadline() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let raise = provider(&key(&holder.run, 1), &requester, "Routed to People");
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);

    let committed = assert_rollback_each_write(&fixture, 3, async |store| {
        store
            .raise_from_holder(&admission, Box::new(raise.clone()))
            .await
    })
    .await;

    assert!(matches!(committed.reply, RaiseReply::Receipt { .. }));
    assert_eq!(
        fixture
            .inspect_rows("SELECT destination_kind FROM approval_request_destinations")
            .await,
        ["people"]
    );
}

#[tokio::test]
async fn a_due_raise_replay_rolls_back_the_timeout_until_its_receipt_commits() {
    let (fixture, holder, _, raise, first_receipt) = waiting_approval("Due replay").await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    fixture.clock.set_millis(HOUR);

    let committed = assert_rollback_each_write(&fixture, 3, async |store| {
        store
            .raise_from_holder(&admission, Box::new(raise.clone()))
            .await
    })
    .await;

    assert_eq!(committed.reply, first_receipt);
    assert_eq!(committed.ending_offers.len(), 1);
    assert_eq!(
        fixture
            .inspect_rows("SELECT delivery_state FROM approval_requests")
            .await,
        ["owed"]
    );
}

#[tokio::test]
async fn a_late_stop_rolls_back_the_timeout_and_its_new_retention_due_work() {
    let (fixture, holder, _, raise, _) = waiting_approval("Late stop").await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    let stop = provider_stop(&raise);
    fixture.clock.set_millis(HOUR);

    let committed = assert_rollback_each_write(&fixture, 3, async |store| {
        store.stop_from_holder(&admission, stop.clone()).await
    })
    .await;

    assert_eq!(
        committed.reply,
        StopReply::Stands {
            ending: StopReceiptEnding::TimedOut {}
        }
    );
    assert!(committed.ending_offers.is_empty());
    assert_eq!(
        fixture
            .inspect_rows("SELECT delivery_state FROM approval_requests")
            .await,
        ["closed_without_receipt"]
    );
}

#[tokio::test]
async fn a_stop_rolls_back_closing_an_owed_ending() {
    let (fixture, holder, _, raise, _) = waiting_approval("Owed ending stop").await;
    fixture.clock.set_millis(HOUR);
    let _ = raise_and_commit(&fixture, &holder, &raise).await;
    assert_eq!(
        fixture
            .inspect_rows("SELECT delivery_state FROM approval_requests")
            .await,
        ["owed"]
    );
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    let stop = provider_stop(&raise);

    let committed = assert_rollback_each_write(&fixture, 1, async |store| {
        store.stop_from_holder(&admission, stop.clone()).await
    })
    .await;

    assert_eq!(
        committed.reply,
        StopReply::Stands {
            ending: StopReceiptEnding::TimedOut {}
        }
    );
    assert_eq!(
        fixture
            .inspect_rows("SELECT delivery_state FROM approval_requests")
            .await,
        ["closed_without_receipt"]
    );
}
