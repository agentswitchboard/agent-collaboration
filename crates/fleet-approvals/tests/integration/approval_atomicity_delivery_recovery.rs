use approval_request_protocol::DeliverEndingReply;
use fleet_approvals::{ApprovalApplyingTime, ApprovalHolderAdmission};
use fleet_runtime_ports::Clock;

use super::{
    approval_atomicity_raise_stop::waiting_approval,
    approval_store_atomicity::{assert_loan_rollback_each_write, assert_rollback_each_write},
    fixtures::HOUR,
};

#[tokio::test]
async fn recovery_rolls_back_timing_out_a_waiting_approval() {
    let (fixture, holder, _, raise, _) = waiting_approval("Recovered at deadline").await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    fixture.clock.set_millis(HOUR);

    let recovered = assert_rollback_each_write(&fixture, 3, async |store| {
        store.recover_owed_approval_endings(&admission).await
    })
    .await;

    assert_eq!(recovered.reply, Ok(()));
    let [offer] = recovered.ending_offers.as_slice() else {
        panic!("the Waiting request must yield one committed ending");
    };
    assert_eq!(offer.request.request_id(), raise.request_id());
    let (repeated, evidence) = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&admission).await
        })
        .await;
    assert_eq!(repeated.unwrap().ending_offers.len(), 1);
    assert!(evidence.sql_writes.is_empty());
}

#[tokio::test]
async fn not_held_rolls_back_closing_delivery_until_the_answer_commits() {
    let (fixture, holder, _, raise, _) = waiting_approval("Holder lost request").await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    fixture.clock.set_millis(HOUR);
    let (ending, _) = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&admission).await
        })
        .await;
    let [offer] = ending.unwrap().ending_offers.try_into().unwrap();
    assert_eq!(offer.request.request_id(), raise.request_id());

    assert_rollback_each_write(&fixture, 1, async |store| {
        store
            .record_holder_delivery(
                &admission,
                offer.request.clone(),
                DeliverEndingReply::NotHeld {},
            )
            .await
    })
    .await;
    assert_eq!(
        fixture
            .inspect_rows("SELECT delivery_state FROM approval_requests")
            .await,
        ["closed_without_receipt"],
    );
    let (repeated, evidence) = fixture
        .apply(None, async |store| {
            store
                .record_holder_delivery(
                    &admission,
                    offer.request.clone(),
                    DeliverEndingReply::NotHeld {},
                )
                .await
        })
        .await;
    assert!(repeated.is_ok());
    assert!(evidence.sql_writes.is_empty());
}

#[tokio::test]
async fn the_approval_time_writer_rolls_back_an_advance_and_keeps_the_prior_floor() {
    let fixture = super::native_approval_store::NativeApprovalStoreFixture::open_fresh().await;
    fixture.provision_organization().await;
    let advance = async |fail_after, millis| {
        fixture.clock.set_millis(millis);
        fixture
            .apply_on_loan(fail_after, async |loan, _, _| {
                let time = ApprovalApplyingTime::read(loan, fixture.clock.now()).await?;
                let effective = time.now();
                time.persist(loan).await?;
                Ok(effective)
            })
            .await
    };
    advance(None, 100).await.0.unwrap();

    let (_, effective) =
        assert_loan_rollback_each_write(&fixture, 1, async |ordinal| advance(ordinal, 200).await)
            .await;

    assert_eq!(effective.unix_millis(), 200);
    let (replayed, evidence) = advance(None, 150).await;
    assert_eq!(replayed.unwrap().1.unix_millis(), 200);
    assert!(evidence.sql_writes.is_empty());
    assert_eq!(
        fixture
            .inspect_rows("SELECT CAST(observed_ms AS TEXT) FROM approval_applying_time")
            .await,
        ["200"]
    );
}
