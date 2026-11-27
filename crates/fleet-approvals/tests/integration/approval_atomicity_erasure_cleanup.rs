use approval_request_protocol::{Raise, RaiseReply, StopReply};
use central_engine_model::{DueRow, LAST_ALARM_GRID_UNIX_MILLIS};
use fleet_approvals::{
    ApprovalApplyingStore, ApprovalEnrollmentScope, ApprovalHolderAdmission, ApprovalHolderScope,
    ApprovalRun,
};

use super::{
    approval_atomicity_raise_stop::waiting_approval,
    approval_retention_boundary::fixture_with_erased_marker,
    approval_store_atomicity::{assert_loan_rollback_each_write, assert_rollback_each_write},
    approval_store_cases::{decide_input, fixture_with_owner, raise_and_commit},
    fixtures::{HOUR, key, person, provider, provider_stop, router_run, time},
    native_approval_store::{NativeApprovalStoreFixture, StoredSessionFacts},
};

async fn due_work(fixture: &NativeApprovalStoreFixture, subject_prefix: &str) -> DueRow {
    fixture
        .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 300)
        .await
        .into_iter()
        .find(|due| due.key.0.starts_with(subject_prefix))
        .unwrap_or_else(|| panic!("no due item for {subject_prefix}"))
}

async fn apply_due(fixture: &NativeApprovalStoreFixture, due: &DueRow) {
    fixture.clock.set_millis(due.effective_due_ms);
    let (applied, _) = fixture
        .apply(None, async |store| {
            store
                .apply_approval_due_work(&due.key, time(due.occurrence_ms))
                .await
        })
        .await;
    applied.unwrap();
}

async fn assert_due_rollback(
    fixture: &NativeApprovalStoreFixture,
    due: &DueRow,
    expected_writes: usize,
) {
    fixture.clock.set_millis(due.effective_due_ms);
    assert_rollback_each_write(fixture, expected_writes, async |store| {
        store
            .apply_approval_due_work(&due.key, time(due.occurrence_ms))
            .await
    })
    .await;
}

async fn stop_waiting(
    fixture: &NativeApprovalStoreFixture,
    holder: &ApprovalHolderScope,
    raise: &Raise,
) {
    let admission = ApprovalHolderAdmission::for_fixture_scope(holder);
    let (stopped, _) = fixture
        .apply(None, async |store| {
            store
                .stop_from_holder(&admission, provider_stop(raise))
                .await
        })
        .await;
    assert!(matches!(stopped.unwrap().reply, StopReply::Stands { .. }));
}

async fn end_enrollment(fixture: &NativeApprovalStoreFixture, holder: &ApprovalHolderScope) {
    fixture.store_enrollment(holder, "ended").await;
    let scope = ApprovalEnrollmentScope {
        organization_id: holder.organization_id.clone(),
        enrollment: holder.run.enrollment.clone(),
    };
    let (ended, _) = fixture
        .apply(None, async |store| {
            store.end_approval_enrollment(&scope).await
        })
        .await;
    ended.unwrap();
}

#[tokio::test]
async fn passing_the_cutoff_rolls_back_the_floor_without_erasing_the_request() {
    let (fixture, holder, _, raise, _) = waiting_approval("Cutoff observation").await;
    fixture.clock.set_millis(HOUR);
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    let (recovered, _) = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&admission).await
        })
        .await;
    assert_eq!(recovered.unwrap().ending_offers.len(), 1);
    let due = due_work(&fixture, "").await;
    fixture.provision_organization().await;
    let read_owed = async |ordinal| {
        fixture
            .apply_on_loan(ordinal, async |loan, commit, ids| {
                let mut store = ApprovalApplyingStore::new(
                    &commit,
                    loan,
                    &StoredSessionFacts,
                    &fixture.clock,
                    ids,
                );
                Ok(store
                    .holder_ending_is_owed(&holder, raise.request_id())
                    .await?)
            })
            .await
    };
    let (visible, evidence) = read_owed(None).await;
    assert!(visible.unwrap().1);
    assert!(evidence.sql_writes.is_empty());
    fixture.clock.set_millis(due.effective_due_ms + 1);

    let (_, owed) = assert_loan_rollback_each_write(&fixture, 1, read_owed).await;

    assert!(!owed);
    assert_eq!(
        fixture
            .inspect_rows("SELECT CAST(observed_ms AS TEXT) FROM approval_applying_time")
            .await,
        [due.occurrence_ms.to_string()],
    );
    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_requests WHERE delivery_state = 'owed'")
            .await,
        [raise.request_id().as_str()],
    );
    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn erasure_rolls_back_the_marker_and_all_deletions() {
    let (fixture, holder, _, raise, _) = waiting_approval("Erase after decision").await;
    let (decided, _) = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    person(11),
                    decide_input(raise.request_id(), 101, 1),
                )
                .await
        })
        .await;
    assert_eq!(decided.unwrap().ending_offers.len(), 1);
    assert_eq!(
        fixture
            .inspect_rows(
                "SELECT request_id FROM runtime_request_log WHERE approval_row_id IS NOT NULL"
            )
            .await
            .len(),
        1,
    );
    let due = due_work(&fixture, "").await;

    assert_due_rollback(&fixture, &due, 6).await;

    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await,
        [raise.request_id().as_str()],
    );
    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_requests")
            .await
            .is_empty()
    );
    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_request_destinations")
            .await
            .is_empty()
    );
    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM runtime_request_log")
            .await
            .is_empty()
    );
    assert!(
        fixture
            .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 1)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn retiring_marker_cleanup_rolls_back_a_full_page_and_its_next_due_time() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    for request_number in 1..=65 {
        let raise = provider(
            &key(&holder.run, request_number),
            &requester,
            "Retained ending",
        );
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &raise).await.0,
            RaiseReply::Receipt { .. }
        ));
        stop_waiting(&fixture, &holder, &raise).await;
    }
    for due in fixture
        .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 65)
        .await
    {
        apply_due(&fixture, &due).await;
    }
    let replacement = ApprovalHolderScope {
        organization_id: holder.organization_id.clone(),
        run: ApprovalRun {
            router_run: router_run(0xa133),
            ..holder.run.clone()
        },
    };
    let (replaced, _) = fixture
        .apply(None, async |store| {
            store
                .register_approval_run(&ApprovalHolderAdmission::for_fixture_scope(&replacement))
                .await
        })
        .await;
    replaced.unwrap();
    let due = due_work(&fixture, "retired-run:").await;

    assert_due_rollback(&fixture, &due, 4).await;

    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await
            .len(),
        1
    );
    let next = due_work(&fixture, "retired-run:").await;
    assert_eq!(next.key, due.key);
    assert_eq!(next.occurrence_ms, due.occurrence_ms);
    assert!(next.effective_due_ms > due.effective_due_ms);
}

#[tokio::test]
async fn enrollment_cleanup_rolls_back_parking_while_retired_markers_remain() {
    let (fixture, holder, _) = fixture_with_erased_marker().await;
    end_enrollment(&fixture, &holder).await;
    let due = due_work(&fixture, "ended-enrollment:").await;

    assert_due_rollback(&fixture, &due, 3).await;

    assert_eq!(fixture.inspect_rows("SELECT router_run_id FROM approval_router_runs WHERE enrollment_cleanup_due_ms IS NULL").await, [holder.run.router_run.as_str()]);
    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await
            .len(),
        1
    );
    assert!(
        !fixture
            .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 2)
            .await
            .iter()
            .any(|item| item.key == due.key)
    );
}

#[tokio::test]
async fn enrollment_cleanup_rolls_back_deleting_a_full_page_and_rescheduling_the_rest() {
    let (fixture, first, _) = fixture_with_owner().await;
    let mut latest = first.clone();
    for run_number in 1..=64 {
        latest.run.router_run = router_run(0xb000 + run_number);
        let admission = ApprovalHolderAdmission::for_fixture_scope(&latest);
        let (registered, _) = fixture
            .apply(None, async |store| {
                store.register_approval_run(&admission).await
            })
            .await;
        assert_eq!(
            registered.unwrap().reply,
            approval_request_protocol::RegisterRunReply::Registered {}
        );
    }
    end_enrollment(&fixture, &latest).await;
    let due = due_work(&fixture, "ended-enrollment:").await;

    assert_due_rollback(&fixture, &due, 4).await;

    assert_eq!(
        fixture
            .inspect_rows(
                "SELECT router_run_id FROM approval_router_runs ORDER BY registration_order"
            )
            .await,
        [router_run(0xb03f).as_str(), latest.run.router_run.as_str()]
    );
    let next = due_work(&fixture, "ended-enrollment:").await;
    assert_eq!(next.key, due.key);
    assert_eq!(next.occurrence_ms, due.occurrence_ms);
    assert!(next.effective_due_ms > due.effective_due_ms);
}

#[tokio::test]
async fn enrollment_cleanup_rolls_back_deleting_the_last_run() {
    let (fixture, holder, _) = fixture_with_owner().await;
    end_enrollment(&fixture, &holder).await;
    let due = due_work(&fixture, "ended-enrollment:").await;

    assert_due_rollback(&fixture, &due, 4).await;

    assert!(
        fixture
            .inspect_rows("SELECT router_run_id FROM approval_router_runs")
            .await
            .is_empty()
    );
    assert!(
        fixture
            .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 1)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn the_last_marker_page_rolls_back_waking_parked_enrollment_cleanup() {
    let (fixture, holder, _) = fixture_with_erased_marker().await;
    end_enrollment(&fixture, &holder).await;
    let enrollment_due = due_work(&fixture, "ended-enrollment:").await;
    apply_due(&fixture, &enrollment_due).await;
    let marker_due = due_work(&fixture, "retired-run:").await;

    assert_due_rollback(&fixture, &marker_due, 7).await;

    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await
            .is_empty()
    );
    let next = due_work(&fixture, "ended-enrollment:").await;
    assert_eq!(next.key, enrollment_due.key);
    assert_eq!(next.occurrence_ms, enrollment_due.occurrence_ms);
    assert!(next.effective_due_ms > marker_due.effective_due_ms);
}
