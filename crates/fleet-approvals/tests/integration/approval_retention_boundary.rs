use approval_request_protocol::{Raise, RaiseReply, StopReply};
use central_engine_model::{CENTRAL_RECORD_RETENTION_MILLIS, LAST_ALARM_GRID_UNIX_MILLIS};
use central_storage::{CorruptReason, StoreFailure};
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalHolderAdmission, ApprovalHolderCommand, ApprovalHolderReply,
    ApprovalHolderScope, ApprovalRun,
};

use super::{
    approval_store_cases::{fixture_with_owner, raise_and_commit, register, snapshot},
    fixtures::{enrollment, key, provider, provider_stop, request_id, router_run, session, time},
    native_approval_store::{ErasedMarkerReadFault, NativeApprovalStoreFixture},
};

#[tokio::test]
async fn admitted_endings_saturate_retention_at_the_last_grid_mark() {
    let boundary = LAST_ALARM_GRID_UNIX_MILLIS - CENTRAL_RECORD_RETENTION_MILLIS;
    let (fixture, holder, requester) = fixture_with_owner().await;
    fixture.clock.set_millis(boundary - 2);
    let mut requests = Vec::new();
    for ordinal in 1..=3 {
        let request = provider(&key(&holder.run, ordinal), &requester, "boundary");
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &request).await.0,
            RaiseReply::Receipt { .. }
        ));
        requests.push(request);
    }
    for (request, ended) in requests.iter().zip([boundary - 1, boundary, boundary + 1]) {
        fixture.clock.set_millis(ended);
        let (reply, _) = fixture
            .apply(None, async |store| {
                store
                    .apply_holder_command(
                        &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                        ApprovalHolderCommand::Stop(provider_stop(request)),
                    )
                    .await
            })
            .await;
        assert!(matches!(
            reply.unwrap().reply,
            fleet_approvals::ApprovalHolderReply::Stopped(StopReply::Stands { .. })
        ));
    }
    let stored = fixture
        .inspect_rows(
            "SELECT CAST(erase_after_ms AS TEXT) FROM approval_requests ORDER BY request_id",
        )
        .await;
    assert_eq!(
        stored,
        [
            LAST_ALARM_GRID_UNIX_MILLIS - 1,
            LAST_ALARM_GRID_UNIX_MILLIS,
            LAST_ALARM_GRID_UNIX_MILLIS
        ]
        .map(|time| time.to_string())
    );
    assert!(
        fixture
            .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS - 1), 10)
            .await
            .is_empty()
    );
    let work = fixture
        .due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 10)
        .await;
    assert_eq!(work.len(), 3);
    assert!(
        central_engine_model::next_five_minute_mark(time(LAST_ALARM_GRID_UNIX_MILLIS)).is_err()
    );
    assert!(
        work.iter()
            .all(|row| row.effective_due_ms == LAST_ALARM_GRID_UNIX_MILLIS)
    );
}

#[tokio::test]
async fn an_erased_marker_for_another_live_holder_is_not_visible() {
    let (fixture, holder, request) = fixture_with_erased_marker().await;
    let other_holder = ApprovalHolderScope {
        organization_id: holder.organization_id.clone(),
        run: ApprovalRun {
            enrollment: enrollment(0xa191, 0xa192),
            router_run: router_run(0xa193),
        },
    };
    register(&fixture, &other_holder).await;
    let other_session = session(&other_holder.run.enrollment, 0xa194);
    fixture
        .store_session(&other_holder.organization_id, &other_session, None, true)
        .await;
    let attempt = provider(
        &key(&other_holder.run, 1),
        &other_session,
        "Same request ID",
    );
    assert_eq!(attempt.request_id(), request.request_id());
    let before = snapshot(&fixture).await;

    let (reply, evidence) = raise_and_commit(&fixture, &other_holder, &attempt).await;

    assert_eq!(reply, RaiseReply::ContentsDiffer {});
    assert!(evidence.sql_writes.is_empty());
    assert_eq!(snapshot(&fixture).await, before);
}

#[tokio::test]
async fn an_erased_marker_with_a_different_request_id_is_corrupt() {
    let reply =
        raise_with_one_erased_marker_cell_changed("request_id", request_id(2).to_string()).await;

    assert!(
        matches!(
            reply,
            Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt {
                reason: CorruptReason::BadValue { ref column },
            })) if column == "approval_erased_requests.request_id"
        ),
        "{reply:?}"
    );
}

#[tokio::test]
async fn an_erased_marker_with_a_different_run_belongs_to_another_holder() {
    let reply =
        raise_with_one_erased_marker_cell_changed("router_run_id", router_run(0xa195).to_string())
            .await;

    assert_eq!(
        reply.unwrap(),
        ApprovalHolderReply::Raised(RaiseReply::ContentsDiffer {})
    );
}

#[tokio::test]
async fn an_erased_marker_with_a_different_machine_belongs_to_another_holder() {
    let reply = raise_with_one_erased_marker_cell_changed(
        "machine_id",
        enrollment(0xa196, 0xa197).machine_id.to_string(),
    )
    .await;

    assert_eq!(
        reply.unwrap(),
        ApprovalHolderReply::Raised(RaiseReply::ContentsDiffer {})
    );
}

#[tokio::test]
async fn an_erased_marker_with_a_different_enrollment_belongs_to_another_holder() {
    let reply = raise_with_one_erased_marker_cell_changed(
        "enrollment_id",
        enrollment(0xa196, 0xa197).enrollment_id.as_str().to_owned(),
    )
    .await;

    assert_eq!(
        reply.unwrap(),
        ApprovalHolderReply::Raised(RaiseReply::ContentsDiffer {})
    );
}

async fn raise_with_one_erased_marker_cell_changed(
    column: &'static str,
    replacement: String,
) -> Result<ApprovalHolderReply, ApprovalApplyFailure> {
    let (fixture, holder, request) = fixture_with_erased_marker().await;
    let before = snapshot(&fixture).await;
    let fault = ErasedMarkerReadFault {
        column,
        replacement,
    };

    let (reply, evidence) = fixture
        .apply_with_erased_marker_read_fault(fault, async |store| {
            store
                .apply_holder_command(
                    &ApprovalHolderAdmission::for_fixture_scope(&holder),
                    ApprovalHolderCommand::Raise(Box::new(request)),
                )
                .await
        })
        .await;

    assert!(evidence.sql_writes.is_empty());
    assert_eq!(snapshot(&fixture).await, before);
    reply.map(|committed| committed.reply)
}

pub(super) async fn fixture_with_erased_marker()
-> (NativeApprovalStoreFixture, ApprovalHolderScope, Raise) {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = provider(&key(&holder.run, 1), &requester, "Retained then erased");
    let (raised, _) = raise_and_commit(&fixture, &holder, &request).await;
    assert!(matches!(raised, RaiseReply::Receipt { .. }));
    let (stopped, _) = fixture
        .apply(None, async |store| {
            store
                .stop_from_holder(
                    &ApprovalHolderAdmission::for_fixture_scope(&holder),
                    provider_stop(&request),
                )
                .await
        })
        .await;
    assert!(
        matches!(&stopped, Ok(committed) if matches!(committed.reply, StopReply::Stands { .. })),
        "ending must be retained before erasure: {stopped:?}"
    );

    let due = fixture.due_rows(time(LAST_ALARM_GRID_UNIX_MILLIS), 2).await;
    let [erasure] = due.as_slice() else {
        panic!("one ended request must have one erasure due, got {due:?}");
    };
    fixture.clock.set_millis(erasure.effective_due_ms);
    let (erased, _) = fixture
        .apply(None, async |store| {
            store
                .apply_approval_due_work(&erasure.key, time(erasure.occurrence_ms))
                .await
        })
        .await;
    assert!(erased.is_ok(), "due work must erase the ending: {erased:?}");
    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_erased_requests")
            .await,
        [request.request_id().as_str()]
    );
    assert!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_requests")
            .await
            .is_empty()
    );
    let (replayed, evidence) = raise_and_commit(&fixture, &holder, &request).await;
    assert!(matches!(replayed, RaiseReply::AlreadyEnded { .. }));
    assert!(evidence.sql_writes.is_empty());
    (fixture, holder, request)
}
