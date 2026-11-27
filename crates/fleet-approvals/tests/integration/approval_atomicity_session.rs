use approval_request_protocol::{
    ApprovalDestination, ApprovalRequestId, DecideApprovalAsSessionReply,
    DecideApprovalAsSessionRequest, PassOnApprovalReply, PassOnApprovalRequest,
    ReadApprovalForSessionReply,
};
use fleet_actor_identity::AuthenticatedSession;
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyFailure, ApprovalApplyingStore, ApprovalCommitProof,
    ApprovalNoticeRecovery, PendingApprovalEffects, PendingApprovalReply, SessionDecisionApply,
    SessionReadApply, SettledApprovalMacCall,
};

use super::{
    approval_store_atomicity::assert_loan_rollback_each_write,
    fixtures::{HOUR, answer, operation_id},
    native_approval_store::{ApprovalTransactionEvidence, NativeApprovalTransaction},
    native_session_approval_fixture::NativeSessionApprovalFixture,
};

type SessionApprovalStore<'loan> =
    ApprovalApplyingStore<'loan, NativeApprovalTransaction, ActorApprovalSessionFacts>;

async fn apply_session<Reply>(
    fixture: &NativeSessionApprovalFixture,
    fail_after: Option<usize>,
    apply: impl AsyncFnOnce(
        &mut SessionApprovalStore<'_>,
        &AuthenticatedSession,
    ) -> Result<Reply, ApprovalApplyFailure>,
) -> (
    Result<(ApprovalCommitProof, (Reply, PendingApprovalEffects)), ApprovalApplyFailure>,
    ApprovalTransactionEvidence,
) {
    fixture
        .store
        .apply_on_loan(fail_after, async |loan, commit, ids| {
            let session = fixture.authenticate(loan).await?;
            let mut store = ApprovalApplyingStore::new(
                &commit,
                loan,
                &ActorApprovalSessionFacts,
                &fixture.store.clock,
                ids,
            );
            let reply = apply(&mut store, &session).await?;
            Ok((reply, store.finish_effects()))
        })
        .await
}

async fn assert_session_rollback<Reply>(
    fixture: &NativeSessionApprovalFixture,
    writes: usize,
    apply: impl AsyncFn(
        &mut SessionApprovalStore<'_>,
        &AuthenticatedSession,
    ) -> Result<PendingApprovalReply<Reply>, ApprovalApplyFailure>,
) -> (Reply, usize) {
    let (proof, (reply, effects)) =
        assert_loan_rollback_each_write(&fixture.store, writes, async |ordinal| {
            apply_session(fixture, ordinal, &apply).await
        })
        .await;
    (
        reply.take_fixture_reply(&proof).unwrap(),
        effects.release(&proof).unwrap().endings.len(),
    )
}

fn decision(request_id: &ApprovalRequestId) -> DecideApprovalAsSessionRequest {
    DecideApprovalAsSessionRequest {
        request_id: request_id.clone(),
        operation_id: operation_id(101),
        value: answer(1),
    }
}

async fn decide(
    store: &mut SessionApprovalStore<'_>,
    admitted: &AuthenticatedSession,
    input: DecideApprovalAsSessionRequest,
) -> Result<PendingApprovalReply<DecideApprovalAsSessionReply>, ApprovalApplyFailure> {
    let SessionDecisionApply::Reply(reply) = store.decide_as_session(admitted, input).await? else {
        panic!("this answer must be eligible for an authenticated session");
    };
    Ok(reply)
}

#[tokio::test]
async fn a_session_decision_rolls_back_its_ending_and_retained_result() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let request = fixture.raise(1).await;
    let input = decision(&request);

    let (reply, offers) = assert_session_rollback(&fixture, 6, async |store, admitted| {
        decide(store, admitted, input.clone()).await
    })
    .await;

    assert!(matches!(
        reply,
        DecideApprovalAsSessionReply::SavedDecision { .. }
    ));
    assert_eq!(offers, 1);
    assert_eq!(
        fixture.store.inspect_rows(
            "SELECT request.request_id FROM runtime_request_log AS retained
             JOIN approval_requests AS request ON request.approval_row_id = retained.approval_row_id",
        ).await,
        [request.as_str()],
    );
    let (replayed, evidence) = apply_session(&fixture, None, async |store, admitted| {
        decide(store, admitted, input.clone()).await
    })
    .await;
    let (proof, (replayed, effects)) = replayed.unwrap();
    assert_eq!(replayed.take_fixture_reply(&proof).unwrap(), reply);
    assert!(effects.release(&proof).unwrap().is_empty());
    assert!(evidence.sql_writes.is_empty());
}

#[tokio::test]
async fn a_fresh_late_session_decision_rolls_back_its_timeout_and_reply() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let input = decision(&fixture.raise(1).await);
    fixture.store.clock.set_millis(HOUR);

    let (reply, offers) = assert_session_rollback(&fixture, 6, async |store, admitted| {
        decide(store, admitted, input.clone()).await
    })
    .await;

    assert_eq!(reply, DecideApprovalAsSessionReply::TooLate {});
    assert_eq!(offers, 1);
}

#[tokio::test]
async fn a_conflicting_session_decision_rolls_back_expiring_the_selected_request() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let first = decision(&fixture.raise(1).await);
    let (saved, _) = apply_session(&fixture, None, async |store, admitted| {
        decide(store, admitted, first.clone()).await
    })
    .await;
    let (proof, (reply, _)) = saved.unwrap();
    assert!(matches!(
        reply.take_fixture_reply(&proof).unwrap(),
        DecideApprovalAsSessionReply::SavedDecision { .. }
    ));
    let second = decision(&fixture.raise(2).await);
    fixture.store.clock.set_millis(HOUR);

    let (reply, offers) = assert_session_rollback(&fixture, 3, async |store, admitted| {
        decide(store, admitted, second.clone()).await
    })
    .await;

    assert_eq!(reply, DecideApprovalAsSessionReply::OperationConflict {});
    assert_eq!(offers, 1);
    assert_eq!(
        fixture
            .store
            .inspect_rows("SELECT request_id FROM runtime_request_log")
            .await
            .len(),
        1
    );
}

fn forward(request_id: &ApprovalRequestId) -> PassOnApprovalRequest {
    PassOnApprovalRequest {
        request_id: request_id.clone(),
        operation_id: operation_id(102),
    }
}

#[tokio::test]
async fn passing_on_rolls_back_the_destination_and_retained_result() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let input = forward(&fixture.raise(1).await);

    let (reply, offers) = assert_session_rollback(&fixture, 5, async |store, admitted| {
        store.pass_on_as_session(admitted, input.clone()).await
    })
    .await;

    assert_eq!(
        reply,
        PassOnApprovalReply::PassedOn {
            to: ApprovalDestination::People {}
        }
    );
    assert_eq!(offers, 0);
    assert_eq!(
        fixture
            .store
            .inspect_rows(
                "SELECT destination_kind FROM approval_request_destinations ORDER BY position"
            )
            .await,
        ["parent_session", "people"],
    );
}

#[tokio::test]
async fn a_fresh_late_pass_on_rolls_back_its_timeout_and_reply() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let input = forward(&fixture.raise(1).await);
    fixture.store.clock.set_millis(HOUR);

    let (reply, offers) = assert_session_rollback(&fixture, 6, async |store, admitted| {
        store.pass_on_as_session(admitted, input.clone()).await
    })
    .await;

    assert_eq!(reply, PassOnApprovalReply::TooLate {});
    assert_eq!(offers, 1);
}

#[tokio::test]
async fn a_pass_on_replay_rolls_back_expiring_its_waiting_request() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let input = forward(&fixture.raise(1).await);
    let apply = async |store: &mut SessionApprovalStore<'_>, admitted: &AuthenticatedSession| {
        store.pass_on_as_session(admitted, input.clone()).await
    };
    let (saved, _) = apply_session(&fixture, None, &apply).await;
    let (proof, (reply, _)) = saved.unwrap();
    let original = reply.take_fixture_reply(&proof).unwrap();
    assert!(matches!(original, PassOnApprovalReply::PassedOn { .. }));
    let (unchanged, evidence) = apply_session(&fixture, None, &apply).await;
    let (proof, (reply, effects)) = unchanged.unwrap();
    assert_eq!(reply.take_fixture_reply(&proof).unwrap(), original);
    assert!(effects.release(&proof).unwrap().is_empty());
    assert!(evidence.sql_writes.is_empty());
    fixture.store.clock.set_millis(HOUR);

    let (reply, offers) = assert_session_rollback(&fixture, 3, apply).await;

    assert_eq!(reply, original);
    assert_eq!(offers, 1);
    assert_eq!(
        fixture
            .store
            .inspect_rows("SELECT request_id FROM runtime_request_log")
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn the_first_session_read_rolls_back_settling_its_notice() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let request = fixture.raise(1).await;
    fixture.store.clock.set_millis(100);
    let read = async |store: &mut SessionApprovalStore<'_>, admitted: &AuthenticatedSession| {
        store.read_for_session(admitted, &request).await
    };

    let (proof, (reply, effects)) =
        assert_loan_rollback_each_write(&fixture.store, 1, async |ordinal| {
            apply_session(&fixture, ordinal, &read).await
        })
        .await;

    let SessionReadApply::Applied(reply) = reply else {
        panic!("the first read must settle its pending notice");
    };
    assert!(matches!(
        reply.take_fixture_reply(&proof).unwrap(),
        ReadApprovalForSessionReply::Request { .. }
    ));
    let effects = effects.release(&proof).unwrap();
    assert!(matches!(
        effects.settled_mac_calls.as_slice(),
        [SettledApprovalMacCall::Notice { request_id, .. }] if request_id == &request
    ));
    assert_eq!(fixture.store.inspect_rows("SELECT request_id FROM approval_request_destinations WHERE first_read_at IS NOT NULL").await, [request.as_str()]);
    let (repeated, evidence) = apply_session(&fixture, None, read).await;
    let (proof, (reply, effects)) = repeated.unwrap();
    assert!(matches!(reply, SessionReadApply::Unchanged(_)));
    assert!(effects.release(&proof).unwrap().is_empty());
    assert!(evidence.sql_writes.is_empty());
}

#[tokio::test]
async fn notice_recovery_rolls_back_a_timeout_without_delivering_the_old_notice() {
    let fixture = NativeSessionApprovalFixture::open().await;
    let request = fixture.raise(1).await;
    fixture.store.clock.set_millis(HOUR);

    let (proof, (page, effects)) =
        assert_loan_rollback_each_write(&fixture.store, 3, async |ordinal| {
            fixture
                .store
                .apply_on_loan(ordinal, async |loan, commit, ids| {
                    let mut store = ApprovalApplyingStore::new(
                        &commit,
                        loan,
                        &ActorApprovalSessionFacts,
                        &fixture.store.clock,
                        ids,
                    );
                    let page = store
                        .recover_machine_notices(&fixture.machine, None)
                        .await?;
                    Ok((page, store.finish_effects()))
                })
                .await
        })
        .await;

    assert!(matches!(page, ApprovalNoticeRecovery::Page { next: None }));
    let effects = effects.release(&proof).unwrap();
    assert!(effects.notices.is_empty());
    assert_eq!(effects.endings.len(), 1);
    assert_eq!(
        fixture
            .store
            .inspect_rows("SELECT request_id FROM approval_requests WHERE delivery_state = 'owed'")
            .await,
        [request.as_str()],
    );
}
