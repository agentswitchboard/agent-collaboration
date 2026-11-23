use approval_request_protocol::RaiseReply;
use central_storage::StoreFailure;
use fleet_approvals::{ApprovalApplyFailure, ApprovalDecisionReply, FactorStanding};

use super::{
    approval_store_cases::{decide_input, fixture_with_owner, raise_and_commit, snapshot},
    fixtures::{HOUR, key, person, provider},
};

const REQUEST_LOG_WINDOW_MS: i64 = 24 * HOUR;

#[tokio::test]
async fn admitted_refusal_and_acceptance_replay_their_first_answers_until_the_strict_cutoff() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = provider(&key(&holder.run, 1), &requester, "Retention window");
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &request).await.0,
        RaiseReply::Receipt { .. }
    ));
    fixture.clock.set_millis(1);
    let retry = decide_input(request.request_id(), 101, 3);
    let allowed = decide_input(request.request_id(), 102, 2);
    let stale_actor = person(11, FactorStanding::AbsentOrStale);
    let live_actor = person(11, FactorStanding::Fresh);

    let invalid = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, stale_actor.clone(), retry.clone())
                .await
        })
        .await;
    assert_eq!(invalid.0.unwrap().reply, ApprovalDecisionReply::Invalid {});
    assert_eq!(invalid.1.sql_writes.len(), 2);
    let fresh_retry = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, live_actor.clone(), retry.clone())
                .await
        })
        .await;
    assert_eq!(
        fresh_retry.0.unwrap().reply,
        ApprovalDecisionReply::Invalid {}
    );
    assert!(fresh_retry.1.sql_writes.is_empty());

    let accepted = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, live_actor.clone(), allowed.clone())
                .await
        })
        .await;
    let accepted = accepted.0.unwrap().reply;
    assert!(matches!(
        accepted,
        ApprovalDecisionReply::SavedDecision { .. }
    ));

    fixture.clock.set_millis(REQUEST_LOG_WINDOW_MS);
    let stored_before_expiry = snapshot(&fixture).await;
    for (input, desired) in [
        (retry.clone(), ApprovalDecisionReply::Invalid {}),
        (allowed.clone(), accepted),
    ] {
        let (answer, evidence) = fixture
            .apply(None, async |store| {
                store
                    .decide_approval(&holder.organization_id, live_actor.clone(), input)
                    .await
            })
            .await;
        assert_eq!(answer.unwrap().reply, desired);
        assert!(evidence.sql_writes.is_empty());
    }
    assert_eq!(snapshot(&fixture).await, stored_before_expiry);

    fixture.clock.set_millis(REQUEST_LOG_WINDOW_MS + 1);
    let (first_at_cutoff, evidence) = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, live_actor.clone(), retry.clone())
                .await
        })
        .await;
    let first_at_cutoff = first_at_cutoff.unwrap().reply;
    assert!(matches!(
        first_at_cutoff,
        ApprovalDecisionReply::Standing { .. }
    ));
    assert_eq!(evidence.sql_writes.len(), 2);
    let retained_standing = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, live_actor.clone(), retry)
                .await
        })
        .await;
    assert_eq!(retained_standing.0.unwrap().reply, first_at_cutoff);
    assert!(retained_standing.1.sql_writes.is_empty());

    let (expired_acceptance, evidence) = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, live_actor.clone(), allowed)
                .await
        })
        .await;
    assert!(matches!(
        expired_acceptance.unwrap().reply,
        ApprovalDecisionReply::Standing { .. }
    ));
    assert_eq!(evidence.sql_writes.len(), 2);
    assert_eq!(
        fixture
            .inspect_rows("SELECT decided_operation_id FROM approval_requests")
            .await,
        ["019ac19f-aa00-7000-8000-000000000066"],
    );
}

#[tokio::test]
async fn shared_request_log_conflicts_and_expired_operation_id_can_be_new_provenance() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let first = provider(&key(&holder.run, 1), &requester, "First");
    raise_and_commit(&fixture, &holder, &first).await;
    let actor = person(11, FactorStanding::Fresh);
    fixture
        .alter_fixture(
            "INSERT INTO runtime_request_log VALUES (
            '019ac19f-aa00-7000-8000-000000000065', '019ac19f-aa00-7000-8000-00000000000b',
            'the digest of another Central operation', '{\"kind\":\"unrelated\"}',
            '1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00+00:00')",
        )
        .await;
    let original = snapshot(&fixture).await;
    let collided = fixture
        .apply(None, async |store| {
            store
                .decide_approval(
                    &holder.organization_id,
                    actor.clone(),
                    decide_input(first.request_id(), 101, 1),
                )
                .await
        })
        .await;
    assert_eq!(
        collided.0.unwrap().reply,
        ApprovalDecisionReply::OperationConflict {}
    );
    assert!(collided.1.sql_writes.is_empty());
    assert_eq!(snapshot(&fixture).await, original);

    let first_acceptance = fixture
        .apply(None, async |store| {
            store
                .decide_approval(
                    &holder.organization_id,
                    actor.clone(),
                    decide_input(first.request_id(), 102, 1),
                )
                .await
        })
        .await;
    assert!(matches!(
        first_acceptance.0.unwrap().reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ));

    fixture.clock.set_millis(REQUEST_LOG_WINDOW_MS);
    let second = provider(&key(&holder.run, 2), &requester, "Second after the cutoff");
    raise_and_commit(&fixture, &holder, &second).await;
    let second_acceptance = fixture
        .apply(None, async |store| {
            store
                .decide_approval(
                    &holder.organization_id,
                    actor.clone(),
                    decide_input(second.request_id(), 102, 2),
                )
                .await
        })
        .await;
    assert!(matches!(
        second_acceptance.0.unwrap().reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ));
    assert_eq!(
        fixture
            .inspect_rows("SELECT decided_operation_id FROM approval_requests ORDER BY request_id")
            .await,
        ["019ac19f-aa00-7000-8000-000000000066"; 2],
    );
    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM runtime_request_log")
            .await,
        ["019ac19f-aa00-7000-8000-000000000066"],
    );
}

#[tokio::test]
async fn a_tampered_retained_answer_cannot_release_a_different_giver() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = provider(&key(&holder.run, 1), &requester, "Bound retained answer");
    raise_and_commit(&fixture, &holder, &request).await;
    let actor = person(11, FactorStanding::Fresh);
    let command = decide_input(request.request_id(), 101, 1);
    let accepted = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, actor.clone(), command.clone())
                .await
        })
        .await;
    assert!(matches!(
        accepted.0.unwrap().reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ));

    fixture
        .alter_fixture(
            "UPDATE runtime_request_log SET answer=json_set(answer,'$.by.personId',
         '019ac19f-aa00-7000-8000-00000000000c')",
        )
        .await;
    let (replay, evidence) = fixture
        .apply(None, async |store| {
            store
                .decide_approval(&holder.organization_id, actor, command)
                .await
        })
        .await;
    assert!(matches!(
        replay,
        Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt { .. }))
    ));
    assert!(evidence.sql_writes.is_empty());
}
