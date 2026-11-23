use std::num::NonZeroU16;

use approval_request_protocol::{Ending, Raise, RaiseReply, RegisterRunReply};
use central_storage::StoreFailure;
use fleet_approvals::{
    AdmittedDecider, ApprovalApplyFailure, ApprovalDecideInput, ApprovalDecisionReply,
    ApprovalHolderCommand, ApprovalHolderReply, ApprovalHolderScope, ApprovalRun, ApprovalSession,
};
use fleet_identity_protocol::OrganizationId;
use workspace_ids::ApprovalRequestId;

use super::{
    fixtures::{
        HOUR, answer, enrollment, key, operation_id, person, person_id, provider, router_run,
        session, time,
    },
    native_approval_store::{ApprovalTransactionEvidence, NativeApprovalStoreFixture},
};

pub(super) fn first_holder() -> ApprovalHolderScope {
    ApprovalHolderScope {
        organization_id: OrganizationId::try_from(
            "019ac19f-aa00-7000-8000-00000000a100".to_owned(),
        )
        .unwrap(),
        run: ApprovalRun {
            enrollment: enrollment(0xa101, 0xa102),
            router_run: router_run(0xa103),
        },
    }
}

pub(super) async fn register(fixture: &NativeApprovalStoreFixture, holder: &ApprovalHolderScope) {
    fixture.store_enrollment(holder, "live").await;
    let (outcome, evidence) = fixture
        .apply(None, async |store| {
            store
                .register_approval_run(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(holder),
                )
                .await
        })
        .await;
    assert_eq!(outcome.unwrap().reply, RegisterRunReply::Registered {});
    assert_eq!(evidence.sql_writes.len(), 1);
}

pub(super) async fn fixture_with_owner() -> (
    NativeApprovalStoreFixture,
    ApprovalHolderScope,
    ApprovalSession,
) {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    let requester = session(&holder.run.enrollment, 0xa104);
    register(&fixture, &holder).await;
    fixture
        .store_session(&holder.organization_id, &requester, None, true)
        .await;
    (fixture, holder, requester)
}

pub(super) fn decide_input(
    request_id: &ApprovalRequestId,
    operation: u16,
    choice: u8,
) -> ApprovalDecideInput {
    ApprovalDecideInput {
        request_id: request_id.clone(),
        operation_id: operation_id(operation),
        value: answer(choice),
    }
}

pub(super) async fn raise_and_commit(
    fixture: &NativeApprovalStoreFixture,
    holder: &ApprovalHolderScope,
    raise: &Raise,
) -> (RaiseReply, ApprovalTransactionEvidence) {
    let (outcome, evidence) = fixture
        .apply(None, async |store| {
            store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(holder),
                    ApprovalHolderCommand::Raise(Box::new(raise.clone())),
                )
                .await
        })
        .await;
    let ApprovalHolderReply::Raised(reply) = outcome.unwrap().reply else {
        panic!("the holder command must return a Raise reply");
    };
    (reply, evidence)
}

#[tokio::test]
async fn reopened_store_preserves_the_first_receipt_and_fences_other_holders_and_contents() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = key(&holder.run, 1);
    let first = provider(&request, &requester, "One frozen offer");
    let (first_reply, first_evidence) = raise_and_commit(&fixture, &holder, &first).await;
    assert!(matches!(first_reply, RaiseReply::Receipt { .. }));
    assert_eq!(
        first_evidence
            .sql_writes
            .iter()
            .map(|(_, count)| count)
            .sum::<u64>(),
        3
    );
    assert_eq!(
        first_evidence.session_route_reads,
        std::slice::from_ref(&requester)
    );

    fixture.clock.set_millis(1);
    let before = snapshot(&fixture).await;
    let (replay, evidence) = raise_and_commit(&fixture, &holder, &first).await;
    assert_eq!(replay, first_reply);
    assert!(evidence.sql_writes.is_empty() && evidence.session_route_reads.is_empty());

    let changed = provider(&request, &requester, "Changed offer");
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &changed).await;
    assert_eq!(reply, RaiseReply::ContentsDiffer {});
    assert!(evidence.sql_writes.is_empty());
    assert_eq!(snapshot(&fixture).await, before);

    let other_holder = ApprovalHolderScope {
        organization_id: holder.organization_id.clone(),
        run: ApprovalRun {
            enrollment: enrollment(0xa111, 0xa112),
            router_run: router_run(0xa113),
        },
    };
    register(&fixture, &other_holder).await;
    let other_session = session(&other_holder.run.enrollment, 0xa114);
    fixture
        .store_session(&holder.organization_id, &other_session, None, true)
        .await;
    let cross_holder = provider(&key(&other_holder.run, 1), &other_session, "Other holder");
    let before = snapshot(&fixture).await;
    let (reply, evidence) = raise_and_commit(&fixture, &other_holder, &cross_holder).await;
    assert_eq!(reply, RaiseReply::ContentsDiffer {});
    assert!(evidence.sql_writes.is_empty() && evidence.session_route_reads.is_empty());
    assert_eq!(snapshot(&fixture).await, before);

    fixture
        .alter_fixture("UPDATE approval_requests SET request_kind='future_kind'")
        .await;
    let (reply, evidence) = raise_and_commit(&fixture, &other_holder, &cross_holder).await;
    assert_eq!(reply, RaiseReply::ContentsDiffer {});
    assert!(evidence.sql_writes.is_empty() && evidence.session_route_reads.is_empty());
}

#[tokio::test]
async fn public_raise_installs_the_recorded_parent_and_unknown_session_writes_nothing() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    register(&fixture, &holder).await;
    let child = session(&holder.run.enrollment, 0xa105);
    let parent = session(&enrollment(0xa106, 0xa107), 0xa108);
    let unrelated = session(&parent.enrollment, 0xa109);
    let unknown = session(&holder.run.enrollment, 0xa110);
    fixture
        .store_session(&holder.organization_id, &parent, None, true)
        .await;
    fixture
        .store_session(&holder.organization_id, &unrelated, None, true)
        .await;
    fixture
        .store_session(&holder.organization_id, &child, Some(&parent), true)
        .await;

    let wrong_session = provider(&key(&holder.run, 1), &unknown, "Unknown");
    let before = snapshot(&fixture).await;
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &wrong_session).await;
    assert_eq!(reply, RaiseReply::SessionEnded {});
    assert!(evidence.sql_writes.is_empty());
    assert_eq!(evidence.session_route_reads, [unknown]);
    assert_eq!(snapshot(&fixture).await, before);

    let request = key(&holder.run, 1);
    let real = provider(&request, &child, "Delegate");
    let (reply, evidence) = raise_and_commit(&fixture, &holder, &real).await;
    assert!(matches!(reply, RaiseReply::Receipt { .. }));
    assert_eq!(evidence.session_route_reads, std::slice::from_ref(&child));
    assert_eq!(evidence.session_liveness_reads, [child, parent.clone()]);
    assert_eq!(
        fixture.inspect_rows("SELECT session_id FROM approval_request_destinations WHERE raised_notice_at IS NOT NULL").await,
        [parent.session_id.as_str()],
    );

    let refused = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    AdmittedDecider::Session(unrelated),
                    decide_input(&request.request_id, 101, 2),
                )
                .await
        })
        .await;
    assert_eq!(
        refused.0.unwrap().reply,
        ApprovalDecisionReply::NotVisible {}
    );
    assert!(refused.1.sql_writes.is_empty());

    let accepted = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    AdmittedDecider::Session(parent),
                    decide_input(&request.request_id, 101, 2),
                )
                .await
        })
        .await;
    assert!(matches!(
        accepted.0.unwrap().reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ));
    assert_eq!(
        accepted
            .1
            .sql_writes
            .iter()
            .map(|(_, count)| count)
            .sum::<u64>(),
        3
    );
}

#[tokio::test]
async fn simultaneous_decisions_commit_once_and_the_accepted_binding_replays_after_reopen() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = key(&holder.run, 1);
    let another = key(&holder.run, 2);
    for key in [&request, &another] {
        let raise = provider(key, &requester, "Review");
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &raise).await.0,
            RaiseReply::Receipt { .. }
        ));
    }

    fixture.clock.set_millis(1);
    let first_actor = person(11);
    let second_actor = person(12);
    let first_input = decide_input(&request.request_id, 101, 1);
    let second_input = decide_input(&request.request_id, 102, 2);
    let invalid_input = decide_input(&request.request_id, 103, 15);
    let invalid = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    first_actor.clone(),
                    invalid_input.clone(),
                )
                .await
        })
        .await;
    assert_eq!(invalid.0.unwrap().reply, ApprovalDecisionReply::Invalid {});
    assert_eq!(invalid.1.sql_writes.len(), 2);

    let (first, second) = tokio::join!(
        fixture.apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    first_actor.clone(),
                    first_input.clone(),
                )
                .await
        }),
        fixture.apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    second_actor.clone(),
                    second_input.clone(),
                )
                .await
        }),
    );
    let (first_result, second_result) = (first.0.unwrap(), second.0.unwrap());
    let (winner, other_actor, saved_input, saved_reply) = if matches!(
        first_result.reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ) {
        assert!(matches!(
            second_result.reply,
            ApprovalDecisionReply::Standing { .. }
        ));
        assert_eq!(second.1.sql_writes.len(), 2);
        (
            first_actor.clone(),
            second_actor,
            first_input,
            first_result.reply,
        )
    } else {
        assert!(matches!(
            first_result.reply,
            ApprovalDecisionReply::Standing { .. }
        ));
        assert!(matches!(
            second_result.reply,
            ApprovalDecisionReply::SavedDecision { .. }
        ));
        assert_eq!(first.1.sql_writes.len(), 2);
        (
            second_actor,
            first_actor.clone(),
            second_input,
            second_result.reply,
        )
    };
    assert_eq!(first.1.sql_writes.len() + second.1.sql_writes.len(), 6);
    assert_eq!(
        fixture.inspect_rows("SELECT decided_operation_id FROM approval_requests WHERE decided_operation_id IS NOT NULL").await,
        [saved_input.operation_id.as_str()],
    );
    let retry = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    winner.clone(),
                    saved_input.clone(),
                )
                .await
        })
        .await;
    assert_eq!(retry.0.unwrap().reply, saved_reply);
    assert!(retry.1.sql_writes.is_empty());
    let changed_target = ApprovalDecideInput {
        request_id: another.request_id,
        ..saved_input.clone()
    };
    let changed_value = ApprovalDecideInput {
        value: answer(3),
        ..saved_input.clone()
    };
    for (actor, input) in [
        (winner.clone(), changed_target),
        (winner, changed_value),
        (other_actor, saved_input),
    ] {
        let conflict = fixture
            .apply(None, async |store| {
                store
                    .fixture_decide_approval(&holder.organization_id, actor, input)
                    .await
            })
            .await;
        assert_eq!(
            conflict.0.unwrap().reply,
            ApprovalDecisionReply::OperationConflict {}
        );
        assert!(conflict.1.sql_writes.is_empty());
    }
    let refused_again = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(&holder.organization_id, first_actor, invalid_input)
                .await
        })
        .await;
    assert_eq!(
        refused_again.0.unwrap().reply,
        ApprovalDecisionReply::Invalid {}
    );
    assert!(refused_again.1.sql_writes.is_empty());
}

#[tokio::test]
async fn a_page_times_out_only_its_rows_and_held_alarms_cannot_restore_waiting() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let expired_first = key(&holder.run, 1);
    let still_waiting = key(&holder.run, 2);
    let expired_after_page = key(&holder.run, 3);
    for (request, first_save) in [
        (&expired_first, 0),
        (&still_waiting, 1),
        (&expired_after_page, 0),
    ] {
        fixture.clock.set_millis(first_save);
        let raise = provider(request, &requester, "Page");
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &raise).await.0,
            RaiseReply::Receipt { .. }
        ));
    }

    fixture.clock.set_millis(HOUR);
    let due_before = fixture.due_rows(time(HOUR), 10).await;
    assert_eq!(due_before.len(), 2);
    let (page, evidence) = fixture
        .apply(None, async |store| {
            store
                .fixture_list_person_approvals(
                    &holder.organization_id,
                    &person_id(11),
                    None,
                    NonZeroU16::new(2).unwrap(),
                )
                .await
        })
        .await;
    let page = page.unwrap();
    assert_eq!(page.reply.requests.len(), 2);
    assert!(page.reply.next_after.is_some());
    assert_eq!(page.ending_offers.len(), 1);
    assert_eq!(
        page.ending_offers[0].request.request_id(),
        &expired_first.request_id
    );
    assert_eq!(evidence.sql_writes.len(), 2);
    let due_after = fixture.due_rows(time(HOUR), 10).await;
    assert_eq!(due_after, [due_before[1].clone()]);

    let (stale, evidence) = fixture
        .apply(None, async |store| {
            store
                .apply_approval_deadline(&due_after[0].key, time(HOUR + 1))
                .await
        })
        .await;
    assert!(stale.unwrap().ending_offers.is_empty() && evidence.sql_writes.is_empty());

    fixture.clock.set_millis(HOUR - 1);
    let (reply, evidence) = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    person(11),
                    decide_input(&expired_first.request_id, 101, 1),
                )
                .await
        })
        .await;
    assert_eq!(reply.unwrap().reply, ApprovalDecisionReply::TooLate {});
    assert_eq!(evidence.sql_writes.len(), 2);
    assert!(
        evidence
            .sql_writes
            .iter()
            .all(|(sql, _)| sql.contains("runtime_request_log"))
    );

    fixture.clock.set_millis(HOUR + 10);
    let (ending, _) = fixture
        .apply(None, async |store| {
            store
                .apply_approval_deadline(&due_after[0].key, time(HOUR))
                .await
        })
        .await;
    let offers = ending.unwrap().ending_offers;
    assert_eq!(
        offers[0].request.request_id(),
        &expired_after_page.request_id
    );
    assert_eq!(
        offers[0].request.as_ref().clone().into_ending(),
        Ending::TimedOut {}
    );
    assert!(fixture.due_rows(time(HOUR + 10), 10).await.is_empty());
    let next_mark = fixture.due_rows(time(HOUR + 300_000), 10).await;
    assert_eq!(next_mark.len(), 1);
    assert_eq!(
        next_mark.first().map(|row| row.occurrence_ms),
        Some(HOUR + 1)
    );
}

#[tokio::test]
async fn destination_request_key_is_copied_on_insert_and_checked_on_decode() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = key(&holder.run, 1);
    let raise = provider(&request, &requester, "Destination key");
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &raise).await.0,
        RaiseReply::Receipt { .. }
    ));
    assert_eq!(
        fixture
            .inspect_rows("SELECT request_id FROM approval_request_destinations")
            .await,
        [request.request_id.as_str()],
    );

    fixture
        .alter_fixture("UPDATE approval_request_destinations SET request_id='019ac19f-aa00-7000-8000-00000000c925'")
        .await;

    // The holder replay selects the request by its own Id and decodes the
    // destination via its physical row foreign key.
    let (replay, evidence) = fixture
        .apply(None, async |store| {
            store
                .apply_holder_command(
                    &fleet_approvals::ApprovalHolderAdmission::for_fixture_scope(&holder),
                    ApprovalHolderCommand::Raise(Box::new(raise.clone())),
                )
                .await
        })
        .await;
    assert!(matches!(
        replay,
        Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt {
            reason: central_storage::CorruptReason::BadValue { ref column },
        })) if column == "approval_request_destinations.scope"
    ));
    assert!(evidence.sql_writes.is_empty());
}

#[tokio::test]
async fn stored_kind_state_and_ending_are_rejected_by_the_public_read() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let request = key(&holder.run, 1);
    let raise = provider(&request, &requester, "Decode");
    raise_and_commit(&fixture, &holder, &raise).await;
    for (corruption, restore) in [
        (
            "UPDATE approval_requests SET request_kind='future_kind'",
            "UPDATE approval_requests SET request_kind='provider_approval'",
        ),
        (
            "UPDATE approval_requests SET record_state='future_state'",
            "UPDATE approval_requests SET record_state='waiting'",
        ),
    ] {
        fixture.alter_fixture(corruption).await;
        assert_corrupt_read(&fixture, &holder, &request.request_id).await;
        fixture.alter_fixture(restore).await;
    }
    let ended = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    person(11),
                    decide_input(&request.request_id, 101, 2),
                )
                .await
        })
        .await;
    assert!(matches!(
        ended.0.unwrap().reply,
        ApprovalDecisionReply::SavedDecision { .. }
    ));
    fixture.alter_fixture("UPDATE approval_requests SET ending_json=json_set(ending_json,'$.ending.kind','future_ending')").await;
    assert_corrupt_read(&fixture, &holder, &request.request_id).await;
}

async fn assert_corrupt_read(
    fixture: &NativeApprovalStoreFixture,
    holder: &ApprovalHolderScope,
    request_id: &ApprovalRequestId,
) {
    let (read, evidence) = fixture
        .apply(None, async |store| {
            store
                .fixture_read_person_approval(&holder.organization_id, &person_id(11), request_id)
                .await
        })
        .await;
    assert!(
        matches!(
            read,
            Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt { .. }))
        ),
        "{read:?}"
    );
    assert!(evidence.sql_writes.is_empty());
}

pub(super) async fn snapshot(fixture: &NativeApprovalStoreFixture) -> Vec<String> {
    fixture
        .inspect_rows(include_str!("../fixtures/approval_store_snapshot.sql"))
        .await
}
