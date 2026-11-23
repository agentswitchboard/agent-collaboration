use approval_request_protocol::{
    DeliverEndingReply, Ending, PassOnReply, RaiseReply, RegisterRunReply,
};
use central_storage::StoreFailure;
use fleet_approvals::{
    AdmittedDecider, ApprovalApplyFailure, ApprovalDecisionReply, ApprovalHolderAdmission,
    ApprovalHolderCommand, ApprovalHolderScope, ApprovalReleaseFailure, ApprovalReplyAddress,
    ApprovalRun, FactorStanding,
};

use super::{
    approval_store_cases::{
        decide_input, first_holder, fixture_with_owner, raise_and_commit, register, snapshot,
    },
    fixtures::{HOUR, key, person, person_id, provider, router_run, session, time},
    native_approval_store::NativeApprovalStoreFixture,
};

#[tokio::test]
async fn native_approval_touch_applies_its_deadline_before_the_projected_alarm() {
    use central_engine_model::{AlarmDecision, AlarmInputs, next_alarm};

    let (fixture, holder, requester) = fixture_with_owner().await;
    fixture.clock.set_millis(1);
    let request = provider(&key(&holder.run, 1), &requester, "Off-mark deadline");
    assert!(matches!(
        raise_and_commit(&fixture, &holder, &request).await.0,
        RaiseReply::Receipt { .. }
    ));
    let deadline = HOUR + 1;
    let touched_at = deadline + 1;
    let alarm_mark = HOUR + 300_000;
    assert!(fixture.due_rows(time(alarm_mark - 1), 1).await.is_empty());
    let due_rows = fixture.due_rows(time(alarm_mark), 1).await;
    let [approval] = due_rows.as_slice() else {
        panic!("expected one indexed approval deadline: {due_rows:?}");
    };
    assert_eq!(approval.occurrence_ms, deadline);
    assert_eq!(approval.effective_due_ms, alarm_mark);
    let alarm_times = fixture.read_alarm_times().await;
    assert_eq!(
        alarm_times.earliest_on_grid_due_unix_millis,
        Some(alarm_mark)
    );
    assert_eq!(
        next_alarm(&AlarmInputs {
            retained_mark: None,
            now: time(1),
            last_batch_start: None,
            leftovers_beyond_cap: false,
            earliest_on_grid_due: alarm_times.earliest_on_grid_due_unix_millis.map(time),
            earliest_legacy_due: alarm_times.earliest_legacy_due_unix_millis.map(time),
        }),
        AlarmDecision::SetAt(time(alarm_mark))
    );

    fixture.clock.set_millis(touched_at);
    let (answer, _) = fixture
        .apply(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    person(11, FactorStanding::Fresh),
                    decide_input(request.request_id(), 5_430, 1),
                )
                .await
        })
        .await;
    let answer = answer.unwrap();
    assert_eq!(answer.reply, ApprovalDecisionReply::TooLate {});
    let [offer] = answer.ending_offers.as_slice() else {
        panic!("the touch did not retain a timeout");
    };
    assert_eq!(offer.request.clone().into_ending(), Ending::TimedOut {});
    assert!(fixture.due_rows(time(alarm_mark), 1).await.is_empty());
}

#[tokio::test]
async fn retained_refusals_conflicts_and_holder_reoffers_persist_the_deadline() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    let caller = person(11, FactorStanding::Fresh);
    for request_no in [1, 2, 3] {
        let request = provider(
            &key(&holder.run, request_no),
            &requester,
            "Expiry observation",
        );
        fixture.clock.set_millis(0);
        assert!(matches!(
            raise_and_commit(&fixture, &holder, &request).await.0,
            RaiseReply::Receipt { .. }
        ));
        let operation = request_no + 100;
        let bad = decide_input(request.request_id(), operation, 15);
        fixture.clock.set_millis(1);
        let bad_decide = fixture
            .apply(None, async |store| {
                store
                    .fixture_decide_approval(&holder.organization_id, caller.clone(), bad.clone())
                    .await
            })
            .await;
        assert_eq!(
            bad_decide.0.unwrap().reply,
            ApprovalDecisionReply::Invalid {}
        );
        let retained_before = fixture
            .inspect_rows("SELECT answer FROM runtime_request_log ORDER BY request_id")
            .await;
        fixture.clock.set_millis(HOUR);
        let command = decide_input(
            request.request_id(),
            operation,
            if request_no == 1 { 15 } else { 1 },
        );
        let before = snapshot(&fixture).await;
        for write in [1, 2] {
            let (failure, evidence) = if request_no == 3 {
                let pending = fixture
                    .apply(Some(write), async |store| {
                        store
                            .apply_holder_command(
                                &admission,
                                ApprovalHolderCommand::Reoffer {
                                    request_id: request.request_id().clone(),
                                },
                            )
                            .await
                    })
                    .await;
                (pending.0.err(), pending.1)
            } else {
                let pending = fixture
                    .apply(Some(write), async |store| {
                        store
                            .fixture_decide_approval(
                                &holder.organization_id,
                                caller.clone(),
                                command.clone(),
                            )
                            .await
                    })
                    .await;
                (pending.0.err(), pending.1)
            };
            assert!(matches!(
                failure,
                Some(ApprovalApplyFailure::Store(StoreFailure::Unavailable))
            ));
            assert_eq!(evidence.sql_writes.len(), write);
            assert_eq!(snapshot(&fixture).await, before);
        }
        let offers = if request_no == 3 {
            fixture
                .apply(None, async |store| {
                    store
                        .apply_holder_command(
                            &admission,
                            ApprovalHolderCommand::Reoffer {
                                request_id: request.request_id().clone(),
                            },
                        )
                        .await
                })
                .await
                .0
                .unwrap()
                .ending_offers
        } else {
            let (answer, evidence) = fixture
                .apply(None, async |store| {
                    store
                        .fixture_decide_approval(
                            &holder.organization_id,
                            caller.clone(),
                            command.clone(),
                        )
                        .await
                })
                .await;
            let answer = answer.unwrap();
            let expected = if request_no == 1 {
                ApprovalDecisionReply::Invalid {}
            } else {
                ApprovalDecisionReply::OperationConflict {}
            };
            assert_eq!(answer.reply, expected);
            assert_eq!(evidence.sql_writes.len(), 2);
            answer.ending_offers
        };
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].request.clone().into_ending(), Ending::TimedOut {});
        assert_eq!(
            fixture
                .inspect_rows("SELECT answer FROM runtime_request_log ORDER BY request_id")
                .await,
            retained_before
        );
        fixture.clock.set_millis(HOUR - 1);
        let attempted = fixture
            .apply(None, async |store| {
                store
                    .fixture_decide_approval(
                        &holder.organization_id,
                        caller.clone(),
                        decide_input(request.request_id(), request_no + 200, 1),
                    )
                    .await
            })
            .await;
        assert_eq!(
            attempted.0.unwrap().reply,
            ApprovalDecisionReply::TooLate {}
        );
        assert!(fixture.due_rows(time(HOUR), 1).await.is_empty());
    }
}

#[tokio::test]
async fn missing_healthy_and_malformed_hidden_targets_are_indistinguishable() {
    let fixture = NativeApprovalStoreFixture::open_fresh().await;
    let holder = first_holder();
    register(&fixture, &holder).await;
    let requester = session(&holder.run.enrollment, 0xa191);
    let parent = session(&holder.run.enrollment, 0xa192);
    let other = session(&holder.run.enrollment, 0xa193);
    for identity in [&parent, &other] {
        fixture
            .store_session(&holder.organization_id, identity, None, true)
            .await;
    }
    fixture
        .store_session(&holder.organization_id, &requester, Some(&parent), true)
        .await;
    let request = provider(&key(&holder.run, 1), &requester, "Assigned parent only");
    for phase in [0, 1, 2] {
        if phase == 1 {
            assert!(matches!(
                raise_and_commit(&fixture, &holder, &request).await.0,
                RaiseReply::Receipt { .. }
            ));
        }
        if phase == 2 {
            fixture
                .alter_fixture("UPDATE approval_requests SET record_state='future_state'")
                .await;
        }
        let (seen, evidence) = fixture
            .apply(None, async |store| {
                store
                    .fixture_read_person_approval(
                        &holder.organization_id,
                        &person_id(11),
                        request.request_id(),
                    )
                    .await
            })
            .await;
        assert!(seen.unwrap().reply.is_none());
        assert!(evidence.sql_writes.is_empty());
        assert_eq!(evidence.sql_reads.len(), 1);
        for actor in [
            person(11, FactorStanding::Fresh),
            AdmittedDecider::Session(other.clone()),
        ] {
            let (decision, evidence) = fixture
                .apply(None, async |store| {
                    store
                        .fixture_decide_approval(
                            &holder.organization_id,
                            actor,
                            decide_input(request.request_id(), 101, 1),
                        )
                        .await
                })
                .await;
            assert_eq!(
                decision.unwrap().reply,
                ApprovalDecisionReply::NotVisible {}
            );
            assert!(evidence.sql_writes.is_empty());
            assert_eq!(evidence.sql_reads.len(), 1);
        }
        let (pass, evidence) = fixture
            .apply(None, async |store| {
                store
                    .fixture_pass_on_approval(
                        &holder.organization_id,
                        other.clone(),
                        request.request_id(),
                    )
                    .await
            })
            .await;
        assert_eq!(pass.unwrap().reply, PassOnReply::NotVisible {});
        assert!(evidence.sql_writes.is_empty());
        assert_eq!(evidence.sql_reads.len(), 1);
    }
    let (admitted, _) = fixture
        .apply(None, async |store| {
            store
                .fixture_pass_on_approval(&holder.organization_id, parent, request.request_id())
                .await
        })
        .await;
    assert!(matches!(
        admitted,
        Err(ApprovalApplyFailure::Store(StoreFailure::Corrupt { .. }))
    ));
}

#[tokio::test]
async fn owed_recovery_and_committed_outputs_stay_bound_to_the_exact_loan_and_recipient() {
    let (fixture, holder, requester) = fixture_with_owner().await;
    let admission = ApprovalHolderAdmission::for_fixture_scope(&holder);
    let wrong_scope = ApprovalHolderScope {
        run: ApprovalRun {
            router_run: router_run(0xa194),
            ..holder.run.clone()
        },
        ..holder.clone()
    };
    let wrong = ApprovalHolderAdmission::for_fixture_scope(&wrong_scope);
    let request = provider(&key(&holder.run, 1), &requester, "Owed recovery");
    raise_and_commit(&fixture, &holder, &request).await;
    let decision = fixture
        .commit_pending(None, async |store| {
            store
                .fixture_decide_approval(
                    &holder.organization_id,
                    person(11, FactorStanding::Fresh),
                    decide_input(request.request_id(), 101, 1),
                )
                .await
        })
        .await
        .0
        .unwrap();
    let (proof, (reply, holder_effects)) = decision;
    assert_eq!(
        reply.take_for_recipient(
            &proof,
            &ApprovalReplyAddress::Person {
                organization: holder.organization_id.clone(),
                person: person_id(12),
            }
        ),
        Err(ApprovalReleaseFailure::OtherRecipient)
    );
    let offer = holder_effects.release(&proof).unwrap().pop().unwrap();
    assert_eq!(
        offer.take_for_holder(&wrong),
        Err(ApprovalReleaseFailure::OtherRecipient)
    );

    let (wrong_recovery, evidence) = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&wrong).await
        })
        .await;
    assert!(wrong_recovery.unwrap().ending_offers.is_empty());
    assert!(evidence.sql_writes.is_empty());
    let recovered = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&admission).await
        })
        .await;
    let offers = recovered.0.unwrap().ending_offers;
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0].request.request_id(), request.request_id());
    assert!(recovered.1.sql_writes.is_empty());

    let pending = fixture
        .commit_pending(None, async |store| {
            store.register_approval_run(&admission).await
        })
        .await
        .0
        .unwrap();
    let (_exact, (reply, effects)) = pending;
    assert_eq!(
        reply.take_for_recipient(&proof, &ApprovalReplyAddress::Holder(holder.clone())),
        Err(ApprovalReleaseFailure::OtherCommit)
    );
    assert!(matches!(
        effects.release(&proof),
        Err(ApprovalReleaseFailure::OtherCommit)
    ));

    let mut foreign_org = holder.clone();
    foreign_org.organization_id = "019ac19f-aa00-7000-8000-00000000a1fe"
        .to_owned()
        .try_into()
        .unwrap();
    let foreign = ApprovalHolderAdmission::for_fixture_scope(&foreign_org);
    let (wrong_loan, evidence) = fixture
        .apply(None, async |store| {
            store.register_approval_run(&foreign).await
        })
        .await;
    assert_eq!(
        wrong_loan.unwrap().reply,
        RegisterRunReply::RunNotRegistered {}
    );
    assert!(evidence.sql_reads.is_empty() && evidence.sql_writes.is_empty());

    fixture
        .apply(None, async |store| {
            store
                .apply_holder_command(
                    &admission,
                    ApprovalHolderCommand::DeliveryReply {
                        request: offers[0].request.clone(),
                        reply: DeliverEndingReply::Received {},
                    },
                )
                .await
        })
        .await
        .0
        .unwrap();
    let paid = fixture
        .apply(None, async |store| {
            store.recover_owed_approval_endings(&admission).await
        })
        .await;
    assert!(paid.0.unwrap().ending_offers.is_empty());
}
