use approval_request_protocol::{
    CancelReason, DecidedBy, Delivery, DestinationEnding, Ending, EndingViewWire, HolderStopCause,
    PassOnReply, PassOnRequest, ProviderStopCause, RaiseReply, StandingEnding, StopReceiptEnding,
    StopReply, StopRequest, WaitingStatus,
};

use fleet_approvals::ApprovalDecisionReply as DecideReply;

use fleet_approvals::{
    AdmittedDecider, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalEffect,
    ApprovalState, FactorStanding,
};

use super::fixtures::{
    self, HOUR, ObservedReply as ApprovalReceipt, answer, closed, decide_event, key, person,
    person_id, provider, provider_stop, raise_event, raw_reply, reply, routing, run_event, session,
    setup, time, view_event,
};

#[test]
fn decide_binds_the_operation_and_keeps_its_original_audience_result() {
    let (mut book, active_run, requester, _) = setup();
    let first = key(&active_run, 1);
    let other = key(&active_run, 2);
    for key in [&first, &other] {
        run_event(
            &mut book,
            raise_event(
                &active_run,
                provider(key, &requester, "Review"),
                routing(None),
            ),
            0,
        );
    }

    let winner = person(11, FactorStanding::Fresh);
    let decide = decide_event(&first, winner.clone(), 101, answer(5));
    let result = run_event(&mut book, decide.clone(), 1);
    let ApprovalReceipt::Decide(DecideReply::SavedDecision {
        answer: saved_answer,
        by,
    }) = reply(&result)
    else {
        panic!("expected the original saved decision");
    };
    assert_eq!(saved_answer, answer(5));
    assert_eq!(
        by,
        DecidedBy::Person {
            person_id: person_id(11)
        }
    );
    let saved_result = raw_reply(&result).clone();

    let after_loss = run_event(&mut book, decide, 2);
    assert_eq!(
        after_loss,
        [ApprovalCollectionEffect::Request(ApprovalEffect::Reply(
            saved_result
        ))]
    );
    for conflicting in [
        decide_event(&first, winner.clone(), 101, answer(2)),
        decide_event(&first, person(12, FactorStanding::Fresh), 101, answer(5)),
        decide_event(&other, winner.clone(), 101, answer(5)),
    ] {
        assert_eq!(
            reply(&run_event(&mut book, conflicting, 2)),
            ApprovalReceipt::Decide(DecideReply::OperationConflict {})
        );
    }
    let mut wrong_target = decide_event(&first, winner, 105, answer(2));
    let ApprovalCollectionEvent::Decide { key, .. } = &mut wrong_target else {
        panic!("fixture must Decide");
    };
    *key = other.clone();
    assert_eq!(
        reply(&run_event(&mut book, wrong_target, 2)),
        ApprovalReceipt::Decide(DecideReply::NotVisible {})
    );
    assert!(matches!(
        book.requests[&other],
        ApprovalState::Waiting { .. }
    ));

    let standing = run_event(
        &mut book,
        decide_event(&first, person(12, FactorStanding::Fresh), 106, answer(1)),
        3,
    );
    let ApprovalReceipt::Decide(DecideReply::Standing { ending }) = reply(&standing) else {
        panic!("expected redacted standing");
    };
    assert_eq!(
        ending.wire_shape(),
        &EndingViewWire::ProviderApproval {
            ending: DestinationEnding::Standing {
                ending: StandingEnding::Decided {
                    by: DecidedBy::Person {
                        person_id: person_id(11)
                    },
                }
            },
        }
    );
    let own = run_event(
        &mut book,
        decide_event(&first, person(11, FactorStanding::Fresh), 107, answer(1)),
        4,
    );
    let ApprovalReceipt::Decide(DecideReply::Standing { ending }) = reply(&own) else {
        panic!("expected the giver's standing");
    };
    assert_eq!(
        ending.wire_shape(),
        &EndingViewWire::ProviderApproval {
            ending: DestinationEnding::Giver {
                decision: approval_request_protocol::GiverDecision {
                    by: DecidedBy::Person {
                        person_id: person_id(11)
                    },
                    answer: fixtures::option(5),
                }
            },
        }
    );
}

#[test]
fn applying_factor_and_offer_are_checked_before_the_deadline() {
    let (mut book, active_run, requester, parent) = setup();
    for slot in 3..=5_u8 {
        let item = key(&active_run, u16::from(slot));
        run_event(
            &mut book,
            raise_event(
                &active_run,
                provider(&item, &requester, "Review"),
                routing(Some(&parent)),
            ),
            0,
        );
        let request_from_requester = decide_event(
            &item,
            AdmittedDecider::Session(requester.clone()),
            100 + u16::from(slot),
            answer(1),
        );
        assert_eq!(
            reply(&run_event(&mut book, request_from_requester, 0)),
            ApprovalReceipt::Decide(DecideReply::NotVisible {})
        );
        let forward = ApprovalCollectionEvent::PassOn {
            key: item.clone(),
            parent: parent.clone(),
            request: PassOnRequest {
                request_id: item.request_id.clone(),
            },
        };
        assert_eq!(
            reply(&run_event(&mut book, forward, 0)),
            ApprovalReceipt::PassOn(PassOnReply::Forwarded {})
        );
        for (actor, suffix) in [
            (AdmittedDecider::Session(parent.clone()), 10),
            (person(12, FactorStanding::AbsentOrStale), 11),
        ] {
            let op = 200 + u16::from(slot) * 10 + suffix;
            let refused = decide_event(&item, actor, op, answer(slot));
            assert_eq!(
                reply(&run_event(&mut book, refused, 0)),
                ApprovalReceipt::Decide(DecideReply::Invalid {})
            );
        }
        let fresh = decide_event(
            &item,
            person(12, FactorStanding::Fresh),
            500 + u16::from(slot),
            answer(slot),
        );
        assert!(matches!(
            reply(&run_event(&mut book, fresh, 0)),
            ApprovalReceipt::Decide(DecideReply::SavedDecision { .. })
        ));
    }
}

#[test]
fn forwarding_checks_the_exact_live_assigned_parent_and_replays_success() {
    let (mut book, active_run, requester, parent) = setup();
    let first = key(&active_run, 1);
    let second = key(&active_run, 2);
    let other_session = session(&active_run.enrollment, 8004);
    let route = routing(Some(&parent));
    for key in [&first, &second] {
        run_event(
            &mut book,
            raise_event(
                &active_run,
                provider(key, &requester, "Review"),
                route.clone(),
            ),
            0,
        );
    }
    let forward = |target: &fleet_approvals::ApprovalRequestKey,
                   body: &fleet_approvals::ApprovalRequestKey,
                   actor| {
        ApprovalCollectionEvent::PassOn {
            key: target.clone(),
            parent: actor,
            request: PassOnRequest {
                request_id: body.request_id.clone(),
            },
        }
    };
    for wrong in [
        forward(&first, &second, parent.clone()),
        forward(&first, &first, other_session),
    ] {
        assert_eq!(
            reply(&run_event(&mut book, wrong, 1)),
            ApprovalReceipt::PassOn(PassOnReply::NotVisible {})
        );
    }
    let before = book.requests[&first].retained().unwrap().window;
    let action = forward(&first, &first, parent.clone());
    let changed = run_event(&mut book, action.clone(), 2);
    assert_eq!(
        reply(&changed),
        ApprovalReceipt::PassOn(PassOnReply::Forwarded {})
    );
    let retained = book.requests[&first].retained().unwrap();
    assert_eq!(retained.waiting_status(), WaitingStatus::PassedOn);
    assert_eq!(retained.window, before);
    assert_eq!(
        retained.routing,
        fleet_approvals::ApprovalRouting::Delegate {
            admitted_parent: parent.clone()
        }
    );
    assert!(matches!(
        reply(&run_event(&mut book, view_event(&first, 12), 2)),
        ApprovalReceipt::Read(Some(_))
    ));

    assert_eq!(
        reply(&run_event(&mut book, action.clone(), HOUR)),
        ApprovalReceipt::PassOn(PassOnReply::Forwarded {})
    );
    assert_eq!(
        closed(&book, &first).ending.clone().into_ending(),
        Ending::TimedOut {}
    );
    assert_eq!(
        reply(&run_event(
            &mut book,
            forward(&second, &second, parent.clone()),
            HOUR
        )),
        ApprovalReceipt::PassOn(PassOnReply::TooLate {})
    );
    run_event(
        &mut book,
        ApprovalCollectionEvent::EndSession { session: parent },
        HOUR + 1,
    );
    assert_eq!(
        reply(&run_event(&mut book, action, HOUR + 1)),
        ApprovalReceipt::PassOn(PassOnReply::NotVisible {})
    );
}

#[test]
fn stop_marker_binds_contents_and_deadline_wins_a_matching_waiting_stop() {
    let (mut book, active_run, requester, _) = setup();
    let before_raise = key(&active_run, 1);
    let raised = key(&active_run, 2);
    let original = provider(&before_raise, &requester, "Exact");
    let changed = provider(&before_raise, &requester, "Changed");
    let stop = |request| ApprovalCollectionEvent::Stop {
        run: active_run.clone(),
        request,
    };
    let cancelled = StopReceiptEnding::Cancelled {
        reason: CancelReason::Stopped {
            cause: HolderStopCause::AskReplaced,
        },
    };
    assert_eq!(
        reply(&run_event(&mut book, stop(provider_stop(&original)), 0)),
        ApprovalReceipt::Stop(StopReply::Stands { ending: cancelled })
    );
    assert_eq!(
        reply(&run_event(&mut book, stop(provider_stop(&changed)), 0)),
        ApprovalReceipt::Stop(StopReply::NotVisible {})
    );
    assert_eq!(
        reply(&run_event(
            &mut book,
            raise_event(&active_run, changed, routing(None)),
            0
        )),
        ApprovalReceipt::Raise(RaiseReply::ContentsDiffer {})
    );
    assert_eq!(
        reply(&run_event(
            &mut book,
            raise_event(&active_run, original, routing(None)),
            0
        )),
        ApprovalReceipt::Raise(RaiseReply::AlreadyStopped {})
    );

    let real = provider(&raised, &requester, "Exact");
    run_event(
        &mut book,
        raise_event(&active_run, real.clone(), routing(None)),
        0,
    );
    let effects = run_event(&mut book, stop(provider_stop(&real)), HOUR);
    assert_eq!(
        reply(&effects),
        ApprovalReceipt::Stop(StopReply::Stands {
            ending: StopReceiptEnding::TimedOut {}
        })
    );
    assert_eq!(
        closed(&book, &raised).ending.clone().into_ending(),
        Ending::TimedOut {}
    );
    assert_eq!(
        closed(&book, &raised).delivery,
        Delivery::ClosedWithoutReceipt {}
    );
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { .. })
    )));
}

#[test]
fn withdrawing_an_ask_closes_delivery_and_preserves_deadline_precedence() {
    let withdrawal = CancelReason::Stopped {
        cause: HolderStopCause::AskWithdrawn,
    };
    for (stop_at, expected_ending, expected_receipt) in [
        (
            HOUR - 1,
            Ending::Cancelled { reason: withdrawal },
            StopReceiptEnding::Cancelled { reason: withdrawal },
        ),
        (HOUR, Ending::TimedOut {}, StopReceiptEnding::TimedOut {}),
    ] {
        let (mut book, active_run, requester, _) = setup();
        let request_key = key(&active_run, 1);
        let raise = provider(&request_key, &requester, "Review");
        run_event(
            &mut book,
            raise_event(&active_run, raise.clone(), routing(None)),
            0,
        );
        let effects = run_event(
            &mut book,
            ApprovalCollectionEvent::Stop {
                run: active_run,
                request: StopRequest::Provider {
                    request_id: raise.request_id().clone(),
                    router_run: raise.router_run().clone(),
                    contents_digest: raise.contents_digest().unwrap(),
                    cause: ProviderStopCause::AskWithdrawn,
                },
            },
            stop_at,
        );
        assert_eq!(
            reply(&effects),
            ApprovalReceipt::Stop(StopReply::Stands {
                ending: expected_receipt
            })
        );
        let recorded = closed(&book, &request_key);
        assert_eq!(recorded.ending.clone().into_ending(), expected_ending);
        assert_eq!(recorded.delivery, Delivery::ClosedWithoutReceipt {});
        assert!(effects.contains(&ApprovalCollectionEffect::Request(
            ApprovalEffect::ClearDue { key: request_key },
        )));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { .. })
        )));
    }
}

#[test]
fn a_rollback_cannot_override_deadline_and_alarm_tokens_are_exact() {
    let (mut book, active_run, requester, _) = setup();
    let item = key(&active_run, 1);
    run_event(
        &mut book,
        raise_event(
            &active_run,
            provider(&item, &requester, "Review"),
            routing(None),
        ),
        0,
    );
    let alarm = |at| ApprovalCollectionEvent::Alarm {
        key: item.clone(),
        deadline: time(at),
    };
    assert!(run_event(&mut book, alarm(HOUR), HOUR - 1).is_empty());
    assert!(run_event(&mut book, alarm(HOUR + 1), HOUR).is_empty());
    assert!(matches!(
        book.requests[&item],
        ApprovalState::Waiting { .. }
    ));
    assert!(matches!(
        reply(&run_event(&mut book, view_event(&item, 12), HOUR)),
        ApprovalReceipt::Read(Some(_))
    ));
    let timed_out = book.clone();

    let decision = decide_event(
        &item,
        person(12, FactorStanding::AbsentOrStale),
        101,
        answer(2),
    );
    let effects = run_event(&mut book, decision, HOUR - 1);
    assert_eq!(
        reply(&effects),
        ApprovalReceipt::Decide(DecideReply::TooLate {})
    );
    assert_eq!(closed(&book, &item).ended_at, time(HOUR));
    assert_eq!(closed(&book, &item).delivery, Delivery::Owed {});
    assert_eq!(book, timed_out);
}

#[test]
fn an_invalid_decision_leaves_its_operation_free_and_reevaluates_after_the_ending() {
    let (mut book, active_run, requester, _) = setup();
    let item = key(&active_run, 1);
    let other = key(&active_run, 2);
    for request_key in [&item, &other] {
        run_event(
            &mut book,
            raise_event(
                &active_run,
                provider(request_key, &requester, "Review"),
                routing(None),
            ),
            0,
        );
    }

    let stale = decide_event(
        &item,
        person(12, FactorStanding::AbsentOrStale),
        101,
        answer(3),
    );
    let waiting = book.clone();
    assert_eq!(
        reply(&run_event(&mut book, stale.clone(), 1)),
        ApprovalReceipt::Decide(DecideReply::Invalid {})
    );
    assert_eq!(book, waiting);
    let accepted = decide_event(&item, person(12, FactorStanding::Fresh), 102, answer(3));
    run_event(&mut book, accepted, 2);
    let ended = book.clone();
    assert!(matches!(
        reply(&run_event(&mut book, stale, 3)),
        ApprovalReceipt::Decide(DecideReply::Standing { .. })
    ));
    assert_eq!(book, ended);
    let reused = decide_event(&other, person(12, FactorStanding::Fresh), 101, answer(2));
    assert!(matches!(
        reply(&run_event(&mut book, reused, 4)),
        ApprovalReceipt::Decide(DecideReply::SavedDecision { .. })
    ));
}

#[test]
fn a_visible_read_materializes_the_timeout_and_hidden_read_changes_no_row() {
    let (mut book, active_run, requester, parent) = setup();
    let item = key(&active_run, 1);
    run_event(
        &mut book,
        raise_event(
            &active_run,
            provider(&item, &requester, "Review"),
            routing(Some(&parent)),
        ),
        0,
    );
    let owner_item = key(&active_run, 2);
    run_event(
        &mut book,
        raise_event(
            &active_run,
            provider(&owner_item, &requester, "Owner"),
            routing(None),
        ),
        0,
    );
    assert_eq!(
        reply(&run_event(&mut book, view_event(&item, 12), HOUR)),
        ApprovalReceipt::Read(None)
    );
    assert!(matches!(
        book.requests[&item],
        ApprovalState::Waiting { .. }
    ));

    let before_deadline = run_event(&mut book, view_event(&owner_item, 12), HOUR - 1);
    assert!(matches!(
        reply(&before_deadline),
        ApprovalReceipt::Read(Some(_))
    ));
    assert!(matches!(
        book.requests[&owner_item],
        ApprovalState::Waiting { .. }
    ));

    let effects = run_event(&mut book, view_event(&owner_item, 12), HOUR);
    assert!(matches!(reply(&effects), ApprovalReceipt::Read(Some(_))));
    assert_eq!(
        closed(&book, &owner_item).ending.clone().into_ending(),
        Ending::TimedOut {}
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { .. })
    )));
}

#[test]
fn parent_decides_a_delegate_request_as_its_exact_central_session() {
    let (mut book, run, requester, parent) = setup();
    let item = key(&run, 1);
    let raised = run_event(
        &mut book,
        raise_event(
            &run,
            provider(&item, &requester, "Delegate"),
            routing(Some(&parent)),
        ),
        0,
    );
    assert!(raised.iter().any(|effect| matches!(effect,
        ApprovalCollectionEffect::Request(ApprovalEffect::StoreSessionNotice { session, .. }) if session == &parent
    )));
    assert_eq!(
        reply(&run_event(&mut book, view_event(&item, 10), 0)),
        ApprovalReceipt::Read(None)
    );
    let exact = decide_event(
        &item,
        AdmittedDecider::Session(parent.clone()),
        101,
        answer(1),
    );
    let decided = run_event(&mut book, exact.clone(), 1);
    assert_eq!(
        reply(&decided),
        ApprovalReceipt::Decide(DecideReply::SavedDecision {
            answer: answer(1),
            by: DecidedBy::Session {
                session: parent.session_id
            },
        })
    );
    assert_eq!(reply(&run_event(&mut book, exact, 2)), reply(&decided));
}

#[test]
fn provider_question_uses_its_declared_fields_and_offers_the_exact_answer() {
    use approval_request_protocol::{
        Answer, Description, Label, PrivateEnding, ProviderHolder, QuestionAnswer,
        QuestionAnswerValue, QuestionField, QuestionFieldAnswer, QuestionFieldId, QuestionFields,
        QuestionOffer, Raise,
    };
    let (mut book, run, requester, _) = setup();
    let item = key(&run, 1);
    let field_id = QuestionFieldId::new(1).unwrap();
    let raise = Raise::ProviderQuestion {
        request_id: item.request_id.clone(),
        holder: ProviderHolder {
            session_id: requester.session_id,
            router_run: run.router_run.clone(),
        },
        offer: QuestionOffer {
            prompt: Description::new("Proceed?").unwrap(),
            fields: QuestionFields::new(vec![QuestionField::Boolean {
                field_id: field_id.clone(),
                label: Label::new("Continue").unwrap(),
                description: None,
                required: true,
            }])
            .unwrap(),
        },
    };
    run_event(&mut book, raise_event(&run, raise, routing(None)), 0);
    let decide = |operation, fields| {
        decide_event(
            &item,
            person(10, FactorStanding::Fresh),
            operation,
            Answer::Question {
                answer: QuestionAnswer::Answered { fields },
            },
        )
    };
    let invalid = run_event(&mut book, decide(101, vec![]), 1);
    assert_eq!(
        reply(&invalid),
        ApprovalReceipt::Decide(DecideReply::Invalid {})
    );
    let fields = vec![QuestionFieldAnswer {
        field_id,
        value: QuestionAnswerValue::Boolean { value: true },
    }];
    let accepted = run_event(&mut book, decide(102, fields.clone()), 2);
    let frame = fixtures::offered_frame(&accepted);
    assert!(matches!(
        frame.ending(),
        PrivateEnding::ProviderQuestion { .. }
    ));
    assert_eq!(
        frame.into_ending(),
        Ending::Decided {
            answer: approval_request_protocol::ProviderAnswer::Question {
                answer: QuestionAnswer::Answered { fields }
            },
            by: DecidedBy::Person {
                person_id: person_id(10)
            },
        }
    );
}

#[test]
fn an_equal_raise_replays_the_first_window_and_changed_contents_leave_the_row_alone() {
    let (mut book, run, requester, _) = setup();
    let item = key(&run, 1);
    let first = raise_event(&run, provider(&item, &requester, "Original"), routing(None));
    let first_reply = raw_reply(&run_event(&mut book, first.clone(), 0)).clone();
    let original = book.requests[&item].clone();
    let conflicting = raise_event(&run, provider(&item, &requester, "Changed"), routing(None));
    assert_eq!(
        reply(&run_event(&mut book, conflicting, 1)),
        ApprovalReceipt::Raise(RaiseReply::ContentsDiffer {})
    );
    assert_eq!(book.requests[&item], original);
    assert_eq!(
        run_event(&mut book, first, 2),
        [ApprovalCollectionEffect::Request(ApprovalEffect::Reply(
            first_reply
        )),]
    );
    assert_eq!(book.requests[&item], original);
}
