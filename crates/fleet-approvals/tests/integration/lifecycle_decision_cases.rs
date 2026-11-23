// Included by lifecycle.rs to preserve the existing test selectors.
#[test]
fn an_ended_row_projects_its_standing_decision_to_the_current_audience() {
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
    let same_input = run_event(&mut book, decide, 2);
    assert!(matches!(
        reply(&same_input),
        ApprovalReceipt::Decide(DecideReply::Standing { .. })
    ));
    assert_eq!(
        closed(&book, &first).decided_operation_id,
        Some(fixtures::operation_id(101))
    );
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
    assert!(matches!(
        reply(&run_event(&mut book, exact, 2)),
        ApprovalReceipt::Decide(DecideReply::Standing { .. })
    ));
    assert_eq!(
        closed(&book, &item).decided_operation_id,
        Some(fixtures::operation_id(101))
    );
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

