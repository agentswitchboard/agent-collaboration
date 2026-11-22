use approval_request_protocol::{
    Answer, CancelReason, DecidedBy, Delivery, Ending, EndingView, HolderReport, HolderStopCause,
    InvalidAnswer, OfferEnding, Refusal, StopReceiptEnding, StopReply,
};
use central_engine_model::{
    Context, EventTag, Machine, StateTag, TransitionError, check_transitions,
};

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalLifecycle,
    ApprovalReceipt, ApprovalRefusal, ApprovalState, EndedApproval, FactorStanding,
    PassOnAuthority, RetainedApproval,
};

#[path = "approval_lifecycle_delivery_test.rs"]
mod delivery;
#[path = "approval_lifecycle_fixture_test.rs"]
mod fixtures;
#[path = "approval_lifecycle_model_test.rs"]
mod model;

use fixtures::{
    DEADLINE, UnusedIds, apply, approval_answer, approval_raise, call_raise, destinations,
    holder_report, observer, option_id, parent, parent_pass_on, person_answer, person_id,
    raise_event, requester, router_run, session, session_answer, step, stop_event, time, waiting,
};

const STOPPED: CancelReason = CancelReason::Stopped {
    cause: HolderStopCause::TurnStopped,
};
const GONE: Delivery = Delivery::Reported {
    report: HolderReport::GoneBeforeHandover,
};

fn closed(state: &ApprovalState) -> &EndedApproval {
    match state {
        ApprovalState::Ended { closed, .. } => closed,
        unexpected => panic!("expected an ending, got {unexpected:?}"),
    }
}

fn saved(state: &ApprovalState) -> &RetainedApproval {
    match state {
        ApprovalState::Waiting { saved, .. } | ApprovalState::Ended { saved, .. } => saved,
        unexpected => panic!("expected a saved raise, got {unexpected:?}"),
    }
}

fn refused(refusal: Refusal) -> TransitionError<ApprovalRefusal> {
    TransitionError::Domain(ApprovalRefusal::Destination { refusal })
}

fn decline_from_observer() -> ApprovalEvent {
    session_answer(observer(), "decline")
}

fn allow_from_parent() -> ApprovalEvent {
    session_answer(parent(), "once")
}

fn stop_reply(ending: StopReceiptEnding) -> ApprovalEffect {
    ApprovalEffect::Reply(ApprovalReceipt::Stop(StopReply::StopReceipt { ending }))
}

#[test]
fn first_valid_answer_wins_and_later_destination_sees_only_who_decided() {
    let initial = waiting();

    for (first, later, winner, choice) in [
        (
            allow_from_parent(),
            decline_from_observer(),
            parent(),
            "once",
        ),
        (
            decline_from_observer(),
            allow_from_parent(),
            observer(),
            "decline",
        ),
    ] {
        let by = DecidedBy::Session { session: winner };
        let answer = approval_answer(choice);
        let (decided, effects) = apply(&initial, first, 1);
        assert_eq!(
            closed(&decided).ending,
            Ending::Decided {
                answer: answer.clone(),
                by: by.clone()
            }
        );
        assert_eq!(
            effects.last(),
            Some(&ApprovalEffect::Reply(ApprovalReceipt::Answer(
                EndingView::DecidedWithAnswer {
                    answer,
                    by: by.clone()
                }
            )))
        );
        assert_eq!(
            step(&decided, later, 2),
            Err(refused(Refusal::Ended {
                ending: EndingView::Decided { by }
            }))
        );
        assert_eq!(
            step(&decided, session_answer(session(9), "once"), 2),
            Err(refused(Refusal::NotVisible {}))
        );
        assert_eq!(
            apply(&decided, ApprovalEvent::ClockReached, DEADLINE).0,
            decided
        );
    }
}

#[test]
fn answer_must_match_the_saved_offer() {
    let state = waiting();
    let wrong_kind = ApprovalEvent::Answer {
        actor: AdmittedDecider::Session(parent()),
        answer: Answer::Call {
            option_id: option_id("once"),
        },
    };

    for at in [2, DEADLINE] {
        assert_eq!(
            step(&state, session_answer(parent(), "not-offered"), at),
            Err(refused(Refusal::InvalidAnswer {
                reason: InvalidAnswer::UnofferedOption {
                    option_id: option_id("not-offered")
                }
            }))
        );
        assert_eq!(
            step(&state, wrong_kind.clone(), at),
            Err(refused(Refusal::InvalidAnswer {
                reason: InvalidAnswer::WrongKind {}
            }))
        );
    }
    assert!(matches!(
        closed(&apply(&state, allow_from_parent(), 3).0).ending,
        Ending::Decided { .. }
    ));
}

#[test]
fn deadline_wins_at_the_exact_tick_and_clock_never_ends_early() {
    let state = waiting();

    assert_eq!(
        step(&state, ApprovalEvent::ClockReached, DEADLINE - 1),
        Err(TransitionError::Stale)
    );
    assert!(matches!(
        closed(&apply(&state, allow_from_parent(), DEADLINE - 1).0).ending,
        Ending::Decided { .. }
    ));
    assert_eq!(
        closed(&apply(&state, stop_event(), DEADLINE - 1).0).ending,
        Ending::Cancelled { reason: STOPPED }
    );
    assert_eq!(
        apply(&state, parent_pass_on(), DEADLINE - 1).0.state_tag(),
        "passedOn"
    );

    for at in [DEADLINE, DEADLINE + 1] {
        for (event, receipt) in [
            (
                allow_from_parent(),
                Some(ApprovalEffect::Reply(ApprovalReceipt::AnswerRefused(
                    Refusal::TooLate {},
                ))),
            ),
            (
                parent_pass_on(),
                Some(ApprovalEffect::Reply(ApprovalReceipt::PassOnRefused(
                    Refusal::TooLate {},
                ))),
            ),
            (
                stop_event(),
                Some(stop_reply(StopReceiptEnding::TimedOut {})),
            ),
            (ApprovalEvent::ClockReached, None),
        ] {
            let (timed_out, effects) = apply(&state, event, at);
            assert_eq!(closed(&timed_out).ending, Ending::TimedOut {});
            assert_eq!(closed(&timed_out).ended_at, time(at));
            assert_eq!(
                effects
                    .iter()
                    .find(|effect| matches!(effect, ApprovalEffect::Reply(_))),
                receipt.as_ref()
            );
        }
    }
}

#[test]
fn only_destinations_other_than_requester_act_and_privileged_choices_need_fresh_person() {
    let state = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(approval_raise("First request")),
            destinations: vec![
                ApprovalDestination::People,
                ApprovalDestination::Session(requester()),
                ApprovalDestination::Session(parent()),
            ],
        },
        0,
    )
    .0;

    assert_eq!(
        step(&state, session_answer(requester(), "once"), 2),
        Err(refused(Refusal::RequesterCannotAnswer {}))
    );
    assert_eq!(
        step(&state, session_answer(observer(), "once"), 2),
        Err(refused(Refusal::NotVisible {}))
    );
    for choice in ["kept", "unchecked"] {
        assert_eq!(
            step(&state, session_answer(parent(), choice), 2),
            Err(refused(Refusal::KeptChoiceNeedsAPerson {}))
        );
        assert_eq!(
            step(
                &state,
                person_answer(FactorStanding::AbsentOrStale, choice),
                2
            ),
            Err(refused(Refusal::SecondFactorRequired {}))
        );
        let (approved, _) = apply(&state, person_answer(FactorStanding::Fresh, choice), 2);
        assert_eq!(
            closed(&approved).ending,
            Ending::Decided {
                answer: approval_answer(choice),
                by: DecidedBy::Person {
                    person_id: person_id()
                }
            }
        );
    }
    assert!(
        step(
            &state,
            person_answer(FactorStanding::AbsentOrStale, "once"),
            2
        )
        .is_ok()
    );

    let call = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(call_raise()),
            destinations: vec![
                ApprovalDestination::People,
                ApprovalDestination::Session(parent()),
            ],
        },
        0,
    )
    .0;
    let answer = Answer::Call {
        option_id: option_id("once"),
    };
    assert_eq!(
        step(
            &call,
            ApprovalEvent::Answer {
                actor: AdmittedDecider::Session(parent()),
                answer: answer.clone()
            },
            2
        ),
        Err(refused(Refusal::KeptChoiceNeedsAPerson {}))
    );
    assert_eq!(
        step(
            &call,
            ApprovalEvent::Answer {
                actor: AdmittedDecider::Person {
                    person_id: person_id(),
                    factor: FactorStanding::AbsentOrStale
                },
                answer: answer.clone()
            },
            2
        ),
        Err(refused(Refusal::SecondFactorRequired {}))
    );
    let (approved, effects) = apply(
        &call,
        ApprovalEvent::Answer {
            actor: AdmittedDecider::Person {
                person_id: person_id(),
                factor: FactorStanding::Fresh,
            },
            answer,
        },
        2,
    );
    assert!(matches!(closed(&approved).ending, Ending::Decided { .. }));
    assert!(effects.iter().any(|effect| matches!(effect, ApprovalEffect::OfferEnding(offer) if matches!(offer.as_ref(), OfferEnding::UnprovenCall { .. }))));
}

#[test]
fn live_parent_adds_people_once_and_keeps_destinations_and_deadline() {
    let initial = waiting();
    let not_live = ApprovalEvent::PassOn {
        authority: PassOnAuthority::NotLiveParent,
    };
    let not_a_destination = ApprovalEvent::PassOn {
        authority: PassOnAuthority::LiveParent(session(9)),
    };
    for event in [not_live, not_a_destination] {
        assert_eq!(
            step(&initial, event, 2),
            Err(refused(Refusal::NotVisible {}))
        );
    }
    assert_eq!(
        step(&initial, person_answer(FactorStanding::Fresh, "once"), 2),
        Err(refused(Refusal::NotVisible {}))
    );

    let (passed_on, effects) = apply(&initial, parent_pass_on(), 2);
    assert_eq!(passed_on.state_tag(), "passedOn");
    assert_eq!(effects, [ApprovalEffect::Reply(ApprovalReceipt::PassedOn)]);
    assert_eq!(
        saved(&passed_on).destinations,
        [
            ApprovalDestination::Session(parent()),
            ApprovalDestination::Session(observer()),
            ApprovalDestination::People
        ]
    );
    assert_eq!(
        ApprovalLifecycle::next_due(&passed_on),
        Some(time(DEADLINE))
    );
    assert_eq!(
        apply(&passed_on, parent_pass_on(), 3),
        (passed_on.clone(), effects)
    );
    assert!(step(&passed_on, person_answer(FactorStanding::Fresh, "once"), 3).is_ok());

    let (decided, _) = apply(&initial, decline_from_observer(), 3);
    assert_eq!(
        step(&decided, parent_pass_on(), 4),
        Err(refused(Refusal::Ended {
            ending: EndingView::Decided {
                by: DecidedBy::Session {
                    session: observer()
                }
            },
        }))
    );
    assert_eq!(saved(&decided).destinations, destinations());
    assert_eq!(
        step(&decided, person_answer(FactorStanding::Fresh, "once"), 4),
        Err(refused(Refusal::NotVisible {}))
    );
}

#[test]
fn every_state_event_pair_takes_a_declared_edge_or_refuses() {
    let pending = waiting();
    let (passed, _) = apply(&pending, parent_pass_on(), 1);
    let (pending_fresh, _) = apply(&ApprovalState::Unraised, raise_event(), DEADLINE - 1);
    let (passed_fresh, _) = apply(&pending_fresh, parent_pass_on(), DEADLINE - 1);
    let (awaiting, _) = apply(&pending, allow_from_parent(), 1);
    let (reported, _) = apply(
        &awaiting,
        holder_report(router_run(), HolderReport::Applied),
        1,
    );
    let (no_handover, _) = apply(&pending, ApprovalEvent::RunRetired, 1);
    let (unknown, _) = apply(&awaiting, ApprovalEvent::RunRetired, 1);
    let (marker, _) = apply(&ApprovalState::Unraised, stop_event(), 1);

    let scenarios = [
        ApprovalState::Unraised,
        marker,
        pending_fresh,
        passed_fresh,
        pending,
        passed,
        awaiting,
        reported,
        no_handover,
        unknown,
    ];
    let events = [
        raise_event(),
        allow_from_parent(),
        parent_pass_on(),
        stop_event(),
        ApprovalEvent::ClockReached,
        ApprovalEvent::RunRetired,
        ApprovalEvent::SessionEnded,
        ApprovalEvent::EnrollmentEnded,
        ApprovalEvent::OfferAgain {
            router_run: router_run(),
        },
        holder_report(router_run(), HolderReport::Applied),
    ];
    let mut cases = Vec::new();
    for state in scenarios {
        for event in &events {
            let refusal = match (state.state_tag(), event.event_tag()) {
                ("unraised" | "stoppedBeforeRaise", "raise" | "stop") => None,
                ("unraised" | "stoppedBeforeRaise", _)
                | ("pending" | "passedOn", "offerAgain" | "report") => {
                    Some(TransitionError::NotAllowed {
                        state: state.state_tag(),
                        event: event.event_tag(),
                    })
                }
                ("pending" | "passedOn", "clockReached")
                    if ApprovalLifecycle::next_due(&state) > Some(time(DEADLINE)) =>
                {
                    Some(TransitionError::Stale)
                }
                (
                    "awaitingReport" | "reported" | "noHandoverOwed" | "receiptUnknown",
                    "answer" | "passOn",
                ) => {
                    let ending = match &closed(&state).ending {
                        Ending::Decided { answer, by } => EndingView::DecidedWithAnswer {
                            answer: answer.clone(),
                            by: by.clone(),
                        },
                        Ending::Cancelled { reason } => EndingView::Cancelled { reason: *reason },
                        Ending::TimedOut {} => EndingView::TimedOut {},
                    };
                    Some(refused(Refusal::Ended { ending }))
                }
                ("noHandoverOwed" | "receiptUnknown", "stop") => Some(TransitionError::Stale),
                ("noHandoverOwed" | "receiptUnknown", "report")
                | ("reported" | "noHandoverOwed" | "receiptUnknown", "offerAgain") => {
                    Some(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed))
                }
                _ => None,
            };
            assert_eq!(
                step(&state, event.clone(), DEADLINE).err(),
                refusal,
                "{state:?}, {event:?}"
            );
            cases.push((state.clone(), event.clone()));
        }
    }
    assert_eq!(
        check_transitions::<ApprovalLifecycle>(
            cases,
            &mut Context {
                now: time(DEADLINE),
                ids: &mut UnusedIds
            }
        ),
        Ok(())
    );
}

#[test]
fn provider_question_requires_its_declared_fields_and_preserves_the_answer_for_the_holder() {
    use approval_request_protocol::{
        Description, Label, ProviderHolder, QuestionAnswer, QuestionAnswerValue, QuestionField,
        QuestionFieldAnswer, QuestionFieldId, QuestionFields, QuestionOffer, Raise,
    };

    let field_id = QuestionFieldId::new("continue").unwrap();
    let holder = ProviderHolder {
        session: requester(),
        router_run: router_run(),
    };
    let (state, _) = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(Raise::ProviderQuestion {
                request_id: fixtures::request_id(),
                holder: holder.clone(),
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
            }),
            destinations: destinations(),
        },
        0,
    );

    let submit = |fields| ApprovalEvent::Answer {
        actor: AdmittedDecider::Session(parent()),
        answer: Answer::Question {
            answer: QuestionAnswer::Answered { fields },
        },
    };
    assert_eq!(
        step(&state, submit(vec![]), 1),
        Err(refused(Refusal::InvalidAnswer {
            reason: InvalidAnswer::MissingRequiredField {
                field_id: field_id.clone(),
            },
        })),
    );
    let fields = vec![QuestionFieldAnswer {
        field_id,
        value: QuestionAnswerValue::Boolean(true),
    }];
    let (answered, effects) = apply(&state, submit(fields.clone()), 1);
    let ending = Ending::Decided {
        by: DecidedBy::Session { session: parent() },
        answer: Answer::Question {
            answer: QuestionAnswer::Answered { fields },
        },
    };
    assert_eq!(closed(&answered).ending, ending);
    assert!(effects.contains(&ApprovalEffect::OfferEnding(Box::new(
        OfferEnding::Provider {
            request_id: fixtures::request_id(),
            holder,
            ending,
        }
    ))));
}
