use approval_request_protocol::{
    Answer, CancelReason, DecidedBy, Delivery, Ending, EndingView, HolderReport, HolderStopCause,
    OfferEnding, ParkedCallHolder, RaiseReceipt, RaiseReply, Refusal, ReportDeliveryReply,
    ReportDeliveryRequest, StopReceiptEnding,
};
use central_engine_model::{Machine, TransitionError};

use crate::{
    AdmittedDecider, ApprovalDestination, ApprovalEffect, ApprovalEvent, ApprovalLifecycle,
    ApprovalReceipt, ApprovalRefusal, ApprovalState, FactorStanding, SessionNoticePhase,
};

use super::fixtures::{
    DEADLINE, apply, approval_raise, call_raise, destinations, holder_report, observer, option_id,
    other_request_id, other_run, parent, person_answer, person_id, raise_event, request_id,
    requester, router_run, session_answer, step, stop_event, time, waiting,
};
use super::{GONE, STOPPED, allow_from_parent, closed, refused, stop_reply};

#[test]
fn replay_preserves_first_receipt_and_refuses_changed_contents() {
    let initial = waiting();
    let replay = ApprovalEvent::Raise {
        raise: Box::new(approval_raise("First request")),
        destinations: vec![ApprovalDestination::People],
    };
    let different = ApprovalEvent::Raise {
        raise: Box::new(approval_raise("Replacement request")),
        destinations: destinations(),
    };
    let (closed, _) = apply(&initial, stop_event(), 8);
    let receipt = RaiseReceipt {
        request_id: request_id(),
        raised_at: time(0),
        deadline: time(DEADLINE),
    };

    for state in [initial, closed] {
        assert_eq!(
            apply(&state, replay.clone(), 9),
            (
                state.clone(),
                vec![ApprovalEffect::Reply(ApprovalReceipt::Raise(
                    RaiseReply::RaiseReceipt {
                        receipt: receipt.clone()
                    }
                ))],
            )
        );
        assert_eq!(
            step(&state, different.clone(), 9),
            Err(TransitionError::Domain(ApprovalRefusal::ContentsDiffer))
        );
    }
}

#[test]
fn holder_stop_handles_unraised_waiting_and_already_decided_requests() {
    let (marker, effects) = apply(&ApprovalState::Unraised, stop_event(), 1);
    assert_eq!(
        marker,
        ApprovalState::StoppedBeforeRaise {
            request_id: request_id(),
            router_run: router_run(),
            cause: HolderStopCause::TurnStopped,
            stopped_at: time(1),
        }
    );
    let cancelled_reply = stop_reply(StopReceiptEnding::Cancelled { reason: STOPPED });
    assert_eq!(effects, std::slice::from_ref(&cancelled_reply));
    assert_eq!(ApprovalLifecycle::next_due(&marker), None);
    assert_eq!(
        apply(&marker, raise_event(), 2),
        (
            marker,
            vec![ApprovalEffect::Reply(ApprovalReceipt::Raise(
                RaiseReply::AlreadyStopped {}
            ))]
        )
    );

    let state = waiting();
    let (stopped_first, effects) = apply(&state, stop_event(), 1);
    assert_eq!(
        closed(&stopped_first).ending,
        Ending::Cancelled { reason: STOPPED }
    );
    assert_eq!(closed(&stopped_first).delivery, GONE);
    assert_eq!(effects.last(), Some(&cancelled_reply));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, ApprovalEffect::OfferEnding(_)))
    );
    assert_eq!(
        step(&stopped_first, allow_from_parent(), 2),
        Err(refused(Refusal::Ended {
            ending: EndingView::Cancelled { reason: STOPPED }
        }))
    );

    let (answered_first, _) = apply(&state, allow_from_parent(), 1);
    let (stopped_later, effects) = apply(&answered_first, stop_event(), DEADLINE);
    assert_eq!(
        closed(&stopped_later).ending,
        closed(&answered_first).ending
    );
    assert_eq!(closed(&stopped_later).ended_at, time(1));
    assert_eq!(closed(&stopped_later).delivery, GONE);
    assert_eq!(effects, [stop_reply(StopReceiptEnding::DecidedEarlier {})]);
}

#[test]
fn report_is_scoped_repeatable_and_immutable_even_after_retirement() {
    let initial = waiting();
    let report = holder_report(router_run(), HolderReport::Applied);
    assert_eq!(
        step(&initial, report.clone(), 1),
        Err(TransitionError::NotAllowed {
            state: "pending",
            event: "report"
        })
    );
    let (awaiting, effects) = apply(&initial, allow_from_parent(), 1);
    assert_eq!(closed(&awaiting).delivery, Delivery::AwaitingReport {});
    let offer = effects
        .iter()
        .find(|effect| matches!(effect, ApprovalEffect::OfferEnding(_)));
    assert!(offer.is_some());
    let (same, repeated) = apply(
        &awaiting,
        ApprovalEvent::OfferAgain {
            router_run: router_run(),
        },
        2,
    );
    assert_eq!(same, awaiting);
    assert_eq!(repeated.first(), offer);
    assert_eq!(repeated.len(), 1);
    assert_eq!(
        step(
            &awaiting,
            holder_report(other_run(), HolderReport::Applied),
            2
        ),
        Err(TransitionError::Stale)
    );

    assert_eq!(
        step(
            &awaiting,
            ApprovalEvent::OfferAgain {
                router_run: other_run()
            },
            2
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        step(
            &awaiting,
            ApprovalEvent::Report(ReportDeliveryRequest {
                request_id: other_request_id(),
                router_run: router_run(),
                report: HolderReport::Applied,
            }),
            2
        ),
        Err(TransitionError::Stale)
    );

    let (reported, ack) = apply(&awaiting, report.clone(), 2);
    assert_eq!(closed(&reported).ending, closed(&awaiting).ending);
    assert_eq!(
        closed(&reported).delivery,
        Delivery::Reported {
            report: HolderReport::Applied
        }
    );
    assert_eq!(
        ack,
        [ApprovalEffect::Reply(ApprovalReceipt::Report(
            ReportDeliveryReply::DeliveryRecorded {}
        ))]
    );
    assert_eq!(apply(&reported, report, 3), (reported.clone(), ack));
    assert_eq!(
        step(
            &reported,
            holder_report(router_run(), HolderReport::DeliveryUnknown),
            3
        ),
        Err(TransitionError::Domain(ApprovalRefusal::ReportDiffers))
    );
    assert_eq!(apply(&reported, stop_event(), 4).0, reported);
    assert_eq!(
        apply(&reported, ApprovalEvent::RunRetired, 5),
        (reported.clone(), vec![])
    );

    let (unknown, effects) = apply(&awaiting, ApprovalEvent::RunRetired, 4);
    assert_eq!(closed(&unknown).ending, closed(&awaiting).ending);
    assert_eq!(closed(&unknown).delivery, Delivery::ReceiptUnknown {});
    assert!(effects.is_empty());
}

#[test]
fn authority_end_uses_deadline_and_suppresses_only_unreachable_handover() {
    let state = waiting();

    for (event, reason, reachable) in [
        (
            ApprovalEvent::RunRetired,
            CancelReason::RequesterGone {},
            false,
        ),
        (
            ApprovalEvent::EnrollmentEnded,
            CancelReason::EnrollmentEnded {},
            false,
        ),
        (
            ApprovalEvent::SessionEnded,
            CancelReason::SessionEnded {},
            true,
        ),
    ] {
        for at in [DEADLINE - 1, DEADLINE, DEADLINE + 1] {
            let (ended, effects) = apply(&state, event.clone(), at);
            let expected = if at < DEADLINE {
                Ending::Cancelled { reason }
            } else {
                Ending::TimedOut {}
            };
            assert_eq!(closed(&ended).ending, expected);
            let delivery = match (reachable, at < DEADLINE) {
                (true, _) => Delivery::AwaitingReport {},
                (false, true) => Delivery::NoHandoverOwed {},
                (false, false) => Delivery::ReceiptUnknown {},
            };
            assert_eq!(closed(&ended).delivery, delivery);
            assert_eq!(
                effects
                    .iter()
                    .any(|effect| matches!(effect, ApprovalEffect::OfferEnding(_))),
                reachable
            );
        }
        let (decided, _) = apply(&state, allow_from_parent(), 1);
        let (ended, effects) = apply(&decided, event, DEADLINE);
        assert_eq!(closed(&ended).ending, closed(&decided).ending);
        assert_eq!(
            closed(&ended).delivery,
            if reachable {
                Delivery::AwaitingReport {}
            } else {
                Delivery::ReceiptUnknown {}
            }
        );
        assert!(effects.is_empty());
    }
}

#[test]
fn session_notice_due_and_handover_effects_describe_the_guarded_write() {
    let raise = approval_raise("First request");
    let mut destination_list = destinations();
    destination_list.extend([
        ApprovalDestination::People,
        ApprovalDestination::Session(parent()),
    ]);
    let notice = |session, phase| ApprovalEffect::StoreSessionNotice {
        request_id: request_id(),
        session,
        phase,
    };
    let (waiting, effects) = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(raise.clone()),
            destinations: destination_list,
        },
        0,
    );
    assert_eq!(
        effects,
        [
            notice(parent(), SessionNoticePhase::Raised),
            notice(observer(), SessionNoticePhase::Raised),
            ApprovalEffect::RegisterDue {
                request_id: request_id(),
                deadline: time(DEADLINE)
            },
            ApprovalEffect::Reply(ApprovalReceipt::Raise(RaiseReply::RaiseReceipt {
                receipt: RaiseReceipt {
                    request_id: request_id(),
                    raised_at: time(0),
                    deadline: time(DEADLINE),
                }
            })),
        ]
    );

    let (ended, effects) = apply(&waiting, ApprovalEvent::ClockReached, DEADLINE);
    let approval_request_protocol::Raise::ProviderApproval { holder, .. } = raise else {
        panic!("fixture must be a provider approval");
    };
    assert_eq!(
        effects,
        [
            notice(parent(), SessionNoticePhase::Ended),
            notice(observer(), SessionNoticePhase::Ended),
            ApprovalEffect::ClearDue {
                request_id: request_id()
            },
            ApprovalEffect::OfferEnding(Box::new(OfferEnding::Provider {
                request_id: request_id(),
                holder,
                ending: Ending::TimedOut {}
            })),
        ]
    );
    assert_eq!(ApprovalLifecycle::next_due(&ended), None);
    assert_eq!(
        apply(&ended, ApprovalEvent::ClockReached, DEADLINE + 1),
        (ended, vec![])
    );
}

#[test]
fn expired_admitted_answer_writes_the_full_timeout_even_if_its_choice_or_factor_is_invalid() {
    use crate::EndedApproval;
    use approval_request_protocol::ProviderHolder;

    let mut destinations = destinations();
    destinations.push(ApprovalDestination::People);
    let (initial, _) = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(approval_raise("First request")),
            destinations,
        },
        0,
    );
    let expected_effects = vec![
        ApprovalEffect::StoreSessionNotice {
            request_id: request_id(),
            session: parent(),
            phase: SessionNoticePhase::Ended,
        },
        ApprovalEffect::StoreSessionNotice {
            request_id: request_id(),
            session: observer(),
            phase: SessionNoticePhase::Ended,
        },
        ApprovalEffect::ClearDue {
            request_id: request_id(),
        },
        ApprovalEffect::OfferEnding(Box::new(OfferEnding::Provider {
            request_id: request_id(),
            holder: ProviderHolder {
                session: requester(),
                router_run: router_run(),
            },
            ending: Ending::TimedOut {},
        })),
        ApprovalEffect::Reply(ApprovalReceipt::AnswerRefused(Refusal::TooLate {})),
    ];
    let rejected_inputs = [
        session_answer(parent(), "not-offered"),
        ApprovalEvent::Answer {
            actor: AdmittedDecider::Session(parent()),
            answer: Answer::Call {
                option_id: option_id("once"),
            },
        },
        session_answer(parent(), "kept"),
        person_answer(FactorStanding::AbsentOrStale, "kept"),
    ];

    for at in [DEADLINE, DEADLINE + 1] {
        for event in &rejected_inputs {
            let timed_out = ApprovalState::Ended {
                saved: Box::new(super::saved(&initial).clone()),
                closed: EndedApproval {
                    ending: Ending::TimedOut {},
                    delivery: Delivery::AwaitingReport {},
                    ended_at: time(at),
                },
            };
            assert_eq!(
                step(&initial, event.clone(), at),
                Ok((timed_out, expected_effects.clone())),
                "whole timeout write for {event:?} at {at}"
            );
        }
        assert_eq!(
            step(
                &initial,
                session_answer(super::fixtures::session(9), "not-offered"),
                at
            ),
            Err(refused(Refusal::NotVisible {}))
        );
    }
    let (clock_ended, _) = apply(&initial, ApprovalEvent::ClockReached, DEADLINE);
    assert_eq!(
        step(&clock_ended, rejected_inputs[0].clone(), DEADLINE),
        Err(refused(Refusal::Ended {
            ending: EndingView::TimedOut {}
        }))
    );
}

#[test]
fn parked_call_offers_the_saved_answer_to_its_exact_holder_and_settles_the_report() {
    let (call, _) = apply(
        &ApprovalState::Unraised,
        ApprovalEvent::Raise {
            raise: Box::new(call_raise()),
            destinations: vec![ApprovalDestination::People],
        },
        0,
    );
    let by = DecidedBy::Person {
        person_id: person_id(),
    };
    let answer = Answer::Call {
        option_id: option_id("once"),
    };
    let (decided, effects) = apply(
        &call,
        ApprovalEvent::Answer {
            actor: AdmittedDecider::Person {
                person_id: person_id(),
                factor: FactorStanding::Fresh,
            },
            answer: answer.clone(),
        },
        2,
    );
    let ending = Ending::Decided {
        answer: answer.clone(),
        by: by.clone(),
    };
    assert_eq!(closed(&decided).ending, ending);
    assert_eq!(closed(&decided).delivery, Delivery::AwaitingReport {});
    let holder_offer = ApprovalEffect::OfferEnding(Box::new(OfferEnding::UnprovenCall {
        request_id: request_id(),
        holder: ParkedCallHolder {
            router_run: router_run(),
        },
        ending: ending.clone(),
    }));
    assert_eq!(
        effects,
        [
            ApprovalEffect::ClearDue {
                request_id: request_id()
            },
            holder_offer.clone(),
            ApprovalEffect::Reply(ApprovalReceipt::Answer(EndingView::DecidedWithAnswer {
                answer,
                by
            })),
        ]
    );
    assert_eq!(
        apply(
            &decided,
            ApprovalEvent::OfferAgain {
                router_run: router_run()
            },
            3
        ),
        (decided.clone(), vec![holder_offer])
    );

    let (reported, effects) = apply(
        &decided,
        holder_report(router_run(), HolderReport::Applied),
        4,
    );
    assert_eq!(closed(&reported).ending, ending);
    assert_eq!(
        closed(&reported).delivery,
        Delivery::Reported {
            report: HolderReport::Applied
        }
    );
    assert_eq!(
        effects,
        [ApprovalEffect::Reply(ApprovalReceipt::Report(
            ReportDeliveryReply::DeliveryRecorded {}
        ))]
    );
}
