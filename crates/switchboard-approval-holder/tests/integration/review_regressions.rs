use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Ending, HolderStopCause, ProviderStopCause,
    RaiseReply, RegisterRunReply, RunRefusal, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{
    LinkAttemptDisposition, LinkCallAnswer, RequestFamily,
};
use switchboard_approval_holder::{
    run_gate::{AuthorityEnd, CallAttempt, DialAttempt, RunGate},
    waiting_request::{LocalNegative, ProviderReply, WaitState, WriterOutcome},
};

use super::fixtures::{
    GateEffect, GateEvent, Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority,
    call, ending_reply, link, raise, try_offer, try_publish,
};

const STOP: ProviderStopCause = ProviderStopCause::TurnStopped;
const LOST: ProviderStopCause = ProviderStopCause::ProviderConnectionLost;

fn ready() -> Mac {
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac
}

fn timed_out() -> DeliverEndingRequest {
    DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap()
}

fn permit(reply: ProviderReply) -> RequestEvent {
    RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply,
    }
}

fn start(reply: ProviderReply) -> RequestEvent {
    RequestEvent::StartWrite {
        responder: RESPONDER,
        reply,
    }
}

#[test]
fn stale_dial_and_registration_attempts_cannot_change_a_new_attempt() {
    let mut mac = Mac::new();
    let old = link(1);
    mac.gate_event(GateEvent::DialStarted { link: old.clone() });
    mac.lose_connection(1);
    let fresh = switchboard_approval_holder::run_gate::LinkAttempt {
        connection: 1,
        dial: DialAttempt::new(2),
    };
    mac.gate_event(GateEvent::DialStarted {
        link: fresh.clone(),
    });
    assert_eq!(
        RunGate::transition(
            &mac.gate,
            GateEvent::Welcome {
                authority: authority(),
                link: old
            }
        ),
        Err(TransitionError::Stale),
        "stale Welcome on reused carrier"
    );
    mac.gate_event(GateEvent::Welcome {
        authority: authority(),
        link: fresh.clone(),
    });

    let first = switchboard_approval_holder::run_gate::CallKey {
        link: fresh.clone(),
        attempt: CallAttempt::new(10),
    };
    let retry = switchboard_approval_holder::run_gate::CallKey {
        link: fresh,
        attempt: CallAttempt::new(11),
    };
    mac.gate_event(GateEvent::PublishRegistration {
        call: first.clone(),
    });
    mac.gate_event(GateEvent::RegistrationFailed {
        call: first.clone(),
    });
    mac.gate_event(GateEvent::PublishRegistration {
        call: retry.clone(),
    });
    for event in [
        GateEvent::RegistrationReply {
            call: first.clone(),
            reply: RegisterRunReply::Registered {},
        },
        GateEvent::RegistrationFailed { call: first },
    ] {
        assert_eq!(
            RunGate::transition(&mac.gate, event),
            Err(TransitionError::Stale),
            "late Register completion on same carrier"
        );
    }
    mac.gate_event(GateEvent::RegistrationReply {
        call: retry,
        reply: RegisterRunReply::Registered {},
    });
    assert_eq!(mac.gate.ready_connection(), Some(&1));
}

#[test]
fn same_carrier_raise_and_stop_retries_ignore_old_replies_and_failures() {
    let mut mac = ready();
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 1),
            authority: authority(),
            request: raise()
        }]
    );
    // An unobserved dropped wire copy cannot complete the pending call.
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(2)),
        Err(TransitionError::Stale),
        "wire loss without completion must leave Raise in flight"
    );
    mac.wait_event(RequestEvent::AttemptSettled {
        call: call(1, 1),
        disposition: LinkAttemptDisposition::AnswerLost,
    });
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 2),
            authority: authority(),
            request: raise()
        }]
    );
    for event in [
        RequestEvent::AttemptSettled {
            call: call(1, 1),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(
                RaiseReply::AuthorityRefused {
                    refusal: RunRefusal::RunRetired {},
                },
            )),
        },
        RequestEvent::AttemptSettled {
            call: call(1, 1),
            disposition: LinkAttemptDisposition::AnswerLost,
        },
    ] {
        assert_eq!(
            RequestMachine::transition(&mac.wait, event),
            Err(TransitionError::Stale),
            "late Raise completion on same carrier"
        );
    }
    mac.wait_event(RequestEvent::LocalStop {
        cause: HolderStopCause::TurnStopped,
    });
    let [RequestEffect::Stop { request, .. }]: [RequestEffect; 1] =
        mac.publish().try_into().unwrap()
    else {
        panic!("expected owed Stop");
    };
    mac.wait_event(RequestEvent::AttemptSettled {
        call: call(1, 3),
        disposition: LinkAttemptDisposition::AnswerLost,
    });
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(1, 4),
            authority: authority(),
            request
        }]
    );
    for event in [
        RequestEvent::AttemptSettled {
            call: call(1, 3),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                StopReply::AuthorityRefused {
                    refusal: RunRefusal::EnrollmentEnded {},
                },
            )),
        },
        RequestEvent::AttemptSettled {
            call: call(1, 3),
            disposition: LinkAttemptDisposition::AnswerLost,
        },
    ] {
        assert_eq!(
            RequestMachine::transition(&mac.wait, event),
            Err(TransitionError::Stale),
            "late Stop completion on same carrier"
        );
    }
}

#[test]
fn recoverable_refusal_retires_link_and_preserves_multiple_wait_duties() {
    let mut mac = ready();
    mac.publish();
    let mut other = WaitState::bound(
        super::fixtures::raise_for("01890f2e-7b4c-7cc0-98c4-00000000000c".parse().unwrap()),
        2u8,
    )
    .unwrap();
    other = try_publish(&other, &mac.gate, CallAttempt::new(8))
        .unwrap()
        .0;
    other = RequestMachine::transition(
        &other,
        RequestEvent::AttemptSettled {
            call: call(1, 8),
            disposition: LinkAttemptDisposition::AnswerLost,
        },
    )
    .unwrap()
    .0;
    other = RequestMachine::transition(
        &other,
        RequestEvent::LocalStop {
            cause: HolderStopCause::TurnStopped,
        },
    )
    .unwrap()
    .0;
    let (next, effects) = try_publish(&other, &mac.gate, CallAttempt::new(9)).unwrap();
    other = next;
    let [RequestEffect::Stop { request: stop, .. }]: [RequestEffect; 1] =
        effects.try_into().unwrap()
    else {
        panic!("expected owed Stop");
    };

    let (wait, effects) = RequestMachine::transition(
        &mac.wait,
        RequestEvent::AttemptSettled {
            call: call(1, 1),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(
                RaiseReply::AuthorityRefused {
                    refusal: RunRefusal::RunNotRegistered {},
                },
            )),
        },
    )
    .unwrap();
    mac.wait = wait;
    let [RequestEffect::RunRefused(refusal)]: [RequestEffect; 1] = effects.try_into().unwrap()
    else {
        panic!("expected matched refusal");
    };
    assert_eq!(
        mac.gate_event(GateEvent::HolderAuthorityRefused(refusal)),
        [GateEffect::RetireConnection { link: link(1) }],
        "recoverable refusal retires its exact link"
    );
    other = RequestMachine::transition(&other, RequestEvent::ConnectionRetired { link: link(1) })
        .unwrap()
        .0;
    assert!(mac.gate.ready_link().is_none());
    assert_eq!(
        RunGate::transition(
            &mac.gate,
            GateEvent::PublishRegistration { call: call(1, 10) }
        ),
        Err(TransitionError::Stale)
    );
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(2, 2),
            authority: authority(),
            request: raise()
        }]
    );
    assert_eq!(
        try_publish(&other, &mac.gate, CallAttempt::new(10))
            .unwrap()
            .1,
        [RequestEffect::Stop {
            call: call(2, 10),
            authority: authority(),
            request: stop
        }]
    );
}

#[test]
fn displaced_raise_terminal_refusal_fences_the_gate_before_provider_start() {
    for (refusal, reason) in [
        (RunRefusal::RunRetired {}, AuthorityEnd::RunRetired),
        (
            RunRefusal::EnrollmentEnded {},
            AuthorityEnd::EnrollmentEnded,
        ),
    ] {
        let mut mac = ready();
        mac.publish();
        mac.lose_answer(1, 1);
        mac.publish();
        mac.wait_event(RequestEvent::LocalStop {
            cause: HolderStopCause::TurnStopped,
        });
        let negative = ProviderReply::LocalNegative(LocalNegative::Stopped(STOP));
        mac.wait_event(permit(negative.clone()));

        let result = RequestMachine::transition(
            &mac.wait,
            RequestEvent::AttemptSettled {
                call: call(1, 2),
                disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(
                    RaiseReply::AuthorityRefused { refusal },
                )),
            },
        );
        assert!(
            result.is_ok(),
            "terminal refusal on displaced Raise still belongs to live run"
        );
        let (next, effects) = result.unwrap();
        mac.wait = next;
        let [RequestEffect::RunRefused(matched)]: [RequestEffect; 1] = effects.try_into().unwrap()
        else {
            panic!("expected the gate to receive the terminal refusal first");
        };
        assert_eq!(
            mac.gate_event(GateEvent::HolderAuthorityRefused(matched)),
            [GateEffect::FenceWaits {
                authority: authority(),
                reason
            }]
        );
        assert_eq!(
            RequestMachine::transition(&mac.wait, start(negative)),
            Err(TransitionError::Stale)
        );
    }
}

#[test]
fn ending_admission_discards_old_carriers_and_runs() {
    let mut mac = ready();
    mac.publish();
    mac.lose_answer(1, 1);
    mac.lose_connection(1);
    mac.welcome(2);
    assert_eq!(
        mac.offer(2, timed_out()),
        [ending_reply(2, DeliverEndingReply::HolderNotReady {})]
    );
    mac.registered(2, RegisterRunReply::Registered {});
    assert!(
        mac.offer(1, timed_out()).is_empty(),
        "stale carrier cannot receive an ending reply"
    );
    let mut other = authority();
    other.router_run = "01890f2e-7b4c-7cc0-98c4-00000000000c".parse().unwrap();
    assert!(
        try_offer(&mac.wait, &mac.gate, other, link(2), timed_out())
            .unwrap()
            .1
            .is_empty(),
        "stale run cannot receive an ending reply"
    );
    other = authority();
    other.enrollment = 2;
    assert!(
        try_offer(&mac.wait, &mac.gate, other, link(2), timed_out())
            .unwrap()
            .1
            .is_empty()
    );
    assert!(mac.wait.claimed_reply().is_none());
    mac.publish();
    assert_eq!(
        mac.offer(2, timed_out()),
        [ending_reply(2, DeliverEndingReply::Received {})]
    );
}

#[test]
fn known_provider_disconnect_releases_once_without_submitting_and_keeps_stop() {
    assert_eq!(
        RequestMachine::transition(
            &ready().wait,
            RequestEvent::LocalStop {
                cause: HolderStopCause::AskWithdrawn
            }
        ),
        Err(TransitionError::Stale),
        "agent take-back requires the exact responder"
    );
    for phase in 0..5 {
        let mut mac = ready();
        if phase > 0 {
            mac.publish();
        }
        let ending = timed_out();
        let reply = ProviderReply::CentralEnding(ending.ending().clone());
        if phase >= 2 {
            mac.offer(1, ending);
        }
        if phase >= 3 {
            mac.wait_event(permit(reply.clone()));
        }
        if phase >= 4 {
            mac.wait_event(start(reply.clone()));
        }

        let effects = mac.wait_event(RequestEvent::LocalStop {
            cause: HolderStopCause::ProviderConnectionLost,
        });
        if phase == 4 {
            assert!(effects.is_empty());
            assert_eq!(
                mac.wait_event(RequestEvent::WriteFinished {
                    responder: RESPONDER,
                    outcome: WriterOutcome::OutcomeUnknown
                }),
                [
                    RequestEffect::ReleaseBinding {
                        responder: RESPONDER
                    },
                    RequestEffect::WriterResolved {
                        responder: RESPONDER,
                        outcome: WriterOutcome::OutcomeUnknown
                    }
                ]
            );
        } else {
            assert_eq!(
                effects,
                [RequestEffect::ReleaseBinding {
                    responder: RESPONDER
                }],
                "known loss must release before start"
            );
            assert!(mac.wait.claimed_reply().is_none());
        }
        let dead = ProviderReply::LocalNegative(LocalNegative::Stopped(LOST));
        assert_eq!(
            RequestMachine::transition(&mac.wait, permit(dead.clone())),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            RequestMachine::transition(&mac.wait, start(dead)),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            RequestMachine::transition(&mac.wait, start(reply)),
            Err(TransitionError::Stale)
        );
        if phase == 1 {
            mac.lose_answer(1, 1);
            assert_eq!(
                mac.publish(),
                [RequestEffect::Stop {
                    call: call(1, 2),
                    authority: authority(),
                    request: StopRequest::Provider {
                        request_id: raise().request_id().clone(),
                        router_run: authority().router_run,
                        contents_digest: raise().contents_digest().unwrap(),
                        cause: LOST,
                    }
                }]
            );
        } else {
            assert!(!mac.wait.holds_place());
        }
    }
}

#[test]
fn stop_digest_conflict_is_reported_as_unresolved_independently_of_writer() {
    let mut mac = ready();
    mac.publish();
    mac.lose_answer(1, 1);
    mac.wait_event(RequestEvent::LocalStop {
        cause: HolderStopCause::TurnStopped,
    });
    mac.publish();
    // Central returns NotVisible for a Stop whose digest differs from the incumbent.
    assert_eq!(
        mac.wait_event(RequestEvent::AttemptSettled {
            call: call(1, 2),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                StopReply::NotVisible {}
            ))
        }),
        [RequestEffect::StopNotVisible {
            request_id: raise().request_id().clone(),
            contents_digest: raise().contents_digest().unwrap()
        }],
        "NotVisible cannot be reported as standing"
    );
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(3)),
        Err(TransitionError::Stale)
    );
    let reply = ProviderReply::LocalNegative(LocalNegative::Stopped(STOP));
    mac.wait_event(permit(reply.clone()));
    assert_eq!(
        mac.wait_event(start(reply.clone())),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply
        }]
    );
}

#[test]
fn local_capacity_refusal_is_never_raised_and_uses_the_exact_writer_once() {
    let mut mac = ready();
    mac.wait = WaitState::at_capacity(RequestFamily::ProviderAsk, RESPONDER);
    let reply = ProviderReply::LocalNegative(LocalNegative::HolderAtCapacity);
    assert_eq!(
        mac.wait.claimed_reply(),
        Some((&RESPONDER, &reply)),
        "local capacity is distinct from a Central refusal"
    );
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(1)),
        Err(TransitionError::Stale)
    );
    mac.wait_event(permit(reply.clone()));
    assert_eq!(
        mac.wait_event(start(reply.clone())),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply: reply.clone()
        }]
    );
    assert_eq!(
        RequestMachine::transition(&mac.wait, start(reply)),
        Err(TransitionError::Stale)
    );
}
