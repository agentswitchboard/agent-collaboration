use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Ending, RaiseReply, RegisterRunReply, RunRefusal,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::run_gate::CallAttempt;
use switchboard_approval_holder::waiting_request::{ProviderReply, WaitingRequest, WriterOutcome};

use super::fixtures::{
    GateEvent, Mac, RESPONDER, RequestEffect, RequestEvent, authority, call, link, raise,
};

fn offer(mac: &mut Mac, connection: u8, request: DeliverEndingRequest) -> Vec<RequestEffect> {
    mac.offer(connection, request)
}

#[test]
fn same_ending_after_lost_received_and_reconnect_has_no_finished_claim_to_replay() {
    // cases.txt “lost Received and repeated Deliver after handover”, with core's no-receipt ruling.
    let mut mac = Mac::new();
    let request = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    mac.welcome(1);
    assert_eq!(
        offer(&mut mac, 1, request.clone()),
        [RequestEffect::EndingReply(
            DeliverEndingReply::HolderNotReady {}
        )]
    );

    mac.registered(1, RegisterRunReply::Registered {});
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 1),
            request: raise()
        }]
    );
    assert_eq!(
        offer(&mut mac, 1, request.clone()),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );

    let reply = ProviderReply::CentralEnding(request.ending.clone());
    mac.wait_event(RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: reply.clone(),
    });
    assert_eq!(
        mac.wait_event(RequestEvent::StartWrite {
            responder: RESPONDER,
            reply: reply.clone()
        }),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply
        }]
    );
    mac.wait_event(RequestEvent::WriteFinished {
        responder: RESPONDER,
        outcome: WriterOutcome::WriteReturned,
    });
    assert!(mac.wait.is_closed());

    // The Received reply from connection one disappears on its way back. The provider cannot be reoffered.
    mac.lose_connection(1);
    mac.welcome(2);
    assert_eq!(
        offer(&mut mac, 2, request.clone()),
        [RequestEffect::EndingReply(
            DeliverEndingReply::HolderNotReady {}
        )]
    );
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        offer(&mut mac, 2, request),
        [RequestEffect::EndingReply(DeliverEndingReply::NotHeld {})]
    );
}

#[test]
fn enrollment_close_fences_a_claim_that_has_its_permit_but_has_not_started() {
    // cases.txt “terminal enrollment close revokes an earlier handover claim”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    let request = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    assert_eq!(
        offer(&mut mac, 1, request.clone()),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );
    let reply = ProviderReply::CentralEnding(request.ending.clone());
    mac.wait_event(RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: reply.clone(),
    });

    mac.gate_event(GateEvent::AuthenticatedEnrollmentEnded {
        enrollment: authority().enrollment,
    });
    assert!(mac.wait.is_closed());
    assert_eq!(
        WaitingRequest::transition(
            &mac.wait,
            RequestEvent::StartWrite {
                responder: RESPONDER,
                reply
            }
        ),
        Err(TransitionError::Stale)
    );
    assert!(offer(&mut mac, 1, request).is_empty());
}

#[test]
fn run_not_registered_holds_the_same_wait_until_registration_recovers() {
    // cases.txt 1482–1497. A recoverable reply does not answer the provider or lose the Raise.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    let first = mac.publish();
    mac.wait_event(RequestEvent::RaiseReply {
        call: call(1, 1),
        reply: RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {},
        },
    });
    assert_eq!(mac.gate.ready_connection(), None);
    assert_eq!(mac.wait.claimed_reply(), None);
    let request = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    assert!(offer(&mut mac, 1, request).is_empty());
    assert_eq!(
        switchboard_approval_holder::run_gate::RunGate::transition(
            &mac.gate,
            GateEvent::PublishRegistration { call: call(1, 2) }
        ),
        Err(TransitionError::Stale)
    );

    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        first,
        [RequestEffect::Raise {
            call: call(1, 1),
            request: raise()
        }]
    );
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(2, 2),
            request: raise()
        }],
        "recovery republishes the exact unacknowledged Raise"
    );
}

#[test]
fn same_answer_from_a_different_decider_is_not_an_exact_replay() {
    // cases.txt “same answer with a different decider is not an ending replay”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    let ending_from = |decider| {
        DeliverEndingRequest::from_raise(
            &raise(),
            Ending::Decided {
                answer: approval_request_protocol::Answer::Approval {
                    option_id: approval_request_protocol::OfferedOptionId::new(1).unwrap(),
                },
                by: approval_request_protocol::DecidedBy::Person { person_id: decider },
            },
        )
        .unwrap()
    };
    let first = ending_from("01890f2e-7b4c-7cc0-98c4-000000000008".parse().unwrap());
    let other = ending_from("01890f2e-7b4c-7cc0-98c4-000000000009".parse().unwrap());
    assert_eq!(
        offer(&mut mac, 1, first.clone()),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );
    assert!(matches!(
        offer(&mut mac, 1, other).as_slice(),
        [
            RequestEffect::EndingConflict { .. },
            RequestEffect::EndingReply(DeliverEndingReply::EndingDiffers {})
        ]
    ));
    assert_eq!(
        mac.wait.claimed_reply(),
        Some((&RESPONDER, &ProviderReply::CentralEnding(first.ending)))
    );
}

#[test]
fn withdrawn_exact_responder_cannot_start_even_after_received() {
    // cases.txt “exact callback disappears after claim and before any write”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    let request = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    assert_eq!(
        offer(&mut mac, 1, request.clone()),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );
    let reply = ProviderReply::CentralEnding(request.ending.clone());
    mac.wait_event(RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: reply.clone(),
    });
    assert_eq!(
        mac.wait_event(RequestEvent::CapabilityWithdrawn {
            responder: RESPONDER
        }),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }]
    );
    assert_eq!(
        WaitingRequest::transition(
            &mac.wait,
            RequestEvent::StartWrite {
                responder: RESPONDER,
                reply
            }
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        offer(&mut mac, 1, request),
        [RequestEffect::EndingReply(DeliverEndingReply::NotHeld {})]
    );
}

#[test]
fn acknowledged_wait_continues_through_loss_until_central_offers_its_ending() {
    // Mac projection of cases.txt “Central commits TimedOut while its notification is delayed”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    let saved = "2026-11-22T17:00:00Z".parse().unwrap();
    mac.wait_event(RequestEvent::RaiseReply {
        call: call(1, 1),
        reply: RaiseReply::Receipt {
            receipt: approval_request_protocol::RaiseReceipt {
                request_id: raise().request_id().clone(),
                window: approval_request_protocol::ApprovalWindow::from_first_save(saved).unwrap(),
            },
        },
    });
    mac.lose_connection(1);
    assert!(!mac.wait.is_closed());
    assert_eq!(mac.wait.claimed_reply(), None);
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        WaitingRequest::transition(
            &mac.wait,
            RequestEvent::Publish {
                ready: mac.gate.ready_run().unwrap(),
                attempt: CallAttempt::new(3)
            }
        ),
        Err(TransitionError::Stale),
        "acknowledged replay is outside the core"
    );
    assert_eq!(mac.wait.claimed_reply(), None);
    let request = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    assert_eq!(
        offer(&mut mac, 2, request),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );
}

#[test]
fn replaced_ask_keeps_its_stop_while_a_new_wait_raises_on_the_same_gate() {
    use approval_request_protocol::ProviderStopCause;
    use switchboard_approval_holder::waiting_request::WaitState;
    // cases.txt “old AskReplaced Stop crosses new Raise and old ending”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    mac.wait_event(RequestEvent::LocalStop {
        cause: ProviderStopCause::AskReplaced {},
    });
    let original_stop = mac.publish();
    assert!(matches!(
        original_stop.as_slice(),
        [RequestEffect::Stop {
            request: approval_request_protocol::StopRequest::Provider {
                cause: ProviderStopCause::AskReplaced,
                ..
            },
            ..
        }]
    ));

    let new_request =
        super::fixtures::raise_for("01890f2e-7b4c-7cc0-98c4-000000000010".parse().unwrap());
    let new_wait = WaitState::bound(new_request.clone(), 2u8).unwrap();
    let (new_wait, publication) = WaitingRequest::transition(
        &new_wait,
        RequestEvent::Publish {
            ready: mac.gate.ready_run().unwrap(),
            attempt: CallAttempt::new(3),
        },
    )
    .unwrap();
    assert_eq!(
        publication,
        [RequestEffect::Raise {
            call: call(1, 3),
            request: new_request
        }]
    );

    let old_ending = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    assert_eq!(
        offer(&mut mac, 1, old_ending),
        [RequestEffect::EndingReply(DeliverEndingReply::StopOwed {})]
    );
    assert_eq!(new_wait.claimed_reply(), None);
}

#[test]
fn unsent_stop_claims_a_local_reply_without_a_central_call() {
    use approval_request_protocol::ProviderStopCause;
    use switchboard_approval_holder::waiting_request::LocalNegative;
    // cases.txt “unsent Raise handoff and local stop share a serialization point”.
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    let cause = ProviderStopCause::TurnStopped {};
    assert!(mac.wait_event(RequestEvent::LocalStop { cause }).is_empty());
    assert_eq!(
        WaitingRequest::transition(
            &mac.wait,
            RequestEvent::Publish {
                ready: mac.gate.ready_run().unwrap(),
                attempt: CallAttempt::new(3)
            }
        ),
        Err(TransitionError::Stale),
    );
    let negative = ProviderReply::LocalNegative(LocalNegative::Stopped(cause));
    assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &negative)));
}

#[test]
fn an_exact_provider_question_uses_the_same_one_shot_writer() {
    use approval_request_protocol::{
        Answer, DecidedBy, PrivateEnding, ProviderHolder, QuestionAnswer, Raise,
    };
    use switchboard_approval_holder::waiting_request::WaitState;
    let mut mac = Mac::new();
    let question = Raise::ProviderQuestion {
        request_id: raise().request_id().clone(),
        holder: ProviderHolder {
            session_id: "01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap(),
            router_run: authority().router_run,
        },
        offer: serde_json::from_value(serde_json::json!({
            "prompt": "Choose a region",
            "fields": [
                {"kind":"singleChoice","fieldId":1,"label":"Region","description":null,
                 "required":true,"options":[{"optionId":1,"label":"Canada"}]}
            ]
        }))
        .unwrap(),
    };
    mac.wait = WaitState::bound(question.clone(), RESPONDER).unwrap();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 1),
            request: question.clone()
        }]
    );
    let request = DeliverEndingRequest::from_raise(
        &question,
        Ending::Decided {
            answer: Answer::Question {
                answer: QuestionAnswer::Declined {},
            },
            by: DecidedBy::Person {
                person_id: "01890f2e-7b4c-7cc0-98c4-000000000004".parse().unwrap(),
            },
        },
    )
    .unwrap();
    assert!(matches!(
        &request.ending,
        PrivateEnding::ProviderQuestion { .. }
    ));
    assert_eq!(
        offer(&mut mac, 1, request.clone()),
        [RequestEffect::EndingReply(DeliverEndingReply::Received {})]
    );
    let reply = ProviderReply::CentralEnding(request.ending);
    mac.wait_event(RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: reply.clone(),
    });
    assert_eq!(
        mac.wait_event(RequestEvent::StartWrite {
            responder: RESPONDER,
            reply: reply.clone()
        }),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply: reply.clone()
        }]
    );
    assert_eq!(
        WaitingRequest::transition(
            &mac.wait,
            RequestEvent::StartWrite {
                responder: RESPONDER,
                reply
            }
        ),
        Err(TransitionError::Stale)
    );
}

#[test]
fn one_wait_cannot_answer_not_held_for_another_request() {
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac.publish();
    let other = super::fixtures::raise_for("01890f2e-7b4c-7cc0-98c4-00000000000b".parse().unwrap());
    let request = DeliverEndingRequest::from_raise(&other, Ending::TimedOut {}).unwrap();
    let ingress = mac.gate.ending_ingress(&authority(), &link(1));
    assert_eq!(
        WaitingRequest::transition(&mac.wait, RequestEvent::DeliverEnding { request, ingress }),
        Err(TransitionError::Stale),
        "a different wait cannot certify absence for that request",
    );
}
