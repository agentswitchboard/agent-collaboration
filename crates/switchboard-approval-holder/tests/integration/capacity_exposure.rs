use approval_request_protocol::{
    ApprovalWindow, CallStopCause, CancelReason, DeliverEndingReply, DeliverEndingRequest, Ending,
    HolderStopCause, ProviderStopCause, Raise, RaiseReceipt, RaiseReply, RegisterRunReply,
    StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::CallAttempt,
    waiting_request::{
        LinkAttemptDisposition, LinkCallAnswer, LocalNegative, ProviderReply, RaiseRefusal,
        WriterOutcome,
    },
};

use super::fixtures::{
    Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call, call_raise,
    ending_reply, raise, try_publish,
};

const CAPACITY: RaiseReply = RaiseReply::TooManyOutstanding {};
const NEGATIVE: ProviderReply = ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
    RaiseRefusal::TooManyOutstanding,
));

fn families() -> [(Raise, HolderStopCause, RequestEvent); 2] {
    [
        (
            raise(),
            HolderStopCause::AskWithdrawn,
            RequestEvent::ProviderWithdrew {
                responder: RESPONDER,
            },
        ),
        (
            call_raise(),
            HolderStopCause::CallWithdrawn,
            RequestEvent::LocalStop {
                cause: HolderStopCause::CallWithdrawn,
            },
        ),
    ]
}

fn register(mac: &mut Mac, connection: u8) {
    mac.welcome(connection);
    mac.registered(connection, RegisterRunReply::Registered {});
}

fn assert_raise(mac: &mut Mac, request: &Raise, connection: u8, attempt: u64) {
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(connection, attempt),
            authority: authority(),
            request: request.clone(),
        }]
    );
}

fn answer_raise(connection: u8, attempt: u64, reply: RaiseReply) -> RequestEvent {
    RequestEvent::AttemptSettled {
        call: call(connection, attempt),
        disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(reply)),
    }
}

fn assert_no_publication(mac: &Mac) {
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(99)),
        Err(TransitionError::Stale)
    );
}

fn permit() -> RequestEvent {
    RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: NEGATIVE,
    }
}

fn start() -> RequestEvent {
    RequestEvent::StartWrite {
        responder: RESPONDER,
        reply: NEGATIVE,
    }
}

fn finish_negative(mac: &mut Mac) {
    assert!(mac.wait_event(permit()).is_empty());
    assert_eq!(
        mac.wait_event(start()),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply: NEGATIVE
        }]
    );
    let outcome = WriterOutcome::WriteReturned;
    assert_eq!(
        mac.wait_event(RequestEvent::WriteFinished {
            responder: RESPONDER,
            outcome
        }),
        [
            RequestEffect::ReleaseBinding {
                responder: RESPONDER
            },
            RequestEffect::WriterResolved {
                responder: RESPONDER,
                outcome
            },
        ]
    );
}

fn refused_retry(request: &Raise) -> Mac {
    let mut mac = Mac::with_raise(request.clone());
    register(&mut mac, 1);
    assert_raise(&mut mac, request, 1, 1);
    mac.lose_answer(1, 1);
    mac.lose_connection(1);
    register(&mut mac, 2);
    assert_raise(&mut mac, request, 2, 2);
    assert!(mac.wait_event(answer_raise(2, 2, CAPACITY)).is_empty());
    assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &NEGATIVE)));
    assert_no_publication(&mac);
    mac
}

fn expected_stop(raise: &Raise) -> StopRequest {
    let request_id = raise.request_id().clone();
    let router_run = raise.router_run().clone();
    let contents_digest = raise.contents_digest().unwrap();
    match raise {
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            cause: ProviderStopCause::AskWithdrawn,
        },
        Raise::UnprovenCall { .. } => StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            cause: CallStopCause::CallWithdrawn,
        },
    }
}

fn delayed_admission(raise: &Raise, marker: Option<&StopRequest>) -> RaiseReply {
    let Some(marker) = marker else {
        return admitted_receipt(raise);
    };
    let (request_id, router_run, digest, family_matches) = match marker {
        StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            ..
        } => (
            request_id,
            router_run,
            contents_digest,
            matches!(
                raise,
                Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. }
            ),
        ),
        StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            ..
        } => (
            request_id,
            router_run,
            contents_digest,
            matches!(raise, Raise::UnprovenCall { .. }),
        ),
    };
    if request_id != raise.request_id() || router_run != raise.router_run() {
        admitted_receipt(raise)
    } else if family_matches && digest == &raise.contents_digest().unwrap() {
        RaiseReply::AlreadyStopped {}
    } else {
        RaiseReply::ContentsDiffer {}
    }
}

#[test]
fn withdrawal_after_capacity_refused_retry_stops_the_delayed_original_in_both_families() {
    for (delayed_raise, cause, withdraw) in families() {
        for permitted in [false, true] {
            let mut mac = refused_retry(&delayed_raise);
            if permitted {
                assert!(mac.wait_event(permit()).is_empty());
            }
            assert_eq!(
                mac.wait_event(withdraw.clone()),
                [RequestEffect::ReleaseBinding {
                    responder: RESPONDER
                }]
            );
            assert_eq!(
                RequestMachine::transition(&mac.wait, start()),
                Err(TransitionError::Stale)
            );
            assert!(
                !mac.wait.may_be_forgotten(),
                "the older Raise can still reach Central"
            );
            let publication = mac.publish();
            assert_eq!(publication.len(), 1);
            let (stop_call, request) = publication
                .into_iter()
                .find_map(|effect| {
                    if let RequestEffect::Stop { call, request, .. } = effect {
                        Some((call, request))
                    } else {
                        None
                    }
                })
                .unwrap();

            // The modeled Central is now below capacity. The Stop wins the race
            // to the unraised row and installs the marker before A arrives.
            assert_eq!(request, expected_stop(&delayed_raise));
            assert_eq!(stop_call, call(2, 3));
            assert_eq!(
                delayed_admission(&delayed_raise, None),
                admitted_receipt(&delayed_raise)
            );
            assert_eq!(
                delayed_admission(&delayed_raise, Some(&request)),
                RaiseReply::AlreadyStopped {}
            );
            let ending = StopReceiptEnding::Cancelled {
                reason: CancelReason::Stopped { cause },
            };
            assert_eq!(
                mac.wait_event(RequestEvent::AttemptSettled {
                    call: stop_call,
                    disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                        StopReply::Stands { ending }
                    )),
                }),
                [RequestEffect::StopReconciled(ending)]
            );
            assert!(mac.wait.may_be_forgotten());
        }
    }
}

#[test]
fn completed_capacity_negative_keeps_the_old_raise_without_publishing() {
    for (request, _, _) in families() {
        let mut mac = refused_retry(&request);
        finish_negative(&mut mac);
        assert!(mac.wait.holds_place());
        assert!(mac.wait.holds_capacity_refusal_exposure());
        assert!(!mac.wait.may_be_forgotten());
        assert!(mac.wait.claimed_reply().is_none());
        assert_no_publication(&mac);
    }
}

#[test]
fn exact_earlier_non_admission_frees_a_completed_capacity_wait() {
    for (request, _, _) in families() {
        for disposition in [
            LinkAttemptDisposition::NotReached,
            LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(CAPACITY)),
        ] {
            let mut mac = refused_retry(&request);
            finish_negative(&mut mac);
            assert!(!mac.wait.may_be_forgotten());
            assert_eq!(
                RequestMachine::transition(
                    &mac.wait,
                    RequestEvent::AttemptSettled {
                        call: call(2, 1),
                        disposition: disposition.clone(),
                    }
                ),
                Err(TransitionError::Stale)
            );
            assert!(
                mac.wait_event(RequestEvent::AttemptSettled {
                    call: call(1, 1),
                    disposition
                })
                .is_empty()
            );
            assert!(mac.wait.may_be_forgotten());
            assert_no_publication(&mac);
        }
    }
}

fn admitted_receipt(request: &Raise) -> RaiseReply {
    RaiseReply::Receipt {
        receipt: RaiseReceipt {
            request_id: request.request_id().clone(),
            window: ApprovalWindow::from_first_save("2026-11-22T17:00:00Z".parse().unwrap())
                .unwrap(),
        },
    }
}

#[test]
fn late_receipt_keeps_admission_and_an_exact_ending_frees_the_wait() {
    for (request, _, _) in families() {
        let mut mac = refused_retry(&request);
        finish_negative(&mut mac);
        assert!(
            mac.wait_event(answer_raise(1, 1, admitted_receipt(&request)))
                .is_empty()
        );
        assert!(
            !mac.wait.may_be_forgotten(),
            "a receipt can still name a waiting row"
        );
        assert_no_publication(&mac);
        assert!(mac.wait.holds_capacity_refusal_exposure());
        let ending = DeliverEndingRequest::from_raise(&request, Ending::TimedOut {}).unwrap();
        assert_eq!(
            mac.offer(2, ending),
            [ending_reply(2, DeliverEndingReply::NotHeld {})]
        );
        assert!(mac.wait.may_be_forgotten());
        assert!(!mac.wait.holds_capacity_refusal_exposure());
    }
}

#[test]
fn first_capacity_refusal_completes_without_an_owed_stop() {
    for (request, _, _) in families() {
        let mut mac = Mac::with_raise(request.clone());
        register(&mut mac, 1);
        assert_raise(&mut mac, &request, 1, 1);
        assert!(mac.wait_event(answer_raise(1, 1, CAPACITY)).is_empty());
        finish_negative(&mut mac);
        assert!(mac.wait.may_be_forgotten());
        assert!(!mac.wait.holds_capacity_refusal_exposure());
        assert_no_publication(&mac);
    }
}

#[test]
fn each_earlier_non_admission_clears_only_its_attempt() {
    for (request, _, _) in families() {
        let mut mac = Mac::with_raise(request.clone());
        register(&mut mac, 1);
        for attempt in [1, 2] {
            assert_raise(&mut mac, &request, 1, attempt);
            mac.lose_answer(1, attempt);
        }
        mac.lose_connection(1);
        register(&mut mac, 2);
        assert_raise(&mut mac, &request, 2, 3);
        mac.wait_event(answer_raise(2, 3, CAPACITY));
        finish_negative(&mut mac);
        assert!(mac.wait_event(answer_raise(1, 1, CAPACITY)).is_empty());
        assert!(mac.wait.holds_capacity_refusal_exposure());
        assert!(!mac.wait.may_be_forgotten());
        assert_eq!(
            RequestMachine::transition(&mac.wait, answer_raise(1, 1, CAPACITY)),
            Err(TransitionError::Stale)
        );
        assert!(mac.wait_event(answer_raise(1, 2, CAPACITY)).is_empty());
        assert!(mac.wait.may_be_forgotten());
    }
}

#[test]
fn late_already_stopped_settles_the_binding_for_both_families() {
    for (request, _, _) in families() {
        let mut mac = refused_retry(&request);
        finish_negative(&mut mac);
        assert!(mac.wait.holds_capacity_refusal_exposure());
        assert!(
            mac.wait_event(answer_raise(1, 1, RaiseReply::AlreadyStopped {}))
                .is_empty()
        );
        assert!(mac.wait.may_be_forgotten());
        assert_no_publication(&mac);
    }
}
