use approval_request_protocol::{
    Answer, CallCancellation, CallOffer, CallStopCause, CancelReason, Delivery, Ending,
    HolderEnding, HolderStopCause, InvocationDigest, ParkedCallEnding, ParkedCallHolder,
    PassOnReply, PassOnRequest, PrivateEnding, ProviderCancellation, ProviderStopCause, Raise,
    RaiseReply, RequestContentsDigest, SessionCallDecision, SessionCallOperation,
    StopReceiptEnding, StopReply, StopRequest,
};
use fleet_approvals::{
    AdmittedDecider, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalDecisionReply,
    ApprovalEffect,
};

use super::fixtures::{
    HOUR, ObservedReply, answer, closed, decide_event, key, person, provider, raise_event, reply,
    routing, run_event, setup,
};

#[test]
fn parked_call_replay_and_settlement_do_not_depend_on_the_target_session() {
    let (mut collection, run, target, parent) = setup();
    let approval_key = key(&run, 1);
    let raise = Raise::UnprovenCall {
        request_id: approval_key.request_id.clone(),
        holder: ParkedCallHolder {
            router_run: run.router_run.clone(),
        },
        invocation: InvocationDigest::from_bound_bytes(b"held end request"),
        offer: CallOffer {
            operation: SessionCallOperation::EndSessionForGood {
                session_id: target.session_id.clone(),
            },
        },
    };
    let initial_raise = raise_event(&run, raise, routing(None));
    let first = run_event(&mut collection, initial_raise.clone(), 0);
    assert!(matches!(
        reply(&first),
        ObservedReply::Raise(RaiseReply::Receipt { .. })
    ));
    assert!(first.iter().all(|effect| !matches!(
        effect,
        ApprovalCollectionEffect::Request(ApprovalEffect::StoreSessionNotice { .. })
    )));
    run_event(
        &mut collection,
        ApprovalCollectionEvent::EndSession { session: target },
        1,
    );
    let replay = run_event(&mut collection, initial_raise, 2);
    assert_eq!(reply(&replay), reply(&first));
    assert_eq!(replay.len(), 1);
    let pass_on = ApprovalCollectionEvent::PassOn {
        key: approval_key.clone(),
        parent: parent.clone(),
        request: PassOnRequest {
            request_id: approval_key.request_id.clone(),
        },
    };
    assert_eq!(
        reply(&run_event(&mut collection, pass_on, 3)),
        ObservedReply::PassOn(PassOnReply::NotVisible {}),
    );
    for (actor, suffix, answer, result) in [
        (
            AdmittedDecider::Session(parent),
            11,
            Answer::UnprovenCall {
                decision: SessionCallDecision::Allow,
            },
            ApprovalDecisionReply::NotVisible {},
        ),
        (person(12), 12, answer(1), ApprovalDecisionReply::Invalid {}),
    ] {
        let action = decide_event(&approval_key, actor, suffix, answer);
        assert_eq!(
            reply(&run_event(&mut collection, action, 4)),
            ObservedReply::Decide(result),
        );
    }
    let approve = decide_event(
        &approval_key,
        person(12),
        13,
        Answer::UnprovenCall {
            decision: SessionCallDecision::Allow,
        },
    );
    assert!(matches!(
        reply(&run_event(&mut collection, approve, 5)),
        ObservedReply::Decide(ApprovalDecisionReply::SavedDecision { .. })
    ));
    assert_eq!(
        closed(&collection, &approval_key).delivery,
        Delivery::Owed {}
    );
}

#[test]
fn a_parked_stop_at_the_deadline_closes_delivery_with_the_timeout_standing() {
    for (at, expected) in [
        (
            HOUR - 1,
            Ending::Cancelled {
                reason: CancelReason::Stopped {
                    cause: HolderStopCause::CallWithdrawn,
                },
            },
        ),
        (HOUR, Ending::TimedOut {}),
    ] {
        let (mut collection, run, target, _) = setup();
        let approval_key = key(&run, 1);
        let raise = Raise::UnprovenCall {
            request_id: approval_key.request_id.clone(),
            holder: ParkedCallHolder {
                router_run: run.router_run.clone(),
            },
            invocation: InvocationDigest::from_bound_bytes(b"held end request"),
            offer: CallOffer {
                operation: SessionCallOperation::EndSessionForGood {
                    session_id: target.session_id,
                },
            },
        };
        let request = StopRequest::UnprovenCall {
            request_id: approval_key.request_id.clone(),
            router_run: run.router_run.clone(),
            contents_digest: raise.contents_digest().unwrap(),
            cause: approval_request_protocol::CallStopCause::CallWithdrawn,
        };
        run_event(&mut collection, raise_event(&run, raise, routing(None)), 0);
        let effects = run_event(
            &mut collection,
            ApprovalCollectionEvent::Stop { run, request },
            at,
        );
        assert_eq!(
            reply(&effects),
            ObservedReply::Stop(StopReply::Stands {
                ending: if at == HOUR {
                    StopReceiptEnding::TimedOut {}
                } else {
                    StopReceiptEnding::Cancelled {
                        reason: CancelReason::Stopped {
                            cause: HolderStopCause::CallWithdrawn,
                        },
                    }
                },
            })
        );
        let ending = closed(&collection, &approval_key);
        assert_eq!(ending.ending.clone().into_ending(), expected);
        assert_eq!(ending.delivery, Delivery::ClosedWithoutReceipt {});
        assert!(effects.contains(&ApprovalCollectionEffect::Request(
            ApprovalEffect::ClearDue { key: approval_key }
        )));
    }
}

#[test]
fn stop_markers_replay_only_inside_their_original_stop_family() {
    for parked_first in [false, true] {
        let (mut collection, run, target, _) = setup();
        let approval_key = key(&run, 1);
        let raise = Raise::UnprovenCall {
            request_id: approval_key.request_id.clone(),
            holder: ParkedCallHolder {
                router_run: run.router_run.clone(),
            },
            invocation: InvocationDigest::from_bound_bytes(b"held end request"),
            offer: CallOffer {
                operation: SessionCallOperation::EndSessionForGood {
                    session_id: target.session_id,
                },
            },
        };
        let digest = raise.contents_digest().unwrap();
        let provider = StopRequest::Provider {
            request_id: approval_key.request_id.clone(),
            router_run: run.router_run.clone(),
            contents_digest: digest.clone(),
            cause: ProviderStopCause::AskWithdrawn,
        };
        let parked = StopRequest::UnprovenCall {
            request_id: approval_key.request_id,
            router_run: run.router_run.clone(),
            contents_digest: digest,
            cause: CallStopCause::CallWithdrawn,
        };
        let (first, cross) = if parked_first {
            (parked, provider)
        } else {
            (provider, parked)
        };
        let expected = ObservedReply::Stop(StopReply::Stands {
            ending: StopReceiptEnding::Cancelled {
                reason: CancelReason::Stopped {
                    cause: first.cause(),
                },
            },
        });
        let stop = |request| ApprovalCollectionEvent::Stop {
            run: run.clone(),
            request,
        };
        assert_eq!(
            reply(&run_event(&mut collection, stop(first.clone()), 0)),
            expected
        );
        let prior = collection.requests.clone();
        assert_eq!(
            reply(&run_event(&mut collection, stop(cross), 1)),
            ObservedReply::Stop(StopReply::NotVisible {}),
        );
        assert_eq!(collection.requests, prior);
        assert_eq!(reply(&run_event(&mut collection, stop(first), 2)), expected);
    }
}

fn local_refusal_stop(raise: &Raise, contents_digest: RequestContentsDigest) -> StopRequest {
    let request_id = raise.request_id().clone();
    let router_run = raise.router_run().clone();
    match raise {
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            cause: ProviderStopCause::HolderRefusedLocally,
        },
        Raise::UnprovenCall { .. } => StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            cause: CallStopCause::HolderRefusedLocally,
        },
    }
}

#[test]
fn local_refusal_stops_both_request_families_before_raise_while_waiting_and_after_ending() {
    let reason = CancelReason::Stopped {
        cause: HolderStopCause::HolderRefusedLocally,
    };
    let cancelled = StopReceiptEnding::Cancelled { reason };
    let timed_out = StopReceiptEnding::TimedOut {};
    for parked in [false, true] {
        let (empty, run, target, _) = setup();
        let approval_key = key(&run, 1);
        let raise = if parked {
            Raise::UnprovenCall {
                request_id: approval_key.request_id.clone(),
                holder: ParkedCallHolder {
                    router_run: run.router_run.clone(),
                },
                invocation: InvocationDigest::from_bound_bytes(b"held end request"),
                offer: CallOffer {
                    operation: SessionCallOperation::EndSessionForGood {
                        session_id: target.session_id.clone(),
                    },
                },
            }
        } else {
            provider(&approval_key, &target, "Review")
        };
        let stop = ApprovalCollectionEvent::Stop {
            run: run.clone(),
            request: local_refusal_stop(&raise, raise.contents_digest().unwrap()),
        };
        let raise = raise_event(&run, raise, routing(None));

        let mut unseen = empty.clone();
        assert_eq!(
            reply(&run_event(&mut unseen, stop.clone(), 0)),
            ObservedReply::Stop(StopReply::Stands { ending: cancelled })
        );
        assert_eq!(
            reply(&run_event(&mut unseen, raise.clone(), 1)),
            ObservedReply::Raise(RaiseReply::AlreadyStopped {})
        );

        let mut waiting = empty;
        assert!(matches!(
            reply(&run_event(&mut waiting, raise, 0)),
            ObservedReply::Raise(RaiseReply::Receipt { .. })
        ));
        let (stopped_ending, timed_out_ending) = if parked {
            (
                PrivateEnding::UnprovenCall {
                    ending: ParkedCallEnding::Cancelled {
                        reason: CallCancellation::Stopped {
                            cause: CallStopCause::HolderRefusedLocally,
                        },
                    },
                },
                PrivateEnding::UnprovenCall {
                    ending: ParkedCallEnding::TimedOut {},
                },
            )
        } else {
            (
                PrivateEnding::ProviderApproval {
                    ending: HolderEnding::Cancelled {
                        reason: ProviderCancellation::Stopped {
                            cause: ProviderStopCause::HolderRefusedLocally,
                        },
                    },
                },
                PrivateEnding::ProviderApproval {
                    ending: HolderEnding::TimedOut {},
                },
            )
        };
        for (at, receipt, ending) in [
            (HOUR - 1, cancelled, stopped_ending),
            (HOUR, timed_out, timed_out_ending),
        ] {
            let mut collection = waiting.clone();
            let effects = run_event(&mut collection, stop.clone(), at);
            assert_eq!(
                reply(&effects),
                ObservedReply::Stop(StopReply::Stands { ending: receipt })
            );
            let recorded = closed(&collection, &approval_key);
            assert_eq!(recorded.ending.ending(), &ending);
            assert_eq!(recorded.delivery, Delivery::ClosedWithoutReceipt {});
            assert!(effects.contains(&ApprovalCollectionEffect::Request(
                ApprovalEffect::ClearDue {
                    key: approval_key.clone()
                },
            )));
            assert!(!effects.iter().any(|effect| matches!(
                effect,
                ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding { .. })
            )));
        }

        let mut ended = waiting;
        let decision = if parked {
            Answer::UnprovenCall {
                decision: SessionCallDecision::Allow,
            }
        } else {
            answer(1)
        };
        let decide = decide_event(&approval_key, person(12), 13, decision);
        assert!(matches!(
            reply(&run_event(&mut ended, decide, 1)),
            ObservedReply::Decide(ApprovalDecisionReply::SavedDecision { .. })
        ));
        let prior = closed(&ended, &approval_key).clone();
        assert_eq!(prior.delivery, Delivery::Owed {});
        assert_eq!(
            reply(&run_event(&mut ended, stop, HOUR + 1)),
            ObservedReply::Stop(StopReply::Stands {
                ending: StopReceiptEnding::DecidedEarlier {},
            })
        );
        let recorded = closed(&ended, &approval_key);
        assert_eq!(recorded.ending, prior.ending);
        assert_eq!(recorded.ended_at, prior.ended_at);
        assert_eq!(recorded.delivery, Delivery::ClosedWithoutReceipt {});
    }
}

#[test]
fn local_refusal_markers_reject_the_other_family_even_at_the_same_digest() {
    let (empty, run, target, _) = setup();
    let approval_key = key(&run, 1);
    let provider = provider(&approval_key, &target, "Review");
    let parked = Raise::UnprovenCall {
        request_id: approval_key.request_id,
        holder: ParkedCallHolder {
            router_run: run.router_run.clone(),
        },
        invocation: InvocationDigest::from_bound_bytes(b"held end request"),
        offer: CallOffer {
            operation: SessionCallOperation::EndSessionForGood {
                session_id: target.session_id,
            },
        },
    };
    let stop = |request| ApprovalCollectionEvent::Stop {
        run: run.clone(),
        request,
    };
    let cancelled = ObservedReply::Stop(StopReply::Stands {
        ending: StopReceiptEnding::Cancelled {
            reason: CancelReason::Stopped {
                cause: HolderStopCause::HolderRefusedLocally,
            },
        },
    });

    for (original, cross) in [(&provider, &parked), (&parked, &provider)] {
        // Give the marker the opposite Raise's exact digest to isolate the family check.
        let digest = cross.contents_digest().unwrap();
        let first = stop(local_refusal_stop(original, digest.clone()));
        let mut collection = empty.clone();
        assert_eq!(
            reply(&run_event(&mut collection, first.clone(), 0)),
            cancelled
        );
        let prior = collection.requests.clone();
        assert_eq!(
            reply(&run_event(
                &mut collection,
                stop(local_refusal_stop(cross, digest)),
                1,
            )),
            ObservedReply::Stop(StopReply::NotVisible {})
        );
        assert_eq!(
            reply(&run_event(
                &mut collection,
                raise_event(&run, cross.clone(), routing(None)),
                2,
            )),
            ObservedReply::Raise(RaiseReply::ContentsDiffer {})
        );
        assert_eq!(collection.requests, prior);
        assert_eq!(reply(&run_event(&mut collection, first, 3)), cancelled);
    }
}
