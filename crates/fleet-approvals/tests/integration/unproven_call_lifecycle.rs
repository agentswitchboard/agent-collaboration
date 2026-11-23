use approval_request_protocol::{
    Answer, CallOffer, CancelReason, Delivery, Ending, HolderStopCause, InvocationDigest,
    ParkedCallHolder, PassOnReply, PassOnRequest, Raise, RaiseReply, SessionCallDecision,
    SessionCallOperation, StopReceiptEnding, StopReply, StopRequest,
};
use fleet_approvals::{
    AdmittedDecider, ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalDecisionReply,
    ApprovalEffect, FactorStanding,
};

use super::fixtures::{
    HOUR, ObservedReply, closed, decide_event, key, person, raise_event, reply, routing, run_event,
    setup,
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
    for (actor, suffix, result) in [
        (
            AdmittedDecider::Session(parent),
            11,
            ApprovalDecisionReply::NotVisible {},
        ),
        (
            person(12, FactorStanding::AbsentOrStale),
            12,
            ApprovalDecisionReply::Invalid {},
        ),
    ] {
        let action = decide_event(
            &approval_key,
            actor,
            suffix,
            Answer::UnprovenCall {
                decision: SessionCallDecision::Allow,
            },
        );
        assert_eq!(
            reply(&run_event(&mut collection, action, 4)),
            ObservedReply::Decide(result),
        );
    }
    let approve = decide_event(
        &approval_key,
        person(12, FactorStanding::Fresh),
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
    use approval_request_protocol::{CallStopCause, ProviderStopCause};

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
