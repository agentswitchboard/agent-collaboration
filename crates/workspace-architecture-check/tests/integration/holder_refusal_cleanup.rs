use approval_request_protocol::{
    CallOffer, CallStopCause, CancelReason, DeliverEndingRequest, Delivery, HolderStopCause,
    InvocationDigest, ParkedCallHolder, ProviderStopCause, Raise, RaiseReply, RegisterRunReply,
    SessionCallOperation, StopReceiptEnding, StopReply, StopRequest,
};
use fleet_approvals::{
    ApprovalCollectionEffect, ApprovalCollectionEvent, ApprovalState, MAX_OUTSTANDING_PER_RUN,
    StopMarkerCause,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{
    LinkAttemptDisposition, LinkCallAnswer, LocalNegative, ProviderReply, RaiseRefusal, WaitEffect,
    WaitEvent, WaitState, WaitingRequest, WriterOutcome,
    gate_access::{GateWaitEvent, step_under_gate},
};
use switchboard_run_gate::{
    CallAttempt, CallKey, DialAttempt, LinkAttempt, RunAuthority, RunGate, RunGateEvent,
    RunGateState,
};

mod fixtures;

use self::fixtures::{
    ObservedReply, closed, key, provider, provider_stop, raise_event, register, reply, routing,
    run_event, setup,
};

const RESPONDER: u8 = 7;
type HolderMachine = WaitingRequest<u8, u8, u8>;
type HolderEffect = WaitEffect<u8, u8, u8>;

fn call(connection: u8, attempt: u64) -> CallKey<u8> {
    CallKey {
        link: LinkAttempt {
            connection,
            dial: DialAttempt::new(u64::from(connection)),
        },
        attempt: CallAttempt::new(attempt),
    }
}

#[derive(Clone)]
struct Holder {
    authority: RunAuthority<u8>,
    gate: RunGateState<u8, u8>,
    wait: WaitState<u8, u8>,
}

impl Holder {
    fn new(raise: Raise) -> Self {
        let authority = RunAuthority {
            enrollment: 1,
            router_run: raise.router_run().clone(),
        };
        Self {
            gate: RunGateState::new(authority.clone()),
            authority,
            wait: WaitState::bound(raise, RESPONDER).unwrap(),
        }
    }

    fn connect(&mut self, connection: u8, reply: RegisterRunReply) {
        let registration = call(connection, 0);
        for event in [
            RunGateEvent::DialStarted {
                link: registration.link.clone(),
            },
            RunGateEvent::Welcome {
                authority: self.authority.clone(),
                link: registration.link.clone(),
            },
            RunGateEvent::PublishRegistration {
                call: registration.clone(),
            },
            RunGateEvent::RegistrationReply {
                call: registration,
                reply,
            },
        ] {
            self.gate = RunGate::transition(&self.gate, event).unwrap().0;
        }
    }

    fn event(&mut self, event: WaitEvent<u8, u8>) -> Vec<HolderEffect> {
        let (next, effects) = HolderMachine::transition(&self.wait, event).unwrap();
        self.wait = next;
        effects
    }

    fn publish(&mut self, key: CallKey<u8>) -> HolderEffect {
        let (wait, effects) = step_under_gate(
            &self.wait,
            &self.gate,
            GateWaitEvent::Publish {
                authority: self.authority.clone(),
                call: key,
            },
        )
        .unwrap();
        self.wait = wait;
        assert_eq!(effects.len(), 1);
        effects.into_iter().next().unwrap()
    }

    fn publish_raise(&mut self, key: CallKey<u8>) -> Raise {
        let effect = self.publish(key.clone());
        match effect {
            HolderEffect::Raise {
                call,
                authority,
                request,
            } => {
                assert_eq!(call, key);
                assert_eq!(authority, self.authority);
                request
            }
            other => panic!("expected a Raise, got {other:?}"),
        }
    }

    fn finish_capacity_negative(&mut self) {
        let reply = ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
            RaiseRefusal::TooManyOutstanding,
        ));
        assert_eq!(self.wait.claimed_reply(), Some((&RESPONDER, &reply)));
        assert!(
            self.event(WaitEvent::TakeFinalPermit {
                responder: RESPONDER,
                reply: reply.clone()
            })
            .is_empty()
        );
        assert_eq!(
            self.event(WaitEvent::StartWrite {
                responder: RESPONDER,
                reply: reply.clone()
            }),
            [HolderEffect::Submit {
                responder: RESPONDER,
                reply
            }]
        );
        let outcome = WriterOutcome::WriteReturned;
        assert_eq!(
            self.event(WaitEvent::WriteFinished {
                responder: RESPONDER,
                outcome
            }),
            [
                HolderEffect::ReleaseBinding {
                    responder: RESPONDER
                },
                HolderEffect::WriterResolved {
                    responder: RESPONDER,
                    outcome
                },
            ]
        );
    }

    fn settle_stop(&mut self, key: CallKey<u8>, reply: StopReply, ending: StopReceiptEnding) {
        let event = WaitEvent::AttemptSettled {
            call: key,
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(reply)),
        };
        assert!(!self.wait.may_be_forgotten());
        assert_eq!(
            self.event(event.clone()),
            [HolderEffect::StopReconciled(ending)]
        );
        assert!(self.wait.may_be_forgotten());
        assert_eq!(
            HolderMachine::transition(&self.wait, event),
            Err(TransitionError::Stale)
        );
    }
}

#[test]
fn holder_cleanup_marker_blocks_the_delayed_raise_in_both_families() {
    capacity_retry_history(false);
}

#[test]
fn holder_cleanup_cancels_the_delayed_raise_after_admission_in_both_families() {
    capacity_retry_history(true);
}

fn capacity_retry_history(older_admitted_first: bool) {
    for held_call in [false, true] {
        let (mut central, run, requester, _) = setup();
        let target = key(&run, 1);
        let raise = if held_call {
            Raise::UnprovenCall {
                request_id: target.request_id.clone(),
                holder: ParkedCallHolder {
                    router_run: run.router_run.clone(),
                },
                invocation: InvocationDigest::from_bound_bytes(b"delayed end-for-good call"),
                offer: CallOffer {
                    operation: SessionCallOperation::EndSessionForGood {
                        session_id: requester.session_id.clone(),
                    },
                },
            }
        } else {
            provider(&target, &requester, "Delayed provider ask")
        };
        let mut holder = Holder::new(raise.clone());
        holder.connect(1, RegisterRunReply::Registered {});
        let delayed = holder.publish_raise(call(1, 1));
        assert_eq!(delayed, raise);
        assert!(
            holder
                .event(WaitEvent::AttemptSettled {
                    call: call(1, 1),
                    disposition: LinkAttemptDisposition::AnswerLost,
                })
                .is_empty()
        );
        holder.gate = RunGate::transition(
            &holder.gate,
            RunGateEvent::ConnectionLost {
                link: call(1, 1).link,
            },
        )
        .unwrap()
        .0;

        let filler = provider(&key(&run, 2), &requester, "Takes a place");
        for ordinal in 0..MAX_OUTSTANDING_PER_RUN {
            let filling = provider(
                &key(&run, u16::try_from(ordinal + 2).unwrap()),
                &requester,
                "Takes a place",
            );
            assert!(matches!(
                reply(&run_event(
                    &mut central,
                    raise_event(&run, filling, routing(None)),
                    1
                )),
                ObservedReply::Raise(RaiseReply::Receipt { .. })
            ));
        }
        let [ApprovalCollectionEffect::Registered(registered)]: [ApprovalCollectionEffect; 1] =
            register(&mut central, &run, 2).try_into().unwrap()
        else {
            panic!("retry needs the real Central registration");
        };
        holder.connect(2, registered);
        let retry = holder.publish_raise(call(2, 2));
        assert_eq!(retry, delayed);
        let refusal = reply(&run_event(
            &mut central,
            raise_event(&run, retry, routing(None)),
            3,
        ));
        assert_eq!(
            refusal,
            ObservedReply::Raise(RaiseReply::TooManyOutstanding {})
        );
        assert!(!central.requests.contains_key(&target));
        let ObservedReply::Raise(refusal) = refusal else {
            panic!("expected a Raise reply");
        };
        let refused_retry = WaitEvent::AttemptSettled {
            call: call(2, 2),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(refusal)),
        };
        let withdrawn_holder = holder.clone();
        assert!(holder.event(refused_retry.clone()).is_empty());

        // Free a place so the original frame can arrive after the refused retry.
        assert!(matches!(
            reply(&run_event(
                &mut central,
                ApprovalCollectionEvent::Stop {
                    run: run.clone(),
                    request: provider_stop(&filler),
                },
                4
            )),
            ObservedReply::Stop(StopReply::Stands { .. })
        ));

        holder.finish_capacity_negative();
        for (mut holder, completed_refusal) in [(holder, true), (withdrawn_holder, false)] {
            if !completed_refusal {
                let withdrawal = if held_call {
                    WaitEvent::LocalStop {
                        cause: HolderStopCause::CallWithdrawn,
                    }
                } else {
                    WaitEvent::ProviderWithdrew {
                        responder: RESPONDER,
                    }
                };
                assert_eq!(
                    holder.event(withdrawal),
                    [HolderEffect::ReleaseBinding {
                        responder: RESPONDER
                    }]
                );
                assert!(holder.event(refused_retry.clone()).is_empty());
            }
            let mut central = central.clone();
            let marker = match (held_call, completed_refusal) {
                (true, true) => StopMarkerCause::UnprovenCall(CallStopCause::HolderRefusedLocally),
                (true, false) => StopMarkerCause::UnprovenCall(CallStopCause::CallWithdrawn),
                (false, true) => StopMarkerCause::Provider(ProviderStopCause::HolderRefusedLocally),
                (false, false) => StopMarkerCause::Provider(ProviderStopCause::AskWithdrawn),
            };
            let digest = delayed.contents_digest().unwrap();
            let (stop_call, request) = match holder.publish(call(2, 3)) {
                HolderEffect::Stop {
                    call: matched_call,
                    authority,
                    request,
                } => {
                    assert_eq!(matched_call, call(2, 3));
                    assert_eq!(authority, holder.authority);
                    (matched_call, request)
                }
                other => panic!("expected a Stop, got {other:?}"),
            };
            let expected_request = match marker {
                StopMarkerCause::Provider(cause) => StopRequest::Provider {
                    request_id: target.request_id.clone(),
                    router_run: run.router_run.clone(),
                    contents_digest: digest.clone(),
                    cause,
                },
                StopMarkerCause::UnprovenCall(cause) => StopRequest::UnprovenCall {
                    request_id: target.request_id.clone(),
                    router_run: run.router_run.clone(),
                    contents_digest: digest.clone(),
                    cause,
                },
            };
            assert_eq!(request, expected_request);
            let original = raise_event(&run, delayed.clone(), routing(None));
            if older_admitted_first {
                assert!(matches!(
                    reply(&run_event(&mut central, original.clone(), 5)),
                    ObservedReply::Raise(RaiseReply::Receipt { .. })
                ));
                assert!(matches!(
                    central.requests.get(&target),
                    Some(ApprovalState::Waiting { .. })
                ));
            }
            let expected_ending = DeliverEndingRequest::from_stop(&delayed, &request).unwrap();
            let ending = StopReceiptEnding::Cancelled {
                reason: CancelReason::Stopped {
                    cause: marker.holder_cause(),
                },
            };
            let central_reply = reply(&run_event(
                &mut central,
                ApprovalCollectionEvent::Stop {
                    run: run.clone(),
                    request,
                },
                6,
            ));
            assert_eq!(
                central_reply,
                ObservedReply::Stop(StopReply::Stands { ending })
            );
            if older_admitted_first {
                let ended = closed(&central, &target);
                assert_eq!(ended.ending, expected_ending);
                assert_eq!(ended.delivery, Delivery::ClosedWithoutReceipt {});
            } else {
                assert_eq!(
                    central.requests.get(&target),
                    Some(&ApprovalState::StoppedBeforeRaise {
                        key: target.clone(),
                        contents_digest: digest,
                        cause: marker,
                    })
                );
                assert_eq!(
                    reply(&run_event(&mut central, original, 7)),
                    ObservedReply::Raise(RaiseReply::AlreadyStopped {})
                );
            }
            let ObservedReply::Stop(accepted) = central_reply else {
                panic!("expected Central's Stop reply");
            };
            holder.settle_stop(stop_call, accepted, ending);
        }
    }
}
