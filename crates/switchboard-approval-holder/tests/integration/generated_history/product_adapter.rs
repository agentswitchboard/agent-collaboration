#![expect(
    clippy::panic,
    clippy::unwrap_in_result,
    reason = "Unexpected reducer outcomes must fail the generated history at the boundary."
)]

use approval_request_protocol::Raise;
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::{RunGate, RunGateState},
    waiting_request::{LinkAttemptDisposition, LinkCallAnswer, WaitRefusal, WaitState},
};

use super::super::fixtures::{
    GateEffect, GateEvent, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority,
    try_offer, try_publish,
};
use super::{Link, Observation, WaitInput};

pub(super) struct Product {
    pub(super) gate: RunGateState<u8, u8>,
    pub(super) wait: WaitState<u8, u8>,
}

impl Product {
    pub(super) fn new(request: Raise) -> Self {
        Self {
            gate: RunGateState::new(authority()),
            wait: WaitState::bound(request, RESPONDER).unwrap(),
        }
    }

    pub(super) fn gate(&mut self, event: GateEvent) -> Option<Vec<Observation>> {
        let lost = match &event {
            GateEvent::ConnectionLost { link } => Some(link.clone()),
            _ => None,
        };
        let (next, effects) = match RunGate::transition(&self.gate, event) {
            Ok(result) => result,
            Err(TransitionError::Stale) => return None,
            Err(TransitionError::Domain(impossible)) => match impossible {},
            Err(error @ TransitionError::NotAllowed { .. }) => {
                panic!("Unexpected gate refusal: {error:?}")
            }
        };
        self.gate = next;
        let mut observed = Vec::new();
        for effect in effects {
            observed.push(Observation::Gate(effect.clone()));
            match effect {
                GateEffect::RetireConnection { link } => self.retire(link),
                GateEffect::FenceWaits { .. } => observed.extend(
                    self.apply_local(RequestEvent::AuthorityEnded)
                        .unwrap()
                        .into_iter()
                        .map(Observation::Wait),
                ),
                GateEffect::Register { .. } => {}
            }
        }
        if let Some(link) = lost {
            self.retire(link);
        }
        Some(observed)
    }

    fn retire(&mut self, link: Link) {
        if let Some(effects) = self.apply_local(RequestEvent::ConnectionRetired { link }) {
            assert!(effects.is_empty());
        }
    }

    fn apply_local(&mut self, event: RequestEvent) -> Option<Vec<RequestEffect>> {
        match RequestMachine::transition(&self.wait, event) {
            Ok((state, effects)) => {
                self.wait = state;
                Some(effects)
            }
            Err(TransitionError::Stale) => None,
            Err(error) => panic!("Unexpected wait refusal: {error:?}"),
        }
    }

    pub(super) fn wait(&mut self, input: &WaitInput) -> Option<Vec<Observation>> {
        let result = match input {
            WaitInput::Publish(attempt) => try_publish(&self.wait, &self.gate, *attempt),
            WaitInput::Ending { link, request } => try_offer(
                &self.wait,
                &self.gate,
                authority(),
                link.clone(),
                request.clone(),
            ),
            other => RequestMachine::transition(&self.wait, Self::local_event(other)?),
        };
        let (next, effects) = match result {
            Ok(result) => result,
            Err(TransitionError::Stale) => return None,
            Err(TransitionError::Domain(WaitRefusal::StopCauseForDifferentKind)) => {
                return Some(vec![Observation::StopCauseRefused]);
            }
            Err(error) => panic!("Unexpected wait refusal: {error:?}"),
        };
        self.wait = next;
        let mut observed = Vec::new();
        for effect in effects {
            match effect {
                RequestEffect::RunRefused(matched) => {
                    observed.push(Observation::Refusal {
                        key: matched.call().clone(),
                        reason: matched.reason(),
                    });
                    observed.extend(
                        self.gate(GateEvent::HolderAuthorityRefused(matched))
                            .unwrap_or_default(),
                    );
                }
                other => observed.push(Observation::Wait(other)),
            }
        }
        Some(observed)
    }

    fn local_event(input: &WaitInput) -> Option<RequestEvent> {
        Some(match input {
            WaitInput::Publish(_) | WaitInput::Ending { .. } => return None,
            WaitInput::Raise { key, reply } => RequestEvent::AttemptSettled {
                call: key.clone(),
                disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(reply.clone())),
            },
            WaitInput::Stop { key, reply } => RequestEvent::AttemptSettled {
                call: key.clone(),
                disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(reply.clone())),
            },
            WaitInput::AnswerLost(key) => RequestEvent::AttemptSettled {
                call: key.clone(),
                disposition: LinkAttemptDisposition::AnswerLost,
            },
            WaitInput::NotReached(key) => RequestEvent::AttemptSettled {
                call: key.clone(),
                disposition: LinkAttemptDisposition::NotReached,
            },
            WaitInput::LocalStop(cause) => RequestEvent::LocalStop { cause: *cause },
            WaitInput::Permit { responder, reply } => RequestEvent::TakeFinalPermit {
                responder: *responder,
                reply: reply.clone(),
            },
            WaitInput::Start { responder, reply } => RequestEvent::StartWrite {
                responder: *responder,
                reply: reply.clone(),
            },
            WaitInput::ProviderWithdrew(responder) => RequestEvent::ProviderWithdrew {
                responder: *responder,
            },
            WaitInput::Withdraw => RequestEvent::CapabilityWithdrawn {
                responder: RESPONDER,
            },
            WaitInput::Finish(outcome) => RequestEvent::WriteFinished {
                responder: RESPONDER,
                outcome: *outcome,
            },
        })
    }
}
