#![expect(
    clippy::panic,
    clippy::unwrap_in_result,
    reason = "The generated test fails loudly on unexpected machine refusals or missing executor effects."
)]

use approval_request_protocol::{
    CancelReason, DeliverEndingReply, DeliverEndingRequest, Ending, InvalidApprovalEnding,
    ProviderStopCause, RaiseReceipt, RaiseReply, RegisterRunReply, RegisterRunRequest, RunRefusal,
    StopReceiptEnding, StopReply, StopRequest,
};
use hegel::generators as gs;
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::{
        AuthorityEnd, CallAttempt, CallKey, DialAttempt, LinkAttempt, RunGate, RunGateState,
    },
    waiting_request::{LocalNegative, ProviderReply, WaitState, WriterOutcome},
};

use super::fixtures::{
    GateEffect, GateEvent, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority,
    raise, try_offer, try_publish,
};

const STOP: ProviderStopCause = ProviderStopCause::TurnStopped {};
const LOST: ProviderStopCause = ProviderStopCause::ProviderConnectionLost {};

type Key = CallKey<u8>;
type Link = LinkAttempt<u8>;

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Gate(GateEffect),
    Wait(RequestEffect),
    Refusal { key: Key, reason: RunRefusal },
}

#[derive(Default)]
struct HistoryFacts {
    dial: Option<Link>,
    welcomed: bool,
    registration: Option<Key>,
    ready: bool,
    ended: bool,
    exposed: bool,
    saved: bool,
    raise: Option<Key>,
    stop: Option<Key>,
    owed: Option<StopRequest>,
    recorded_ending: Option<DeliverEndingRequest>,
    central_done: bool,
    refusing_locally: bool,
    selected: Option<ProviderReply>,
    permitted: bool,
    writer_started: bool,
    handle_released: bool,
}

impl HistoryFacts {
    fn closed(&self) -> bool {
        (self.central_done || self.refusing_locally) && self.handle_released && !self.writer_started
    }

    fn claim(&mut self, reply: ProviderReply) {
        self.selected = Some(reply);
        self.permitted = false;
    }

    fn release(&mut self, effects: &mut Vec<Observation>) {
        if !self.handle_released {
            effects.push(Observation::Wait(RequestEffect::ReleaseBinding {
                responder: RESPONDER,
            }));
            self.handle_released = true;
            self.selected = None;
            self.permitted = false;
        }
    }

    fn retire(&mut self, link: &Link) {
        self.dial = None;
        self.welcomed = false;
        self.registration = None;
        self.ready = false;
        if self.raise.as_ref().is_some_and(|key| &key.link == link) {
            self.raise = None;
        }
        if self.stop.as_ref().is_some_and(|key| &key.link == link) {
            self.stop = None;
        }
    }

    fn fence(&mut self, reason: AuthorityEnd, effects: &mut Vec<Observation>) {
        self.ended = true;
        self.ready = false;
        self.raise = None;
        self.stop = None;
        self.owed = None;
        self.central_done = true;
        self.recorded_ending = None;
        effects.push(Observation::Gate(GateEffect::FenceWaits {
            authority: authority(),
            reason,
        }));
        self.release(effects);
    }

    fn authority_refused(&mut self, key: Key, reason: RunRefusal, effects: &mut Vec<Observation>) {
        effects.push(Observation::Refusal {
            key: key.clone(),
            reason,
        });
        match reason {
            RunRefusal::RunNotRegistered {} => {
                self.retire(&key.link);
                effects.push(Observation::Gate(GateEffect::RetireConnection {
                    link: key.link,
                }));
            }
            RunRefusal::RunRetired {} => self.fence(AuthorityEnd::RunRetired, effects),
            RunRefusal::EnrollmentEnded {} => self.fence(AuthorityEnd::EnrollmentEnded, effects),
        }
    }

    fn gate(&mut self, event: &GateEvent) -> Option<Vec<Observation>> {
        if self.ended {
            return None;
        }
        let mut effects = Vec::new();
        match event {
            GateEvent::DialStarted { link } if self.dial.is_none() => {
                self.dial = Some(link.clone())
            }
            GateEvent::Welcome {
                authority: presented,
                link,
            } if presented == &authority()
                && self.dial.as_ref() == Some(link)
                && !self.welcomed =>
            {
                self.welcomed = true;
            }
            GateEvent::PublishRegistration { call }
                if self.welcomed
                    && self.registration.is_none()
                    && !self.ready
                    && self.dial.as_ref() == Some(&call.link) =>
            {
                self.registration = Some(call.clone());
                effects.push(Observation::Gate(GateEffect::Register {
                    call: call.clone(),
                    request: RegisterRunRequest {
                        router_run: authority().router_run,
                    },
                }));
            }
            GateEvent::RegistrationFailed { call } if self.registration.as_ref() == Some(call) => {
                self.registration = None;
            }
            GateEvent::RegistrationReply { call, reply }
                if self.registration.as_ref() == Some(call) =>
            {
                self.registration = None;
                match reply {
                    RegisterRunReply::Registered {} => self.ready = true,
                    RegisterRunReply::RunNotRegistered {} => {
                        self.retire(&call.link);
                        effects.push(Observation::Gate(GateEffect::RetireConnection {
                            link: call.link.clone(),
                        }));
                    }
                    RegisterRunReply::RunRetired {} => {
                        self.fence(AuthorityEnd::RunRetired, &mut effects)
                    }
                    RegisterRunReply::EnrollmentEnded {} => {
                        self.fence(AuthorityEnd::EnrollmentEnded, &mut effects)
                    }
                }
            }
            GateEvent::ConnectionLost { link } if self.dial.as_ref() == Some(link) => {
                self.retire(link)
            }
            GateEvent::AuthenticatedEnrollmentEnded { enrollment: 1 } => {
                self.fence(AuthorityEnd::EnrollmentEnded, &mut effects)
            }
            _ => return None,
        }
        Some(effects)
    }

    fn publish(&mut self, attempt: CallAttempt, effects: &mut Vec<Observation>) -> Option<()> {
        if !self.ready {
            return None;
        }
        let key = Key {
            link: self.dial.clone()?,
            attempt,
        };
        if let Some(request) = &self.owed {
            if self.stop.is_some() {
                return None;
            }
            self.stop = Some(key.clone());
            effects.push(Observation::Wait(RequestEffect::Stop {
                call: key,
                authority: authority(),
                request: request.clone(),
            }));
        } else {
            if self.central_done || self.refusing_locally || self.raise.is_some() || self.saved {
                return None;
            }
            self.exposed = true;
            self.raise = Some(key.clone());
            effects.push(Observation::Wait(RequestEffect::Raise {
                call: key,
                authority: authority(),
                request: raise(),
            }));
        }
        Some(())
    }

    fn arrive(
        &mut self,
        link: &Link,
        request: &DeliverEndingRequest,
        effects: &mut Vec<Observation>,
    ) {
        if self.ended || self.dial.as_ref() != Some(link) {
            return;
        }
        let reply = if !self.ready {
            DeliverEndingReply::HolderNotReady {}
        } else if self.closed() {
            DeliverEndingReply::NotHeld {}
        } else if let Some(claimed) = &self.recorded_ending {
            if claimed == request {
                DeliverEndingReply::Received {}
            } else {
                effects.push(Observation::Wait(RequestEffect::EndingConflict {
                    request_id: request.request_id.clone(),
                    reason: InvalidApprovalEnding::BindingMismatch,
                }));
                DeliverEndingReply::EndingDiffers {}
            }
        } else if self.owed.is_some() {
            DeliverEndingReply::StopOwed {}
        } else if self.central_done {
            DeliverEndingReply::NotHeld {}
        } else if !self.exposed || self.refusing_locally {
            DeliverEndingReply::HolderNotReady {}
        } else {
            self.recorded_ending = Some(request.clone());
            self.central_done = true;
            self.claim(ProviderReply::CentralEnding(request.ending.clone()));
            DeliverEndingReply::Received {}
        };
        effects.push(Observation::Wait(RequestEffect::EndingReply {
            authority: authority(),
            link: link.clone(),
            reply,
        }));
    }

    fn stop_locally(
        &mut self,
        cause: ProviderStopCause,
        effects: &mut Vec<Observation>,
    ) -> Option<()> {
        if cause == LOST {
            if self.owed.is_none() && !self.central_done && !self.refusing_locally {
                self.record_stop(cause);
            }
            if !self.writer_started {
                self.release(effects);
            }
            return Some(());
        }
        if self.owed.is_some() || self.refusing_locally {
            return None;
        }
        if self.central_done {
            self.recorded_ending.as_ref()?;
            if self.writer_started {
                return Some(());
            }
            if !matches!(self.selected, Some(ProviderReply::CentralEnding(_))) {
                return None;
            }
        } else {
            self.record_stop(cause);
        }
        self.claim(ProviderReply::LocalNegative(LocalNegative::Stopped(cause)));
        Some(())
    }

    fn record_stop(&mut self, cause: ProviderStopCause) {
        if self.exposed {
            let request = raise();
            self.owed = Some(StopRequest::Provider {
                request_id: request.request_id().clone(),
                router_run: request.router_run().clone(),
                contents_digest: request.contents_digest().unwrap(),
                cause,
            });
        } else {
            self.refusing_locally = true;
        }
    }

    fn wait(&mut self, action: &WaitInput) -> Option<Vec<Observation>> {
        let mut effects = Vec::new();
        match action {
            WaitInput::Ending { link, request } => {
                if self.closed() && (self.raise.is_some() || self.stop.is_some()) {
                    return None;
                }
                self.arrive(link, request, &mut effects);
            }
            WaitInput::Raise { key, reply } if self.raise.as_ref() == Some(key) => {
                self.raise = None;
                match reply {
                    RaiseReply::AuthorityRefused { refusal } => {
                        self.authority_refused(key.clone(), *refusal, &mut effects)
                    }
                    RaiseReply::Receipt { .. } if self.owed.is_none() && !self.central_done => {
                        self.saved = true
                    }
                    _ => {}
                }
            }
            WaitInput::Stop { key, reply } if self.stop.as_ref() == Some(key) => {
                self.stop = None;
                match reply {
                    StopReply::Stands { ending } => {
                        self.central_done = true;
                        self.owed = None;
                        effects.push(Observation::Wait(RequestEffect::StopReconciled(*ending)));
                    }
                    StopReply::NotVisible {} => {
                        self.central_done = true;
                        self.owed = None;
                        effects.push(Observation::Wait(RequestEffect::StopNotVisible {
                            request_id: raise().request_id().clone(),
                            contents_digest: raise().contents_digest().unwrap(),
                        }));
                    }
                    StopReply::AuthorityRefused { refusal } => {
                        self.authority_refused(key.clone(), *refusal, &mut effects)
                    }
                }
            }
            WaitInput::Lost(key)
                if self.raise.as_ref() == Some(key) || self.stop.as_ref() == Some(key) =>
            {
                if self.raise.as_ref() == Some(key) {
                    self.raise = None;
                }
                if self.stop.as_ref() == Some(key) {
                    self.stop = None;
                }
            }
            WaitInput::Finish(outcome) if self.writer_started => {
                self.writer_started = false;
                self.release(&mut effects);
                effects.push(Observation::Wait(RequestEffect::WriterResolved {
                    responder: RESPONDER,
                    outcome: *outcome,
                }));
            }
            _ if self.closed() || self.ended => return None,
            WaitInput::Publish(attempt) => self.publish(*attempt, &mut effects)?,
            WaitInput::LocalStop(cause) => self.stop_locally(*cause, &mut effects)?,
            WaitInput::Permit { responder, reply }
                if *responder == RESPONDER
                    && !self.permitted
                    && self.selected.as_ref() == Some(reply) =>
            {
                self.permitted = true;
            }
            WaitInput::Start { responder, reply }
                if *responder == RESPONDER
                    && self.permitted
                    && self.selected.as_ref() == Some(reply) =>
            {
                self.writer_started = true;
                self.selected = None;
                self.permitted = false;
                effects.push(Observation::Wait(RequestEffect::Submit {
                    responder: *responder,
                    reply: reply.clone(),
                }));
            }
            WaitInput::ProviderWithdrew(responder)
                if *responder == RESPONDER && !self.handle_released =>
            {
                if !self.writer_started {
                    if self.owed.is_none() && !self.central_done && !self.refusing_locally {
                        self.record_stop(ProviderStopCause::AskWithdrawn);
                    }
                    self.release(&mut effects);
                }
            }
            WaitInput::Withdraw if self.selected.is_some() => self.release(&mut effects),
            _ => return None,
        }
        if self.closed() {
            self.recorded_ending = None;
        }
        Some(effects)
    }
}

#[derive(Debug)]
enum WaitInput {
    Publish(CallAttempt),
    Raise {
        key: Key,
        reply: RaiseReply,
    },
    Stop {
        key: Key,
        reply: StopReply,
    },
    Lost(Key),
    Ending {
        link: Link,
        request: DeliverEndingRequest,
    },
    LocalStop(ProviderStopCause),
    Permit {
        responder: u8,
        reply: ProviderReply,
    },
    Start {
        responder: u8,
        reply: ProviderReply,
    },
    ProviderWithdrew(u8),
    Withdraw,
    Finish(WriterOutcome),
}

struct Product {
    gate: RunGateState<u8, u8>,
    wait: WaitState<u8, u8>,
}

impl Product {
    fn new() -> Self {
        Self {
            gate: RunGateState::new(authority()),
            wait: WaitState::bound(raise(), RESPONDER).unwrap(),
        }
    }

    fn gate(&mut self, event: GateEvent) -> Option<Vec<Observation>> {
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

    fn wait(&mut self, input: &WaitInput) -> Option<Vec<Observation>> {
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
                            .unwrap(),
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
            WaitInput::Raise { key, reply } => RequestEvent::RaiseReply {
                call: key.clone(),
                reply: reply.clone(),
            },
            WaitInput::Stop { key, reply } => RequestEvent::StopReply {
                call: key.clone(),
                reply: reply.clone(),
            },
            WaitInput::Lost(key) => RequestEvent::CallEndedWithoutReply { call: key.clone() },
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

fn ending(alternate: bool) -> DeliverEndingRequest {
    let value = if alternate {
        Ending::Cancelled {
            reason: CancelReason::RequesterGone {},
        }
    } else {
        Ending::TimedOut {}
    };
    DeliverEndingRequest::from_raise(&raise(), value).unwrap()
}

fn assert_observations(actual: &Product, model: &HistoryFacts) {
    let ready = model.dial.as_ref().filter(|_| model.ready);
    assert_eq!(actual.gate.ready_link(), ready);
    assert_eq!(actual.wait.holds_place(), !model.closed());
    assert_eq!(
        actual.wait.may_be_forgotten(),
        model.closed() && model.raise.is_none() && model.stop.is_none()
    );
    assert_eq!(
        actual.wait.has_pending_calls(),
        model.raise.is_some() || model.stop.is_some()
    );
    assert_eq!(
        actual.wait.claimed_reply(),
        model.selected.as_ref().map(|reply| (&RESPONDER, reply))
    );
}

#[hegel::test(test_cases = 300)]
fn generated_gate_and_wait_histories_match_the_independent_facts(case: hegel::TestCase) {
    let mut actual = Product::new();
    let mut model = HistoryFacts::default();
    let initial = Link {
        connection: 1,
        dial: DialAttempt::new(1),
    };
    let registration = Key {
        link: initial.clone(),
        attempt: CallAttempt::new(1),
    };
    for event in [
        GateEvent::DialStarted {
            link: initial.clone(),
        },
        GateEvent::Welcome {
            authority: authority(),
            link: initial,
        },
        GateEvent::PublishRegistration {
            call: registration.clone(),
        },
        GateEvent::RegistrationReply {
            call: registration,
            reply: RegisterRunReply::Registered {},
        },
    ] {
        assert_eq!(actual.gate(event.clone()), model.gate(&event), "{event:?}");
        assert_observations(&actual, &model);
    }
    let first = WaitInput::Publish(CallAttempt::new(2));
    assert_eq!(
        actual.wait(&first),
        model.wait(&first),
        "initial publication"
    );
    let steps = case.draw(gs::integers::<u8>().min_value(6).max_value(48));
    for step in 0..steps {
        let event = case.draw(gs::integers::<u8>().max_value(22));
        let exact = case.draw(gs::booleans());
        let alternate = case.draw(gs::booleans());
        let serial = u64::from(step) + 3;
        let stale_link = Link {
            connection: 1,
            dial: DialAttempt::new(0),
        };
        let link = model.dial.clone().filter(|_| exact).unwrap_or(stale_link);
        let missed = Key {
            link: link.clone(),
            attempt: CallAttempt::new(0),
        };
        let new_call = Key {
            link: link.clone(),
            attempt: CallAttempt::new(serial),
        };
        let key = |current: &Option<Key>| {
            current
                .clone()
                .filter(|_| exact)
                .unwrap_or_else(|| missed.clone())
        };
        let gate_event = match event {
            0 => Some(GateEvent::DialStarted {
                link: Link {
                    connection: 1,
                    dial: DialAttempt::new(serial),
                },
            }),
            1 => Some(GateEvent::Welcome {
                authority: authority(),
                link: link.clone(),
            }),
            2 => Some(GateEvent::PublishRegistration {
                call: new_call.clone(),
            }),
            3 => Some(GateEvent::RegistrationReply {
                call: key(&model.registration),
                reply: if alternate {
                    RegisterRunReply::RunNotRegistered {}
                } else {
                    RegisterRunReply::Registered {}
                },
            }),
            4 => Some(GateEvent::RegistrationFailed {
                call: key(&model.registration),
            }),
            5 => Some(GateEvent::ConnectionLost { link: link.clone() }),
            6 if alternate => Some(GateEvent::AuthenticatedEnrollmentEnded {
                enrollment: if exact { 1 } else { 2 },
            }),
            _ => None,
        };
        if let Some(event) = gate_event {
            assert_eq!(
                actual.gate(event.clone()),
                model.gate(&event),
                "history step {step}: {event:?}"
            );
        } else {
            let reply = model
                .selected
                .clone()
                .filter(|_| exact)
                .unwrap_or_else(|| ProviderReply::CentralEnding(ending(alternate).ending));
            let responder = if alternate { RESPONDER } else { 2 };
            let input = match event {
                7 => WaitInput::Publish(CallAttempt::new(serial)),
                8 => WaitInput::Raise {
                    key: key(&model.raise),
                    reply: RaiseReply::Receipt {
                        receipt: RaiseReceipt {
                            request_id: raise().request_id().clone(),
                            window: "2026-11-22T17:00:00Z"
                                .parse()
                                .map(approval_request_protocol::ApprovalWindow::from_first_save)
                                .unwrap()
                                .unwrap(),
                        },
                    },
                },
                9 => WaitInput::Raise {
                    key: key(&model.raise),
                    reply: RaiseReply::AuthorityRefused {
                        refusal: if alternate {
                            RunRefusal::RunRetired {}
                        } else {
                            RunRefusal::RunNotRegistered {}
                        },
                    },
                },
                10 => WaitInput::Stop {
                    key: key(&model.stop),
                    reply: if alternate {
                        StopReply::NotVisible {}
                    } else {
                        StopReply::Stands {
                            ending: StopReceiptEnding::TimedOut {},
                        }
                    },
                },
                11 => WaitInput::Stop {
                    key: key(&model.stop),
                    reply: StopReply::AuthorityRefused {
                        refusal: if alternate {
                            RunRefusal::EnrollmentEnded {}
                        } else {
                            RunRefusal::RunNotRegistered {}
                        },
                    },
                },
                12 => WaitInput::Lost(key(if alternate { &model.raise } else { &model.stop })),
                13 | 14 => WaitInput::Ending {
                    link,
                    request: ending(alternate),
                },
                15 => WaitInput::LocalStop(if alternate { LOST } else { STOP }),
                16 => WaitInput::Permit { responder, reply },
                17 => WaitInput::Start { responder, reply },
                18 => WaitInput::Withdraw,
                19 => WaitInput::Finish(if alternate {
                    WriterOutcome::OutcomeUnknown
                } else {
                    WriterOutcome::WriteReturned
                }),
                20 => WaitInput::Permit {
                    responder: RESPONDER,
                    reply,
                },
                21 => WaitInput::Start {
                    responder: RESPONDER,
                    reply,
                },
                22 => WaitInput::ProviderWithdrew(responder),
                _ => WaitInput::Publish(CallAttempt::new(serial)),
            };
            assert_eq!(
                actual.wait(&input),
                model.wait(&input),
                "history step {step}: {input:?}"
            );
        }
        assert_observations(&actual, &model);
    }
}
