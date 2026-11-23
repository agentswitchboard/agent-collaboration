use approval_request_protocol::{
    CallStopCause, CancelReason, DeliverEndingReply, DeliverEndingRequest, Ending, HolderStopCause,
    InvalidApprovalEnding, ProviderStopCause, Raise, RaiseReceipt, RaiseReply, RegisterRunReply,
    RegisterRunRequest, RunRefusal, StopReceiptEnding, StopReply, StopRequest,
};
use hegel::generators as gs;
use switchboard_approval_holder::{
    run_gate::{AuthorityEnd, CallAttempt, CallKey, DialAttempt, LinkAttempt},
    waiting_request::{LocalNegative, ProviderReply, RaiseRefusal, WriterOutcome},
};

use super::fixtures::{
    GateEffect, GateEvent, RESPONDER, RequestEffect, authority, call_raise, raise,
};

mod capacity_history;
mod product_adapter;

use product_adapter::Product;

const STOP: HolderStopCause = HolderStopCause::TurnStopped;
const LOST: HolderStopCause = HolderStopCause::ProviderConnectionLost;
const CALL_WITHDRAWN: HolderStopCause = HolderStopCause::CallWithdrawn;

fn provider_cause(cause: HolderStopCause) -> Option<ProviderStopCause> {
    match cause {
        HolderStopCause::TurnStopped => Some(ProviderStopCause::TurnStopped),
        HolderStopCause::AskReplaced => Some(ProviderStopCause::AskReplaced),
        HolderStopCause::AskWithdrawn => Some(ProviderStopCause::AskWithdrawn),
        HolderStopCause::SessionClosed => Some(ProviderStopCause::SessionClosed),
        HolderStopCause::ProviderConnectionLost => Some(ProviderStopCause::ProviderConnectionLost),
        HolderStopCause::HolderRefusedLocally => Some(ProviderStopCause::HolderRefusedLocally),
        HolderStopCause::CallWithdrawn => None,
    }
}

type Key = CallKey<u8>;
type Link = LinkAttempt<u8>;

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Gate(GateEffect),
    Wait(RequestEffect),
    Refusal { key: Key, reason: RunRefusal },
    StopCauseRefused,
}

#[derive(Default)]
struct HistoryFacts {
    call_wait: bool,
    dial: Option<Link>,
    welcomed: bool,
    registration: Option<Key>,
    ready: bool,
    ended: bool,
    published: bool,
    could_be_admitted: bool,
    saved: bool,
    raise: Option<Key>,
    stop: Option<Key>,
    owed: Option<StopRequest>,
    withdrawal_waiting_for_link: Option<HolderStopCause>,
    recorded_ending: Option<DeliverEndingRequest>,
    central_done: bool,
    refusing_locally: bool,
    selected: Option<ProviderReply>,
    permitted: bool,
    writer_started: bool,
    handle_released: bool,
}

impl HistoryFacts {
    fn request(&self) -> Raise {
        if self.call_wait {
            call_raise()
        } else {
            raise()
        }
    }

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
        if (self.central_done || self.owed.is_some())
            && self.raise.as_ref().is_some_and(|key| &key.link == link)
        {
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
        self.withdrawal_waiting_for_link = None;
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
        if !self.ready || self.dial.as_ref() != Some(&key.link) {
            return;
        }
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
            self.published = true;
            self.raise = Some(key.clone());
            effects.push(Observation::Wait(RequestEffect::Raise {
                call: key,
                authority: authority(),
                request: self.request(),
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
                    request_id: request.request_id().clone(),
                    reason: InvalidApprovalEnding::BindingMismatch,
                }));
                DeliverEndingReply::EndingDiffers {}
            }
        } else if let Some(cause) = self.withdrawal_waiting_for_link.take() {
            self.could_be_admitted = true;
            self.record_stop(cause);
            DeliverEndingReply::StopOwed {}
        } else if self.owed.is_some() {
            DeliverEndingReply::StopOwed {}
        } else if self.central_done {
            DeliverEndingReply::NotHeld {}
        } else if !self.published || self.refusing_locally {
            DeliverEndingReply::HolderNotReady {}
        } else {
            self.recorded_ending = Some(request.clone());
            self.central_done = true;
            self.claim(ProviderReply::CentralEnding(request.ending().clone()));
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
        cause: HolderStopCause,
        effects: &mut Vec<Observation>,
    ) -> Option<()> {
        if cause == LOST || cause == CALL_WITHDRAWN {
            if self.owed.is_none()
                && self.withdrawal_waiting_for_link.is_none()
                && !self.central_done
                && !self.refusing_locally
            {
                self.record_stop(cause);
            }
            if !self.writer_started {
                self.release(effects);
            }
            return Some(());
        }
        if self.owed.is_some()
            || self.refusing_locally
            || self.withdrawal_waiting_for_link.is_some()
        {
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
        self.claim(ProviderReply::LocalNegative(LocalNegative::Stopped(
            provider_cause(cause)?,
        )));
        Some(())
    }

    fn record_stop(&mut self, cause: HolderStopCause) {
        if self.saved || self.could_be_admitted {
            self.refusing_locally = false;
            let request = self.request();
            let request_id = request.request_id().clone();
            let router_run = request.router_run().clone();
            let contents_digest = request.contents_digest().unwrap();
            self.owed = Some(if self.call_wait {
                assert!(cause == CALL_WITHDRAWN || cause == HolderStopCause::HolderRefusedLocally);
                StopRequest::UnprovenCall {
                    request_id,
                    router_run,
                    contents_digest,
                    cause: if cause == CALL_WITHDRAWN {
                        CallStopCause::CallWithdrawn
                    } else {
                        CallStopCause::HolderRefusedLocally
                    },
                }
            } else {
                StopRequest::Provider {
                    request_id,
                    router_run,
                    contents_digest,
                    cause: provider_cause(cause).unwrap(),
                }
            });
        } else if self.raise.is_some() {
            self.withdrawal_waiting_for_link = Some(cause);
        } else {
            self.refusing_locally = true;
        }
    }

    fn settle_withdrawal(&mut self) {
        if let Some(cause) = self.withdrawal_waiting_for_link.take() {
            self.record_stop(cause);
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
                        self.settle_withdrawal();
                        self.authority_refused(key.clone(), *refusal, &mut effects)
                    }
                    RaiseReply::Receipt { .. } if self.owed.is_none() && !self.central_done => {
                        self.saved = true;
                        self.settle_withdrawal();
                    }
                    RaiseReply::AlreadyEnded { marker }
                        if self.owed.is_none() && !self.central_done =>
                    {
                        self.refusing_locally = true;
                        if self.withdrawal_waiting_for_link.take().is_none() {
                            self.claim(ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
                                RaiseRefusal::AlreadyEnded(marker.ending),
                            )));
                        }
                    }
                    RaiseReply::TooManyOutstanding {}
                        if self.owed.is_none() && !self.central_done =>
                    {
                        if self.withdrawal_waiting_for_link.is_some() {
                            self.settle_withdrawal();
                        } else {
                            self.record_stop(HolderStopCause::HolderRefusedLocally);
                            self.claim(ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
                                RaiseRefusal::TooManyOutstanding,
                            )));
                        }
                    }
                    RaiseReply::Receipt { .. }
                    | RaiseReply::AlreadyStopped {}
                    | RaiseReply::AlreadyEnded { .. }
                    | RaiseReply::SessionEnded {}
                    | RaiseReply::ContentsDiffer {}
                    | RaiseReply::TooManyOutstanding {} => {}
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
                            request_id: self.request().request_id().clone(),
                            contents_digest: self.request().contents_digest().unwrap(),
                        }));
                    }
                    StopReply::AuthorityRefused { refusal } => {
                        self.authority_refused(key.clone(), *refusal, &mut effects)
                    }
                }
            }
            WaitInput::AnswerLost(key) | WaitInput::NotReached(key)
                if self.raise.as_ref() == Some(key) || self.stop.as_ref() == Some(key) =>
            {
                if self.raise.as_ref() == Some(key) {
                    self.raise = None;
                    if matches!(action, WaitInput::AnswerLost(_)) {
                        self.could_be_admitted = true;
                    }
                    self.settle_withdrawal();
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
            WaitInput::LocalStop(cause) if self.call_wait != (*cause == CALL_WITHDRAWN) => {
                effects.push(Observation::StopCauseRefused);
            }
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
            WaitInput::ProviderWithdrew(_) if self.call_wait => {
                effects.push(Observation::StopCauseRefused);
            }
            WaitInput::ProviderWithdrew(responder)
                if *responder == RESPONDER && !self.handle_released =>
            {
                if !self.writer_started {
                    if self.owed.is_none()
                        && self.withdrawal_waiting_for_link.is_none()
                        && !self.central_done
                        && !self.refusing_locally
                    {
                        self.record_stop(HolderStopCause::AskWithdrawn);
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
    AnswerLost(Key),
    NotReached(Key),
    Ending {
        link: Link,
        request: DeliverEndingRequest,
    },
    LocalStop(HolderStopCause),
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

fn ending(request: &Raise, alternate: bool) -> DeliverEndingRequest {
    let value = if alternate {
        Ending::Cancelled {
            reason: CancelReason::RequesterGone {},
        }
    } else {
        Ending::TimedOut {}
    };
    DeliverEndingRequest::from_raise(request, value).unwrap()
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
    exercise_history(case, false);
}

#[hegel::test(test_cases = 300)]
fn generated_gate_and_call_wait_histories_match_the_independent_facts(case: hegel::TestCase) {
    exercise_history(case, true);
}

fn exercise_history(case: hegel::TestCase, call_wait: bool) {
    capacity_history::check(call_wait, false);
    capacity_history::check(call_wait, true);
    for earlier_answer_lost in [false, true] {
        check_exposure_history(call_wait, earlier_answer_lost);
    }
    let (mut actual, mut model) = registered_history(call_wait);
    let request = model.request();
    let first = WaitInput::Publish(CallAttempt::new(2));
    assert_eq!(
        actual.wait(&first),
        model.wait(&first),
        "initial publication"
    );
    exercise_generated_steps(case, &mut actual, &mut model, &request, call_wait);
}

fn registered_history(call_wait: bool) -> (Product, HistoryFacts) {
    let mut model = HistoryFacts {
        call_wait,
        ..HistoryFacts::default()
    };
    let mut actual = Product::new(model.request());
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
    (actual, model)
}

fn check_exposure_history(call_wait: bool, earlier_answer_lost: bool) {
    let (mut actual, mut model) = registered_history(call_wait);
    let mut inputs = vec![WaitInput::Publish(CallAttempt::new(2))];
    if earlier_answer_lost {
        inputs.push(WaitInput::AnswerLost(super::fixtures::call(1, 2)));
        inputs.push(WaitInput::Publish(CallAttempt::new(3)));
    }
    inputs.push(WaitInput::NotReached(super::fixtures::call(
        1,
        if earlier_answer_lost { 3 } else { 2 },
    )));
    inputs.push(if call_wait {
        WaitInput::LocalStop(CALL_WITHDRAWN)
    } else {
        WaitInput::ProviderWithdrew(RESPONDER)
    });
    for input in inputs {
        assert_eq!(actual.wait(&input), model.wait(&input), "{input:?}");
        assert_observations(&actual, &model);
    }
    let publish = WaitInput::Publish(CallAttempt::new(4));
    let expected = model.wait(&publish);
    assert_eq!(expected.is_some(), earlier_answer_lost);
    assert_eq!(
        actual.wait(&publish),
        expected,
        "stop eligibility after an unseen Raise"
    );
    assert_observations(&actual, &model);
}

fn exercise_generated_steps(
    case: hegel::TestCase,
    actual: &mut Product,
    model: &mut HistoryFacts,
    request: &Raise,
    call_wait: bool,
) {
    let steps = case.draw(gs::integers::<u8>().min_value(6).max_value(48));
    for step in 0..steps {
        let event = case.draw(gs::integers::<u8>().max_value(24));
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
            let reply = model.selected.clone().filter(|_| exact).unwrap_or_else(|| {
                ProviderReply::CentralEnding(ending(request, alternate).ending().clone())
            });
            let responder = if alternate { RESPONDER } else { 2 };
            let input = match event {
                7 => WaitInput::Publish(CallAttempt::new(serial)),
                8 => WaitInput::Raise {
                    key: key(&model.raise),
                    reply: RaiseReply::Receipt {
                        receipt: RaiseReceipt {
                            request_id: request.request_id().clone(),
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
                12 => {
                    WaitInput::AnswerLost(key(if alternate { &model.raise } else { &model.stop }))
                }
                13 | 14 => WaitInput::Ending {
                    link,
                    request: ending(request, alternate),
                },
                15 => WaitInput::LocalStop(if call_wait {
                    if alternate { CALL_WITHDRAWN } else { LOST }
                } else if alternate {
                    LOST
                } else {
                    STOP
                }),
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
                23 => {
                    WaitInput::NotReached(key(if alternate { &model.raise } else { &model.stop }))
                }
                24 => WaitInput::Raise {
                    key: key(&model.raise),
                    reply: RaiseReply::TooManyOutstanding {},
                },
                _ => WaitInput::Publish(CallAttempt::new(serial)),
            };
            assert_eq!(
                actual.wait(&input),
                model.wait(&input),
                "history step {step}: {input:?}"
            );
        }
        assert_observations(actual, model);
    }
}
