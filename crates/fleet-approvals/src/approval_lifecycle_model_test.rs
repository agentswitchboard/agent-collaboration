//! Reference ledger transcribed from the smaller core table before comparing the reducer.
use std::collections::BTreeMap;

use approval_request_protocol::{
    CancelReason, DecidedBy, DeliverEndingReply, DeliverEndingRequest, Delivery, Ending,
    HolderStopCause, PassOnReply, PassOnRequest, PrivateEnding, RaiseReply, StopReply,
    WaitingStatus,
};
use hegel::generators as gs;

use crate::{
    AdmittedDecider, ApprovalCollection, ApprovalCollectionEffect, ApprovalCollectionEvent,
    ApprovalDecisionReply, ApprovalEffect, ApprovalReceipt, ApprovalRequestKey, ApprovalState,
    FactorStanding,
};

use super::fixtures::{
    HOUR, answer, decide_event, key, person, person_id, provider, provider_stop, raise_event,
    reply, routing, run_event, setup, time, view_event,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Decider {
    Parent,
    Owner,
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Raise(bool),
    Decide {
        operation: u16,
        choice: u8,
        decider: Decider,
    },
    Stop(bool),
    Forward,
    Read,
    Alarm(bool),
    Deliver {
        reply: DeliverEndingReply,
        wrong_ending: bool,
    },
    CloseSession,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelEnding {
    Decided { choice: u8, decider: Decider },
    TimedOut,
    Stopped,
    SessionEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelDelivery {
    Unpaid,
    Received,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResultCode {
    NoReply,
    Receipt,
    AlreadyStopped,
    ContentsDiffer,
    SessionEnded,
    NotVisible,
    Invalid,
    Decision,
    Standing,
    TooLate,
    OperationConflict,
    Forwarded,
    Ended,
    Readable,
    StopReceipt,
}

#[derive(Default)]
struct IndependentLedger {
    floor: i64,
    first_save: Option<(bool, i64)>,
    stopped_before_raise: Option<bool>,
    ending: Option<(ModelEnding, i64)>,
    delivery: Option<ModelDelivery>,
    forwarded: bool,
    session_ended: bool,
    operations: BTreeMap<u16, (Decider, u8, ResultCode)>,
    opens: usize,
    ends: usize,
    offers: usize,
}

impl IndependentLedger {
    fn late(&self) -> bool {
        self.first_save
            .is_some_and(|(_, raised)| self.floor >= raised + HOUR)
    }

    fn waiting(&self) -> bool {
        self.first_save.is_some() && self.ending.is_none()
    }

    fn finish(&mut self, requested: ModelEnding, delivery: ModelDelivery) {
        assert!(self.waiting());
        let ending = if self.late() {
            ModelEnding::TimedOut
        } else {
            requested
        };
        self.ending = Some((ending, self.floor));
        self.delivery = Some(delivery);
        self.ends += 1;
        self.offers += usize::from(delivery == ModelDelivery::Unpaid);
    }

    fn record(&mut self, step: Step, sample: i64) -> ResultCode {
        self.floor = self.floor.max(sample);
        match step {
            Step::Raise(contents) => {
                if let Some(marker) = self.stopped_before_raise {
                    return if marker == contents {
                        ResultCode::AlreadyStopped
                    } else {
                        ResultCode::ContentsDiffer
                    };
                }
                if let Some((saved, _)) = self.first_save {
                    return if saved == contents {
                        ResultCode::Receipt
                    } else {
                        ResultCode::ContentsDiffer
                    };
                }
                if self.session_ended {
                    return ResultCode::SessionEnded;
                }
                self.first_save = Some((contents, self.floor));
                self.opens += 1;
                ResultCode::Receipt
            }
            Step::Stop(contents) => {
                let known = self
                    .stopped_before_raise
                    .or_else(|| self.first_save.map(|(contents, _)| contents));
                if known.is_some_and(|known| known != contents) {
                    return ResultCode::NotVisible;
                }
                if known.is_none() {
                    self.stopped_before_raise = Some(contents);
                } else if self.waiting() {
                    self.finish(ModelEnding::Stopped, ModelDelivery::Closed);
                } else if self.delivery == Some(ModelDelivery::Unpaid) {
                    self.delivery = Some(ModelDelivery::Closed);
                }
                ResultCode::StopReceipt
            }
            Step::Decide {
                operation,
                choice,
                decider,
            } => {
                if self.first_save.is_none() || (decider == Decider::Owner && !self.forwarded) {
                    return ResultCode::NotVisible;
                }
                if let Some((prior_decider, prior_choice, reply)) = self.operations.get(&operation)
                {
                    return if *prior_decider == decider && *prior_choice == choice {
                        *reply
                    } else {
                        ResultCode::OperationConflict
                    };
                }
                let reply = if self.ending.is_some() {
                    ResultCode::Standing
                } else if self.late() {
                    self.finish(ModelEnding::TimedOut, ModelDelivery::Unpaid);
                    ResultCode::TooLate
                } else if choice <= 2 || (choice <= 5 && decider == Decider::Owner) {
                    self.finish(
                        ModelEnding::Decided { choice, decider },
                        ModelDelivery::Unpaid,
                    );
                    ResultCode::Decision
                } else {
                    ResultCode::Invalid
                };
                self.operations.insert(operation, (decider, choice, reply));
                reply
            }
            Step::Forward => {
                if self.first_save.is_none() {
                    return ResultCode::NotVisible;
                }
                if self.waiting() && self.late() {
                    self.finish(ModelEnding::TimedOut, ModelDelivery::Unpaid);
                    return if self.forwarded {
                        ResultCode::Forwarded
                    } else {
                        ResultCode::TooLate
                    };
                }
                if self.forwarded {
                    return ResultCode::Forwarded;
                }
                if self.ending.is_some() {
                    return ResultCode::Ended;
                }
                self.forwarded = true;
                ResultCode::Forwarded
            }
            Step::Read => {
                if self.first_save.is_none() || !self.forwarded {
                    return ResultCode::NotVisible;
                }
                if self.waiting() && self.late() {
                    self.finish(ModelEnding::TimedOut, ModelDelivery::Unpaid);
                }
                ResultCode::Readable
            }
            Step::Alarm(other_token) => {
                if self.waiting() && self.late() && !other_token {
                    self.finish(ModelEnding::TimedOut, ModelDelivery::Unpaid);
                }
                ResultCode::NoReply
            }
            Step::Deliver {
                reply,
                wrong_ending,
            } => {
                if !wrong_ending {
                    match reply {
                        DeliverEndingReply::Received {} => {
                            self.delivery = Some(ModelDelivery::Received)
                        }
                        DeliverEndingReply::NotHeld {}
                            if self.delivery == Some(ModelDelivery::Unpaid) =>
                        {
                            self.delivery = Some(ModelDelivery::Closed);
                        }
                        DeliverEndingReply::NotHeld {}
                        | DeliverEndingReply::StopOwed {}
                        | DeliverEndingReply::EndingDiffers {}
                        | DeliverEndingReply::HolderNotReady {} => {}
                    }
                }
                ResultCode::NoReply
            }
            Step::CloseSession => {
                self.session_ended = true;
                if self.waiting() {
                    self.finish(ModelEnding::SessionEnded, ModelDelivery::Unpaid);
                }
                ResultCode::NoReply
            }
        }
    }

    fn assert_projection(
        &self,
        book: &ApprovalCollection,
        key: &ApprovalRequestKey,
        parent: &crate::ApprovalSession,
    ) {
        assert_eq!(book.high_water, time(self.floor));
        assert_eq!(book.decisions.len(), self.operations.len());
        if let Some((ending, ended_at)) = self.ending {
            let observed = super::fixtures::closed(book, key);
            let expected = match ending {
                ModelEnding::Decided { choice, decider } => Ending::Decided {
                    answer: answer(choice),
                    by: match decider {
                        Decider::Owner => DecidedBy::Person {
                            person_id: person_id(10),
                        },
                        Decider::Parent => DecidedBy::Session {
                            session: parent.session_id.clone(),
                        },
                    },
                },
                ModelEnding::TimedOut => Ending::TimedOut {},
                ModelEnding::Stopped => Ending::Cancelled {
                    reason: CancelReason::Stopped {
                        cause: HolderStopCause::AskReplaced,
                    },
                },
                ModelEnding::SessionEnded => Ending::Cancelled {
                    reason: CancelReason::SessionEnded {},
                },
            };
            assert_eq!(observed.ending.clone().into_ending(), expected);
            assert_eq!(observed.ended_at, time(ended_at));
            assert_eq!(
                observed.delivery,
                match self.delivery.unwrap() {
                    ModelDelivery::Unpaid => Delivery::Owed {},
                    ModelDelivery::Received => Delivery::ReceivedByHolder {},
                    ModelDelivery::Closed => Delivery::ClosedWithoutReceipt {},
                }
            );
        } else if let Some((_, raised_at)) = self.first_save {
            let ApprovalState::Waiting { saved } = &book.requests[key] else {
                panic!("expected a retained wait");
            };
            assert_eq!(saved.window.raised_at(), time(raised_at));
            assert_eq!(saved.window.deadline(), time(raised_at + HOUR));
            assert_eq!(
                saved.waiting_status,
                if self.forwarded {
                    WaitingStatus::PassedOn
                } else {
                    WaitingStatus::Pending
                }
            );
        } else if self.stopped_before_raise.is_some() {
            assert!(matches!(
                book.requests[key],
                ApprovalState::StoppedBeforeRaise { .. }
            ));
        } else {
            assert!(!book.requests.contains_key(key));
        }
    }
}

fn code(effects: &[ApprovalCollectionEffect]) -> ResultCode {
    if !matches!(
        effects.last(),
        Some(ApprovalCollectionEffect::Request(ApprovalEffect::Reply(_)))
    ) {
        return ResultCode::NoReply;
    }
    match reply(effects) {
        ApprovalReceipt::Raise(result) => match result {
            RaiseReply::Receipt { .. } => ResultCode::Receipt,
            RaiseReply::AlreadyStopped {} => ResultCode::AlreadyStopped,
            RaiseReply::ContentsDiffer {} => ResultCode::ContentsDiffer,
            RaiseReply::SessionEnded {} => ResultCode::SessionEnded,
            RaiseReply::TooManyOutstanding {} | RaiseReply::AuthorityRefused { .. } => {
                panic!("expected registered fixture")
            }
        },
        ApprovalReceipt::Decide(result) => match result {
            ApprovalDecisionReply::SavedDecision { .. } => ResultCode::Decision,
            ApprovalDecisionReply::Standing { .. } => ResultCode::Standing,
            ApprovalDecisionReply::OperationConflict {} => ResultCode::OperationConflict,
            ApprovalDecisionReply::NotVisible {} => ResultCode::NotVisible,
            ApprovalDecisionReply::Invalid {} => ResultCode::Invalid,
            ApprovalDecisionReply::TooLate {} => ResultCode::TooLate,
        },
        ApprovalReceipt::Read(view) => {
            if view.is_some() {
                ResultCode::Readable
            } else {
                ResultCode::NotVisible
            }
        }
        ApprovalReceipt::PassOn(result) => match result {
            PassOnReply::Forwarded {} => ResultCode::Forwarded,
            PassOnReply::NotVisible {} => ResultCode::NotVisible,
            PassOnReply::TooLate {} => ResultCode::TooLate,
            PassOnReply::Ended {} => ResultCode::Ended,
        },
        ApprovalReceipt::Stop(result) => match result {
            StopReply::Stands { .. } => ResultCode::StopReceipt,
            StopReply::NotVisible {} => ResultCode::NotVisible,
            StopReply::AuthorityRefused { .. } => panic!("expected registered fixture"),
        },
    }
}

#[hegel::test(test_cases = 400)]
fn generated_histories_match_the_separate_smaller_central_ledger(case: hegel::TestCase) {
    let (mut book, run, requester, parent) = setup();
    let key = key(&run, 1);
    let first = provider(&key, &requester, "First");
    let changed = provider(&key, &requester, "Changed");
    let mut expected = IndependentLedger::default();
    let mut observed_counts = [0, 0, 0];
    let mut last_offer: Option<DeliverEndingRequest> = None;
    let mut compare = |step, sample| {
        let input = match step {
            Step::Raise(contents) => raise_event(
                &run,
                if contents {
                    changed.clone()
                } else {
                    first.clone()
                },
                routing(Some(&parent), false),
            ),
            Step::Stop(contents) => ApprovalCollectionEvent::Stop {
                run: run.clone(),
                request: provider_stop(if contents { &changed } else { &first }),
            },
            Step::Decide {
                operation,
                choice,
                decider,
            } => decide_event(
                &key,
                match decider {
                    Decider::Owner => person(10, FactorStanding::Fresh),
                    Decider::Parent => AdmittedDecider::Session(parent.clone()),
                },
                operation,
                answer(choice),
            ),
            Step::Forward => ApprovalCollectionEvent::PassOn {
                key: key.clone(),
                parent: parent.clone(),
                request: PassOnRequest {
                    request_id: key.request_id.clone(),
                },
            },
            Step::Read => view_event(&key, 10),
            Step::Alarm(other_token) => ApprovalCollectionEvent::Alarm {
                key: key.clone(),
                deadline: time(
                    expected.first_save.map_or(HOUR, |(_, at)| at + HOUR) + i64::from(other_token),
                ),
            },
            Step::Deliver {
                reply,
                wrong_ending,
            } => {
                let Some(mut request) = last_offer.clone() else {
                    return;
                };
                if wrong_ending {
                    let raise = if expected.first_save.unwrap().0 {
                        &changed
                    } else {
                        &first
                    };
                    let wrong = if request.ending.clone().into_ending() == (Ending::TimedOut {}) {
                        Ending::Cancelled {
                            reason: CancelReason::SessionEnded {},
                        }
                    } else {
                        Ending::TimedOut {}
                    };
                    request.ending = PrivateEnding::from_raise(raise, wrong).unwrap();
                }
                ApprovalCollectionEvent::DeliveryReply {
                    run: run.clone(),
                    request: Box::new(request),
                    reply,
                }
            }
            Step::CloseSession => ApprovalCollectionEvent::EndSession {
                session: requester.clone(),
            },
        };
        let anticipated = expected.record(step, sample);
        let effects = run_event(&mut book, input, sample);
        assert_eq!(code(&effects), anticipated, "{step:?} at {sample}");
        for effect in &effects {
            match effect {
                ApprovalCollectionEffect::Request(ApprovalEffect::RegisterDue { .. }) => {
                    observed_counts[0] += 1
                }
                ApprovalCollectionEffect::Request(ApprovalEffect::ClearDue { .. }) => {
                    observed_counts[1] += 1
                }
                ApprovalCollectionEffect::Request(ApprovalEffect::OfferEnding {
                    request, ..
                }) => {
                    observed_counts[2] += 1;
                    last_offer = Some((**request).clone());
                }
                ApprovalCollectionEffect::Request(_) | ApprovalCollectionEffect::Registered(_) => {}
            }
        }
        assert_eq!(
            observed_counts,
            [expected.opens, expected.ends, expected.offers]
        );
        expected.assert_projection(&book, &key, &parent);
    };

    if case.draw(gs::booleans()) {
        compare(Step::Raise(false), 0);
    }
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(36)) {
        let step = match case.draw(gs::integers::<u8>().max_value(8)) {
            0 | 1 => Step::Raise(case.draw(gs::booleans())),
            2 => Step::Decide {
                operation: case.draw(gs::integers::<u16>().min_value(101).max_value(105)),
                choice: case.draw(gs::integers::<u8>().min_value(1).max_value(6)),
                decider: if case.draw(gs::booleans()) {
                    Decider::Owner
                } else {
                    Decider::Parent
                },
            },
            3 => Step::Stop(case.draw(gs::booleans())),
            4 => Step::Forward,
            5 => Step::Read,
            6 => Step::Alarm(case.draw(gs::booleans())),
            7 => Step::Deliver {
                reply: case.draw(gs::sampled_from(vec![
                    DeliverEndingReply::Received {},
                    DeliverEndingReply::NotHeld {},
                    DeliverEndingReply::StopOwed {},
                    DeliverEndingReply::HolderNotReady {},
                    DeliverEndingReply::EndingDiffers {},
                ])),
                wrong_ending: case.draw(gs::booleans()),
            },
            _ => Step::CloseSession,
        };
        let sampled = case.draw(gs::sampled_from(vec![
            0,
            1,
            HOUR - 1,
            HOUR,
            HOUR + 1,
            2 * HOUR,
        ]));
        compare(step, sampled);
    }
}
