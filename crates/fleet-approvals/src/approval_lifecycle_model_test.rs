//! An independent ledger of the first receipt, the destinations, the outcome, and delivery.
//! Only fixture construction is shared with the reducer tests.

#![expect(
    clippy::panic_in_result_fn,
    reason = "an inconsistent oracle must fail the property immediately"
)]

use approval_request_protocol::{
    CancelReason, DecidedBy, Delivery, Ending, EndingView, HolderReport, HolderStopCause,
    InvalidAnswer, OfferEnding, Raise, RaiseReceipt, RaiseReply, Refusal, ReportDeliveryReply,
    StopReceiptEnding, StopReply, WaitingStatus,
};
use central_engine_model::{Machine, StateTag, TransitionError};
use hegel::generators as gs;

use crate::{
    ApprovalDestination, ApprovalEffect, ApprovalLifecycle, ApprovalReceipt, ApprovalRefusal,
    ApprovalState, EndedApproval, RetainedApproval, SessionNoticePhase,
};

use super::fixtures::{
    approval_answer, approval_raise, observer, option_id, parent, person_id, request_id,
    router_run, step, time,
};
use super::{GONE, STOPPED};

#[path = "approval_lifecycle_commands_test.rs"]
mod commands;
#[path = "approval_lifecycle_parked_call_model_test.rs"]
mod parked_call;
use commands::{COMMANDS, Command, command_name, history_prefix};
#[path = "approval_lifecycle_erased_model_test.rs"]
mod erased;
use erased::{erased_effects, standing};

const FIRST_TITLE: &str = "First request";
const CHANGED_TITLE: &str = "Replacement request";
const ONE_HOUR_MILLIS: i64 = 3_600_000;
const SIXTY_DAYS_MILLIS: i64 = 60 * 24 * ONE_HOUR_MILLIS;

type Rejected = TransitionError<ApprovalRefusal>;

#[derive(Default)]
struct ApprovalLedger {
    first_request: Option<FirstRequest>,
    stopped_before_raise: Option<i64>,
    visible_to_people: bool,
    forwarded: bool,
    outcome: Option<(Ending, i64)>,
    delivery: Option<Delivery>,
    erased_ending: Option<StopReceiptEnding>,
}

#[derive(Clone, Copy, Debug)]
struct FirstRequest {
    changed: bool,
    at: i64,
}

impl FirstRequest {
    fn receipt(self) -> RaiseReceipt {
        RaiseReceipt {
            request_id: request_id(),
            raised_at: time(self.at),
            deadline: time(self.at + ONE_HOUR_MILLIS),
        }
    }

    fn contents(self) -> Raise {
        approval_raise(if self.changed {
            CHANGED_TITLE
        } else {
            FIRST_TITLE
        })
    }
}

impl ApprovalLedger {
    fn record(
        &mut self,
        command: Command,
        at: i64,
        include_people: bool,
    ) -> Result<Vec<ApprovalEffect>, Rejected> {
        if let Some(ending) = self.erased_ending {
            return erased_effects(command, ending);
        }
        if matches!(command, Command::Erase)
            && let Some((ending, ended_at)) = &self.outcome
        {
            if at - ended_at < SIXTY_DAYS_MILLIS {
                return Err(Rejected::Stale);
            }
            let effects = if self.delivery == Some(Delivery::AwaitingReport {}) {
                vec![ApprovalEffect::SettleErasedDeliveryAsUnknown {
                    request_id: request_id(),
                }]
            } else {
                vec![]
            };
            *self = Self {
                erased_ending: Some(standing(ending)),
                ..Self::default()
            };
            return Ok(effects);
        }
        let was_raised = self.first_request.is_some();
        let had_ending = self.outcome.is_some();
        let mut offer_again = false;
        let receipt = match command {
            Command::Raise { changed } => {
                let reply = if self.stopped_before_raise.is_some() {
                    RaiseReply::AlreadyStopped {}
                } else {
                    let first = self
                        .first_request
                        .get_or_insert(FirstRequest { changed, at });
                    if first.changed != changed {
                        return Err(domain(ApprovalRefusal::ContentsDiffer));
                    }
                    if !was_raised {
                        self.visible_to_people = include_people;
                    }
                    RaiseReply::RaiseReceipt {
                        receipt: first.receipt(),
                    }
                };
                Some(ApprovalReceipt::Raise(reply))
            }
            Command::HolderStops if !was_raised => {
                self.stopped_before_raise.get_or_insert(at);
                Some(stopped_receipt(StopReceiptEnding::Cancelled {
                    reason: STOPPED,
                }))
            }
            other => {
                let Some(first) = self.first_request else {
                    return Err(Rejected::NotAllowed {
                        state: if self.stopped_before_raise.is_some() {
                            "stoppedBeforeRaise"
                        } else {
                            "unraised"
                        },
                        event: command_name(other),
                    });
                };
                let late = at - first.at >= ONE_HOUR_MILLIS;
                match other {
                    Command::HolderStops => Some(self.stop(at, late)?),
                    Command::Erase => {
                        return Err(Rejected::NotAllowed {
                            state: if self.forwarded {
                                "passedOn"
                            } else {
                                "pending"
                            },
                            event: "erase",
                        });
                    }
                    Command::ForwardFromParent | Command::ForwardWithoutParent => {
                        Some(self.forward(other, at, late)?)
                    }
                    Command::ClockTicks if !had_ending && !late => return Err(Rejected::Stale),
                    Command::ClockTicks if !had_ending => {
                        self.write_outcome(Ending::TimedOut {}, at, Delivery::AwaitingReport {});
                        None
                    }
                    Command::ClockTicks => None,
                    Command::RunEnds | Command::SessionEnds | Command::EnrollmentEnds => {
                        self.authority_ended(other, at, late);
                        None
                    }
                    Command::Offer { foreign } => {
                        self.check_delivery_command("offerAgain", foreign)?;
                        if self.delivery != Some(Delivery::AwaitingReport {}) {
                            return Err(domain(ApprovalRefusal::NoHandoverOwed));
                        }
                        offer_again = true;
                        None
                    }
                    Command::Report {
                        foreign_run,
                        foreign_request,
                        report,
                    } => Some(self.record_report(foreign_run, foreign_request, report)?),
                    _ => Some(self.answer(other, at, late)?),
                }
            }
        };

        let mut effects = Vec::new();
        if !was_raised && let Some(first) = self.first_request {
            effects.extend(notices(SessionNoticePhase::Raised));
            effects.push(ApprovalEffect::RegisterDue {
                request_id: request_id(),
                deadline: first.receipt().deadline,
            });
        }
        if !had_ending && self.outcome.is_some() {
            effects.extend(notices(SessionNoticePhase::Ended));
            effects.push(ApprovalEffect::ClearDue {
                request_id: request_id(),
            });
            offer_again = self.delivery == Some(Delivery::AwaitingReport {});
        }
        if offer_again {
            let Some((ending, _)) = &self.outcome else {
                panic!("an offer needs an ending");
            };
            effects.push(ApprovalEffect::OfferEnding(Box::new(
                OfferEnding::Provider {
                    request_id: request_id(),
                    holder: approval_request_protocol::ProviderHolder {
                        session: super::fixtures::requester(),
                        router_run: router_run(),
                    },
                    ending: ending.clone(),
                },
            )));
        }
        if let Some(receipt) = receipt {
            effects.push(ApprovalEffect::Reply(receipt));
        }
        Ok(effects)
    }

    fn write_outcome(&mut self, ending: Ending, at: i64, delivery: Delivery) {
        assert!(self.outcome.is_none());
        self.outcome = Some((ending, at));
        self.delivery = Some(delivery);
    }

    fn answer(
        &mut self,
        command: Command,
        at: i64,
        late: bool,
    ) -> Result<ApprovalReceipt, Rejected> {
        let (by, choice) = match command {
            Command::AllowFromParent => (DecidedBy::Session { session: parent() }, "once"),
            Command::DeclineFromObserver => (
                DecidedBy::Session {
                    session: observer(),
                },
                "decline",
            ),
            Command::UnknownOption => (DecidedBy::Session { session: parent() }, "unknown"),
            Command::KeptFromParent => (DecidedBy::Session { session: parent() }, "kept"),
            Command::OnceFromStalePerson if self.visible_to_people => (
                DecidedBy::Person {
                    person_id: person_id(),
                },
                "once",
            ),
            Command::KeptFromStalePerson | Command::KeptFromFreshPerson
                if self.visible_to_people =>
            {
                (
                    DecidedBy::Person {
                        person_id: person_id(),
                    },
                    "kept",
                )
            }
            Command::OnceFromStalePerson
            | Command::KeptFromStalePerson
            | Command::KeptFromFreshPerson
            | Command::OutsiderAnswers => return Err(hidden()),
            unexpected => panic!("expected a model answer, got {unexpected:?}"),
        };
        self.reject_if_ended(&by)?;
        if late {
            self.write_outcome(Ending::TimedOut {}, at, Delivery::AwaitingReport {});
            return Ok(ApprovalReceipt::AnswerRefused(Refusal::TooLate {}));
        }
        match command {
            Command::UnknownOption => {
                return Err(destination(Refusal::InvalidAnswer {
                    reason: InvalidAnswer::UnofferedOption {
                        option_id: option_id("unknown"),
                    },
                }));
            }
            Command::KeptFromParent => return Err(destination(Refusal::KeptChoiceNeedsAPerson {})),
            Command::KeptFromStalePerson => {
                return Err(destination(Refusal::SecondFactorRequired {}));
            }
            _ => {}
        }
        let answer = approval_answer(choice);
        self.write_outcome(
            Ending::Decided {
                answer: answer.clone(),
                by: by.clone(),
            },
            at,
            Delivery::AwaitingReport {},
        );
        Ok(ApprovalReceipt::Answer(EndingView::DecidedWithAnswer {
            answer,
            by,
        }))
    }

    fn reject_if_ended(&self, destination_id: &DecidedBy) -> Result<(), Rejected> {
        let Some((ending, _)) = &self.outcome else {
            return Ok(());
        };
        let visible = match ending {
            Ending::Decided { answer, by } if destination_id == by => {
                EndingView::DecidedWithAnswer {
                    answer: answer.clone(),
                    by: by.clone(),
                }
            }
            Ending::Decided { by, .. } => EndingView::Decided { by: by.clone() },
            Ending::Cancelled { reason } => EndingView::Cancelled { reason: *reason },
            Ending::TimedOut {} => EndingView::TimedOut {},
        };
        Err(destination(Refusal::Ended { ending: visible }))
    }

    fn forward(
        &mut self,
        command: Command,
        at: i64,
        late: bool,
    ) -> Result<ApprovalReceipt, Rejected> {
        if matches!(command, Command::ForwardWithoutParent) {
            return Err(hidden());
        }
        self.reject_if_ended(&DecidedBy::Session { session: parent() })?;
        if late {
            self.write_outcome(Ending::TimedOut {}, at, Delivery::AwaitingReport {});
            Ok(ApprovalReceipt::PassOnRefused(Refusal::TooLate {}))
        } else {
            self.forwarded = true;
            self.visible_to_people = true;
            Ok(ApprovalReceipt::PassedOn)
        }
    }

    fn stop(&mut self, at: i64, late: bool) -> Result<ApprovalReceipt, Rejected> {
        if self.outcome.is_none() {
            let ending = if late {
                Ending::TimedOut {}
            } else {
                Ending::Cancelled { reason: STOPPED }
            };
            self.write_outcome(ending, at, GONE);
        }
        if matches!(
            self.delivery,
            Some(Delivery::NoHandoverOwed {} | Delivery::ReceiptUnknown {})
        ) {
            return Err(Rejected::Stale);
        }
        if self.delivery == Some(Delivery::AwaitingReport {}) {
            self.delivery = Some(GONE);
        }
        let Some((ending, _)) = &self.outcome else {
            panic!("stop must leave an ending");
        };
        Ok(stopped_receipt(standing(ending)))
    }

    fn authority_ended(&mut self, command: Command, at: i64, late: bool) {
        let reason = match command {
            Command::RunEnds => CancelReason::RequesterGone {},
            Command::SessionEnds => CancelReason::SessionEnded {},
            Command::EnrollmentEnds => CancelReason::EnrollmentEnded {},
            unexpected => panic!("expected an authority event, got {unexpected:?}"),
        };
        let holder_reachable = matches!(command, Command::SessionEnds);
        if self.outcome.is_some() {
            if !holder_reachable && self.delivery == Some(Delivery::AwaitingReport {}) {
                self.delivery = Some(Delivery::ReceiptUnknown {});
            }
            return;
        }
        let ending = if late {
            Ending::TimedOut {}
        } else {
            Ending::Cancelled { reason }
        };
        let delivery = match (holder_reachable, late) {
            (true, _) => Delivery::AwaitingReport {},
            (false, true) => Delivery::ReceiptUnknown {},
            (false, false) => Delivery::NoHandoverOwed {},
        };
        self.write_outcome(ending, at, delivery);
    }

    fn check_delivery_command(&self, event: &'static str, foreign: bool) -> Result<(), Rejected> {
        if self.outcome.is_none() {
            return Err(Rejected::NotAllowed {
                state: if self.forwarded {
                    "passedOn"
                } else {
                    "pending"
                },
                event,
            });
        }
        if foreign {
            return Err(Rejected::Stale);
        }
        Ok(())
    }

    fn record_report(
        &mut self,
        foreign_run: bool,
        foreign_request: bool,
        report: HolderReport,
    ) -> Result<ApprovalReceipt, Rejected> {
        self.check_delivery_command("report", foreign_run || foreign_request)?;
        match self.delivery {
            Some(Delivery::AwaitingReport {}) => {
                self.delivery = Some(Delivery::Reported { report })
            }
            Some(Delivery::Reported { report: first }) if first == report => {}
            Some(Delivery::Reported { .. }) => return Err(domain(ApprovalRefusal::ReportDiffers)),
            _ => return Err(domain(ApprovalRefusal::NoHandoverOwed)),
        }
        Ok(ApprovalReceipt::Report(
            ReportDeliveryReply::DeliveryRecorded {},
        ))
    }

    fn assert_projection(&self, actual: &ApprovalState) {
        if let Some(ending) = self.erased_ending {
            assert_eq!(
                actual,
                &ApprovalState::Erased {
                    request_id: request_id(),
                    router_run: router_run(),
                    ending,
                }
            );
            assert!(actual.is_terminal());
            assert_eq!(ApprovalLifecycle::next_due(actual), None);
            return;
        }
        let expected = match (
            self.first_request,
            self.stopped_before_raise,
            &self.outcome,
            self.delivery,
        ) {
            (None, None, None, None) => ApprovalState::Unraised,
            (None, Some(at), None, None) => ApprovalState::StoppedBeforeRaise {
                request_id: request_id(),
                router_run: router_run(),
                cause: HolderStopCause::TurnStopped,
                stopped_at: time(at),
            },
            (Some(first), None, outcome, delivery) => {
                let mut destinations = vec![
                    ApprovalDestination::Session(parent()),
                    ApprovalDestination::Session(observer()),
                ];
                if self.visible_to_people {
                    destinations.push(ApprovalDestination::People);
                }
                let saved = Box::new(RetainedApproval {
                    raise: first.contents(),
                    receipt: first.receipt(),
                    destinations,
                });
                match (outcome, delivery) {
                    (None, None) => ApprovalState::Waiting {
                        saved,
                        status: if self.forwarded {
                            WaitingStatus::PassedOn
                        } else {
                            WaitingStatus::Pending
                        },
                    },
                    (Some((ending, at)), Some(delivery)) => ApprovalState::Ended {
                        saved,
                        closed: EndedApproval {
                            delivery,
                            ending: ending.clone(),
                            ended_at: time(*at),
                        },
                    },
                    unexpected => panic!("inconsistent outcome and delivery: {unexpected:?}"),
                }
            }
            unexpected => panic!("inconsistent model phase: {unexpected:?}"),
        };
        assert_eq!(&expected, actual);
        assert_eq!(
            ApprovalLifecycle::next_due(actual),
            self.first_request
                .filter(|_| self.outcome.is_none())
                .map(|first| time(first.at + ONE_HOUR_MILLIS)),
        );
        assert_eq!(
            actual.is_terminal(),
            self.stopped_before_raise.is_some()
                || self
                    .delivery
                    .is_some_and(|delivery| delivery != (Delivery::AwaitingReport {})),
        );
    }
}

fn notices(phase: SessionNoticePhase) -> [ApprovalEffect; 2] {
    [parent(), observer()].map(|session| ApprovalEffect::StoreSessionNotice {
        request_id: request_id(),
        session,
        phase,
    })
}

fn stopped_receipt(ending: StopReceiptEnding) -> ApprovalReceipt {
    ApprovalReceipt::Stop(StopReply::StopReceipt { ending })
}

fn destination(refusal: Refusal) -> Rejected {
    domain(ApprovalRefusal::Destination { refusal })
}

fn hidden() -> Rejected {
    destination(Refusal::NotVisible {})
}

fn domain(refusal: ApprovalRefusal) -> Rejected {
    TransitionError::Domain(refusal)
}

#[hegel::test(test_cases = 400)]
fn generated_histories_match_the_independent_approval_ledger(case: hegel::TestCase) {
    let mut actual = ApprovalState::Unraised;
    let mut expected = ApprovalLedger::default();
    let mut now = 0;
    let people_on_first_raise = case.draw(gs::booleans());
    let start_commands = history_prefix(case.draw(gs::integers::<u8>().max_value(14)));
    let tick_choices = [
        0,
        1,
        79,
        ONE_HOUR_MILLIS - 1,
        ONE_HOUR_MILLIS,
        ONE_HOUR_MILLIS + 1,
        SIXTY_DAYS_MILLIS - 1,
        SIXTY_DAYS_MILLIS,
        SIXTY_DAYS_MILLIS + 1,
    ];

    let mut holder_can_act = true;
    let mut compare_step = |command: Command, at: i64, include_people: bool| {
        // Registration is the applying caller's responsibility. A retired holder
        // supplies no reducer event, even when the request was never raised.
        if !holder_can_act && command.requires_live_holder() {
            expected.assert_projection(&actual);
            return;
        }
        if matches!(command, Command::RunEnds | Command::EnrollmentEnds) {
            holder_can_act = false;
        }
        let observed = step(&actual, command.event(include_people), at);
        match (expected.record(command, at, include_people), observed) {
            (Ok(effects), Ok((next, actual_effects))) => {
                assert_eq!(effects, actual_effects, "effects for {command:?} at {at}");
                actual = next;
            }
            (Err(refusal), Err(actual_refusal)) => {
                assert_eq!(refusal, actual_refusal, "{command:?} at {at}")
            }
            pair => panic!("{command:?} at {at}: ledger and reducer differ: {pair:?}"),
        }
        expected.assert_projection(&actual);
    };

    for command in start_commands {
        if matches!(command, Command::Erase) {
            now = SIXTY_DAYS_MILLIS;
        }
        compare_step(command, now, people_on_first_raise);
    }
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(30)) {
        let command = case.draw(gs::sampled_from(COMMANDS.to_vec()));
        let command = if matches!(command, Command::Report { .. }) {
            command.with_foreign_request(case.draw(gs::booleans()))
        } else {
            command
        };
        let tick = case.draw(gs::sampled_from(tick_choices.to_vec()));
        now += tick;
        compare_step(command, now, case.draw(gs::booleans()));
    }
}
