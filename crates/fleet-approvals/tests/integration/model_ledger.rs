//! Independent expectations in terms of first saves, claims, audiences and authority.
use std::collections::BTreeSet;

use approval_request_protocol::{
    DeliverEndingReply, PassOnReply, RegisterRunReply, RunRefusal, StopReceiptEnding,
};

use fleet_approvals::ApprovalRefusal;

use super::fixtures::HOUR;

pub(super) const ROWS: [RowScope; 5] = [
    RowScope {
        run: 0,
        delegate: true,
        parked: false,
    },
    RowScope {
        run: 0,
        delegate: false,
        parked: false,
    },
    RowScope {
        run: 1,
        delegate: true,
        parked: false,
    },
    RowScope {
        run: 2,
        delegate: false,
        parked: false,
    },
    RowScope {
        run: 0,
        delegate: false,
        parked: true,
    },
];
pub(super) const RUN_ENROLLMENTS: [usize; 3] = [0, 0, 1];

#[derive(Clone, Copy, Debug)]
pub(super) struct RowScope {
    pub run: usize,
    pub delegate: bool,
    pub parked: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Actor {
    FirstPerson,
    SecondPerson,
    Parent,
    Unrelated,
}

impl Actor {
    const fn person(self) -> bool {
        matches!(self, Self::FirstPerson | Self::SecondPerson)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Step {
    Raise {
        row: usize,
        changed: bool,
    },
    Decide {
        row: usize,
        operation: u16,
        choice: u8,
        actor: Actor,
    },
    Stop {
        row: usize,
        changed: bool,
    },
    Forward {
        row: usize,
        actor: Actor,
    },
    Read {
        row: usize,
        actor: Actor,
    },
    Alarm {
        row: usize,
        stale_token: bool,
    },
    Deliver {
        row: usize,
        reply: DeliverEndingReply,
        wrong_ending: bool,
    },
    Register(usize),
    EndSession {
        enrollment: usize,
        parent: bool,
    },
    EndEnrollment(usize),
    ReplaceEnrollment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Ending {
    Decided { choice: u8, actor: Actor },
    TimedOut,
    Stopped,
    SessionEnded,
    RequesterGone,
    EnrollmentEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Delivery {
    Unpaid,
    Received,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RaiseResult {
    Receipt(i64),
    AlreadyStopped,
    ContentsDiffer,
    SessionEnded,
    Refused(RunRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Decision {
    Won { choice: u8 },
    Standing(Ending),
    NotVisible,
    Invalid,
    TooLate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct View {
    pub raised_at: i64,
    pub forwarded: bool,
    pub ending: Option<(Ending, i64, Delivery)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Offer {
    pub changed: bool,
    pub ending: Ending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum OperationEffect {
    Notice(usize),
    Due(usize, i64),
    Clear(usize),
    Offer(usize, Offer),
    Registered(RegisterRunReply),
    Raise(usize, RaiseResult),
    Decide(usize, Actor, Decision),
    Read(usize, Actor, Option<View>),
    Forward(usize, Actor, PassOnReply),
    Stop(StopResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StopResult {
    Stands(StopReceiptEnding),
    NotVisible,
    Refused(RunRefusal),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RequestLedger {
    pub first_save: Option<(bool, i64)>,
    pub marker: Option<bool>,
    pub forwarded: bool,
    pub ending: Option<(Ending, i64, Delivery)>,
    pub offered: Option<Offer>,
    pub accepted_operation: Option<u16>,
}

impl RequestLedger {
    fn waiting(self) -> bool {
        self.first_save.is_some() && self.ending.is_none()
    }
    fn late(self, now: i64) -> bool {
        self.first_save.is_some_and(|(_, at)| now >= at + HOUR)
    }
    fn view(self) -> Option<View> {
        self.first_save.map(|(_, raised_at)| View {
            raised_at,
            forwarded: self.forwarded,
            ending: self.ending,
        })
    }
}

pub(super) struct Ledger {
    pub rows: [RequestLedger; ROWS.len()],
    active_enrollment: Option<usize>,
    closed_enrollments: BTreeSet<usize>,
    ended_requesters: BTreeSet<usize>,
    ended_parents: BTreeSet<usize>,
    registered: Option<usize>,
    retired: BTreeSet<usize>,
}

struct DecisionAttempt {
    row: usize,
    operation: u16,
    choice: u8,
    actor: Actor,
}

#[path = "model_ledger_authority.rs"]
mod ledger_authority;

impl Ledger {
    pub fn new() -> Self {
        Self {
            rows: [RequestLedger::default(); ROWS.len()],
            active_enrollment: Some(0),
            closed_enrollments: BTreeSet::new(),
            ended_requesters: BTreeSet::new(),
            ended_parents: BTreeSet::new(),
            registered: Some(0),
            retired: BTreeSet::new(),
        }
    }

    fn finish(
        &mut self,
        row: usize,
        requested: Ending,
        delivery: Delivery,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) -> Ending {
        let ledger = &mut self.rows[row];
        assert!(ledger.waiting());
        let ending = if ledger.late(sampled) {
            Ending::TimedOut
        } else {
            requested
        };
        let (_, first_save) = ledger.first_save.unwrap();
        ledger.ending = Some((ending, sampled.max(first_save), delivery));
        effects.push(OperationEffect::Clear(row));
        if delivery == Delivery::Unpaid {
            let offer = Offer {
                changed: ledger.first_save.unwrap().0,
                ending,
            };
            ledger.offered = Some(offer);
            effects.push(OperationEffect::Offer(row, offer));
        }
        ending
    }

    fn raise(
        &mut self,
        row: usize,
        changed: bool,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) -> RaiseResult {
        let scope = ROWS[row];
        if let Some(refusal) = self.holder_refusal(scope.run) {
            return RaiseResult::Refused(refusal);
        }
        if let Some((old, at)) = self.rows[row].first_save {
            if changed != old {
                return RaiseResult::ContentsDiffer;
            }
            if self.rows[row].waiting() && self.rows[row].late(sampled) {
                self.finish(row, Ending::TimedOut, Delivery::Unpaid, sampled, effects);
            }
            return RaiseResult::Receipt(at);
        }
        if let Some(old) = self.rows[row].marker {
            return if changed == old {
                RaiseResult::AlreadyStopped
            } else {
                RaiseResult::ContentsDiffer
            };
        }
        if !scope.parked && self.ended_requesters.contains(&RUN_ENROLLMENTS[scope.run]) {
            return RaiseResult::SessionEnded;
        }
        self.rows[row].first_save = Some((changed, sampled));
        if scope.delegate {
            effects.push(OperationEffect::Notice(row));
        }
        effects.push(OperationEffect::Due(row, sampled + HOUR));
        RaiseResult::Receipt(sampled)
    }

    fn stop(
        &mut self,
        row: usize,
        changed: bool,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) -> StopResult {
        if let Some(refusal) = self.holder_refusal(ROWS[row].run) {
            return StopResult::Refused(refusal);
        }
        if let Some((expected, _)) = self.rows[row].first_save {
            if expected != changed {
                return StopResult::NotVisible;
            }
            if self.rows[row].waiting() {
                let ending = self.finish(row, Ending::Stopped, Delivery::Closed, sampled, effects);
                return StopResult::Stands(standing(row, ending));
            }
            let (ending, at, delivery) = self.rows[row].ending.unwrap();
            if delivery == Delivery::Unpaid {
                self.rows[row].ending = Some((ending, at, Delivery::Closed));
            }
            return StopResult::Stands(standing(row, ending));
        }

        if self.rows[row]
            .marker
            .is_some_and(|expected| expected != changed)
        {
            return StopResult::NotVisible;
        }
        self.rows[row].marker = Some(changed);
        StopResult::Stands(standing(row, Ending::Stopped))
    }

    fn decide(
        &mut self,
        attempt: DecisionAttempt,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) -> Decision {
        let DecisionAttempt {
            row,
            actor,
            operation,
            choice,
        } = attempt;
        if !self.visible(row, actor) {
            return Decision::NotVisible;
        }
        let answer_fits = if ROWS[row].parked {
            choice <= 2 && actor.person()
        } else {
            choice <= 2 || (choice <= 5 && actor.person())
        };
        if let Some((Ending::TimedOut, ..)) = self.rows[row].ending {
            Decision::TooLate
        } else if let Some((ending, ..)) = self.rows[row].ending {
            Decision::Standing(ending)
        } else if self.rows[row].late(sampled) {
            self.finish(row, Ending::TimedOut, Delivery::Unpaid, sampled, effects);
            Decision::TooLate
        } else if answer_fits {
            self.finish(
                row,
                Ending::Decided { choice, actor },
                Delivery::Unpaid,
                sampled,
                effects,
            );
            self.rows[row].accepted_operation = Some(operation);
            Decision::Won { choice }
        } else {
            Decision::Invalid
        }
    }

    fn forward(
        &mut self,
        row: usize,
        actor: Actor,
        sampled: i64,
        effects: &mut Vec<OperationEffect>,
    ) -> PassOnReply {
        if actor != Actor::Parent || !self.visible(row, actor) {
            return PassOnReply::NotVisible {};
        }
        let forwarded = self.rows[row].forwarded;
        if self.rows[row].waiting() && self.rows[row].late(sampled) {
            self.finish(row, Ending::TimedOut, Delivery::Unpaid, sampled, effects);
            return if forwarded {
                PassOnReply::Forwarded {}
            } else {
                PassOnReply::TooLate {}
            };
        }
        if forwarded {
            return PassOnReply::Forwarded {};
        }
        if self.rows[row].ending.is_some() {
            return PassOnReply::Ended {};
        }
        self.rows[row].forwarded = true;
        PassOnReply::Forwarded {}
    }

    pub fn apply(
        &mut self,
        step: Step,
        sampled: i64,
    ) -> Result<Vec<OperationEffect>, ApprovalRefusal> {
        let mut effects = Vec::new();
        match step {
            Step::Raise { row, changed } => {
                let result = self.raise(row, changed, sampled, &mut effects);
                effects.push(OperationEffect::Raise(row, result));
            }
            Step::Decide {
                row,
                operation,
                choice,
                actor,
            } => {
                let attempt = DecisionAttempt {
                    row,
                    operation,
                    choice,
                    actor,
                };
                let result = self.decide(attempt, sampled, &mut effects);
                effects.push(OperationEffect::Decide(row, actor, result));
            }
            Step::Stop { row, changed } => {
                let result = self.stop(row, changed, sampled, &mut effects);
                effects.push(OperationEffect::Stop(result));
            }
            Step::Forward { row, actor } => {
                let result = self.forward(row, actor, sampled, &mut effects);
                effects.push(OperationEffect::Forward(row, actor, result));
            }
            Step::Read { row, actor } => {
                let view = if self.visible(row, actor) {
                    if self.rows[row].waiting() && self.rows[row].late(sampled) {
                        self.finish(
                            row,
                            Ending::TimedOut,
                            Delivery::Unpaid,
                            sampled,
                            &mut effects,
                        );
                    }
                    self.rows[row].view()
                } else {
                    None
                };
                effects.push(OperationEffect::Read(row, actor, view));
            }
            Step::Alarm { row, stale_token } => {
                if !stale_token && self.rows[row].waiting() && self.rows[row].late(sampled) {
                    self.finish(
                        row,
                        Ending::TimedOut,
                        Delivery::Unpaid,
                        sampled,
                        &mut effects,
                    );
                }
            }
            Step::Deliver {
                row,
                reply,
                wrong_ending,
            } => {
                if !wrong_ending
                    && self.holder_refusal(ROWS[row].run).is_none()
                    && let Some((ending, at, delivery)) = self.rows[row].ending
                {
                    let delivery = match reply {
                        DeliverEndingReply::Received {} => Delivery::Received,
                        DeliverEndingReply::NotHeld {} if delivery == Delivery::Unpaid => {
                            Delivery::Closed
                        }
                        DeliverEndingReply::NotHeld {}
                        | DeliverEndingReply::StopOwed {}
                        | DeliverEndingReply::HolderNotReady {}
                        | DeliverEndingReply::EndingDiffers {} => delivery,
                    };
                    self.rows[row].ending = Some((ending, at, delivery));
                }
            }
            Step::Register(run) => self.register_run(run, sampled, &mut effects),
            Step::EndSession { enrollment, parent } => {
                self.end_session(enrollment, parent, sampled, &mut effects)
            }
            Step::EndEnrollment(enrollment) => {
                self.end_enrollment(enrollment, sampled, &mut effects)
            }
            Step::ReplaceEnrollment => self.replace_enrollment(sampled, &mut effects),
        }
        Ok(effects)
    }
}

fn standing(row: usize, ending: Ending) -> StopReceiptEnding {
    use approval_request_protocol::{CancelReason, HolderStopCause};
    match ending {
        Ending::Decided { .. } => StopReceiptEnding::DecidedEarlier {},
        Ending::TimedOut => StopReceiptEnding::TimedOut {},
        Ending::Stopped => StopReceiptEnding::Cancelled {
            reason: CancelReason::Stopped {
                cause: if ROWS[row].parked {
                    HolderStopCause::CallWithdrawn
                } else {
                    HolderStopCause::AskReplaced
                },
            },
        },
        Ending::SessionEnded => StopReceiptEnding::Cancelled {
            reason: CancelReason::SessionEnded {},
        },
        Ending::RequesterGone => StopReceiptEnding::Cancelled {
            reason: CancelReason::RequesterGone {},
        },
        Ending::EnrollmentEnded => StopReceiptEnding::Cancelled {
            reason: CancelReason::EnrollmentEnded {},
        },
    }
}
