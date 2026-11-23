//! Independent expectations in terms of first saves, claims, audiences and authority.
use std::collections::{BTreeMap, BTreeSet};

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
        fresh_factor: bool,
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
    OperationConflict,
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
    Save {
        row: usize,
        actor: Actor,
        operation: u16,
        result: Decision,
    },
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
    pub floor: i64,
    pub rows: [RequestLedger; ROWS.len()],
    active_enrollment: Option<usize>,
    closed_enrollments: BTreeSet<usize>,
    ended_requesters: BTreeSet<usize>,
    ended_parents: BTreeSet<usize>,
    registered: Option<usize>,
    retired: BTreeSet<usize>,
    operations: BTreeMap<u16, (usize, Actor, u8, Decision)>,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            floor: 0,
            rows: [RequestLedger::default(); ROWS.len()],
            active_enrollment: Some(0),
            closed_enrollments: BTreeSet::new(),
            ended_requesters: BTreeSet::new(),
            ended_parents: BTreeSet::new(),
            registered: Some(0),
            retired: BTreeSet::new(),
            operations: BTreeMap::new(),
        }
    }

    pub fn decisions(&self) -> usize {
        self.operations.len()
    }

    fn visible(&self, row: usize, actor: Actor) -> bool {
        if self.rows[row].first_save.is_none() {
            return false;
        }
        if actor.person() {
            return !ROWS[row].delegate || self.rows[row].forwarded;
        }
        let enrollment = RUN_ENROLLMENTS[ROWS[row].run];
        actor == Actor::Parent
            && ROWS[row].delegate
            && self.active_enrollment == Some(enrollment)
            && !self.ended_parents.contains(&enrollment)
    }

    fn enrollment_refusal(&self, enrollment: usize) -> Option<RunRefusal> {
        if self.closed_enrollments.contains(&enrollment) {
            Some(RunRefusal::EnrollmentEnded {})
        } else if self.active_enrollment != Some(enrollment) {
            Some(RunRefusal::RunNotRegistered {})
        } else {
            None
        }
    }

    fn holder_refusal(&self, run: usize) -> Option<RunRefusal> {
        self.enrollment_refusal(RUN_ENROLLMENTS[run]).or_else(|| {
            if self.retired.contains(&run) {
                Some(RunRefusal::RunRetired {})
            } else if self.registered != Some(run) {
                Some(RunRefusal::RunNotRegistered {})
            } else {
                None
            }
        })
    }

    fn finish(
        &mut self,
        row: usize,
        requested: Ending,
        delivery: Delivery,
        effects: &mut Vec<OperationEffect>,
    ) -> Ending {
        let ledger = &mut self.rows[row];
        assert!(ledger.waiting());
        let ending = if ledger.late(self.floor) {
            Ending::TimedOut
        } else {
            requested
        };
        ledger.ending = Some((ending, self.floor, delivery));
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

    fn close_deliveries(
        &mut self,
        matches_run: impl Fn(usize) -> bool,
        reason: Ending,
        effects: &mut Vec<OperationEffect>,
    ) {
        for (row, scope) in ROWS.iter().enumerate() {
            if !matches_run(scope.run) {
                continue;
            }
            if self.rows[row].waiting() {
                self.finish(row, reason, Delivery::Closed, effects);
            } else if let Some((ending, at, Delivery::Unpaid)) = self.rows[row].ending {
                self.rows[row].ending = Some((ending, at, Delivery::Closed));
            }
        }
    }

    fn end_enrollment(&mut self, enrollment: usize, effects: &mut Vec<OperationEffect>) {
        if self.active_enrollment != Some(enrollment) {
            return;
        }
        self.closed_enrollments.insert(enrollment);
        self.active_enrollment = None;
        self.registered = None;
        self.close_deliveries(
            |run| RUN_ENROLLMENTS[run] == enrollment,
            Ending::EnrollmentEnded,
            effects,
        );
    }

    fn raise(
        &mut self,
        row: usize,
        changed: bool,
        effects: &mut Vec<OperationEffect>,
    ) -> RaiseResult {
        let scope = ROWS[row];
        if let Some(refusal) = self.holder_refusal(scope.run) {
            return RaiseResult::Refused(refusal);
        }
        if let Some((old, at)) = self.rows[row].first_save {
            return if changed == old {
                RaiseResult::Receipt(at)
            } else {
                RaiseResult::ContentsDiffer
            };
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
        self.rows[row].first_save = Some((changed, self.floor));
        if scope.delegate {
            effects.push(OperationEffect::Notice(row));
        }
        effects.push(OperationEffect::Due(row, self.floor + HOUR));
        RaiseResult::Receipt(self.floor)
    }

    fn stop(
        &mut self,
        row: usize,
        changed: bool,
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
                let ending = self.finish(row, Ending::Stopped, Delivery::Closed, effects);
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
        row: usize,
        actor: Actor,
        operation: u16,
        choice: u8,
        fresh_factor: bool,
        effects: &mut Vec<OperationEffect>,
    ) -> Decision {
        if !self.visible(row, actor) {
            return Decision::NotVisible;
        }
        if let Some(&(prior_row, prior_actor, prior_choice, result)) =
            self.operations.get(&operation)
        {
            return if (prior_row, prior_actor, prior_choice) == (row, actor, choice) {
                result
            } else {
                Decision::OperationConflict
            };
        }
        let answer_fits = if ROWS[row].parked {
            choice <= 2 && actor.person() && fresh_factor
        } else {
            choice <= 2 || (choice <= 5 && actor.person() && fresh_factor)
        };
        let result = if let Some((ending, ..)) = self.rows[row].ending {
            Decision::Standing(ending)
        } else if self.rows[row].late(self.floor) {
            self.finish(row, Ending::TimedOut, Delivery::Unpaid, effects);
            Decision::TooLate
        } else if answer_fits {
            self.finish(
                row,
                Ending::Decided { choice, actor },
                Delivery::Unpaid,
                effects,
            );
            Decision::Won { choice }
        } else {
            Decision::Invalid
        };
        self.operations
            .insert(operation, (row, actor, choice, result));
        effects.push(OperationEffect::Save {
            row,
            actor,
            operation,
            result,
        });
        result
    }

    fn forward(
        &mut self,
        row: usize,
        actor: Actor,
        effects: &mut Vec<OperationEffect>,
    ) -> PassOnReply {
        if actor != Actor::Parent || !self.visible(row, actor) {
            return PassOnReply::NotVisible {};
        }
        let forwarded = self.rows[row].forwarded;
        if self.rows[row].waiting() && self.rows[row].late(self.floor) {
            self.finish(row, Ending::TimedOut, Delivery::Unpaid, effects);
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
        if let Step::Raise { row, .. } = step {
            let route = ROWS[row];
            let enrollment = RUN_ENROLLMENTS[route.run];
            let fresh = self.rows[row].first_save.is_none() && self.rows[row].marker.is_none();
            if fresh
                && route.delegate
                && self.holder_refusal(route.run).is_none()
                && !self.ended_requesters.contains(&enrollment)
                && self.ended_parents.contains(&enrollment)
            {
                return Err(ApprovalRefusal::InvalidInitialRoute);
            }
        }
        self.floor = self.floor.max(sampled);
        let mut effects = Vec::new();
        match step {
            Step::Raise { row, changed } => {
                let result = self.raise(row, changed, &mut effects);
                effects.push(OperationEffect::Raise(row, result));
            }
            Step::Decide {
                row,
                operation,
                choice,
                actor,
                fresh_factor,
            } => {
                let result = self.decide(row, actor, operation, choice, fresh_factor, &mut effects);
                effects.push(OperationEffect::Decide(row, actor, result));
            }
            Step::Stop { row, changed } => {
                let result = self.stop(row, changed, &mut effects);
                effects.push(OperationEffect::Stop(result));
            }
            Step::Forward { row, actor } => {
                let result = self.forward(row, actor, &mut effects);
                effects.push(OperationEffect::Forward(row, actor, result));
            }
            Step::Read { row, actor } => {
                let view = if self.visible(row, actor) {
                    if self.rows[row].waiting() && self.rows[row].late(self.floor) {
                        self.finish(row, Ending::TimedOut, Delivery::Unpaid, &mut effects);
                    }
                    self.rows[row].view()
                } else {
                    None
                };
                effects.push(OperationEffect::Read(row, actor, view));
            }
            Step::Alarm { row, stale_token } => {
                if !stale_token && self.rows[row].waiting() && self.rows[row].late(self.floor) {
                    self.finish(row, Ending::TimedOut, Delivery::Unpaid, &mut effects);
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
            Step::Register(run) => {
                let response = if let Some(refusal) = self.enrollment_refusal(RUN_ENROLLMENTS[run])
                {
                    match refusal {
                        RunRefusal::EnrollmentEnded {} => RegisterRunReply::EnrollmentEnded {},
                        RunRefusal::RunNotRegistered {} => RegisterRunReply::RunNotRegistered {},
                        RunRefusal::RunRetired {} => RegisterRunReply::RunRetired {},
                    }
                } else if self.retired.contains(&run) {
                    RegisterRunReply::RunRetired {}
                } else {
                    if let Some(previous) = self
                        .registered
                        .replace(run)
                        .filter(|previous| *previous != run)
                    {
                        self.retired.insert(previous);
                        self.close_deliveries(
                            |candidate| candidate == previous,
                            Ending::RequesterGone,
                            &mut effects,
                        );
                    }
                    RegisterRunReply::Registered {}
                };
                effects.push(OperationEffect::Registered(response));
            }
            Step::EndSession { enrollment, parent } => {
                if self.active_enrollment == Some(enrollment) {
                    if parent {
                        self.ended_parents.insert(enrollment);
                    } else if self.ended_requesters.insert(enrollment) {
                        for (row, scope) in ROWS.iter().enumerate() {
                            if !scope.parked
                                && RUN_ENROLLMENTS[scope.run] == enrollment
                                && self.rows[row].waiting()
                            {
                                self.finish(
                                    row,
                                    Ending::SessionEnded,
                                    Delivery::Unpaid,
                                    &mut effects,
                                );
                            }
                        }
                    }
                }
            }
            Step::EndEnrollment(enrollment) => self.end_enrollment(enrollment, &mut effects),
            Step::ReplaceEnrollment => {
                if !self.closed_enrollments.contains(&1) {
                    self.end_enrollment(0, &mut effects);
                    self.active_enrollment = Some(1);
                }
            }
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
