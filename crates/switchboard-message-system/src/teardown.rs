use std::collections::BTreeSet;

use crate::{EnrollmentPair, FrontRefusal, Generation, StopResult, StopTarget, UnitPiece};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuiesceReason {
    NotEnrolled,
    EnrollmentEnded,
    BuildFailed,
    PieceExited,
    DifferentPair {
        pinned: EnrollmentPair,
        observed: EnrollmentPair,
    },
    Closing,
}

impl QuiesceReason {
    pub(crate) const fn refusal(&self) -> FrontRefusal {
        match self {
            Self::NotEnrolled => FrontRefusal::NotEnrolled,
            Self::EnrollmentEnded => FrontRefusal::EnrollmentEnded,
            Self::BuildFailed | Self::PieceExited | Self::DifferentPair { .. } | Self::Closing => {
                FrontRefusal::Restarting
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Quiescing {
    pub(crate) reason: QuiesceReason,
    generation: Option<Generation>,
    pub(crate) joining: Option<StopTarget>,
    pub(crate) outstanding: BTreeSet<StopTarget>,
}

impl Quiescing {
    pub(crate) const fn new(generation: Option<Generation>, reason: QuiesceReason) -> Self {
        let joining = match generation {
            Some(generation) => Some(StopTarget::Unit {
                generation,
                piece: UnitPiece::Driver,
            }),
            None => Some(StopTarget::EnrollmentStore),
        };
        Self {
            reason,
            generation,
            joining,
            outstanding: BTreeSet::new(),
        }
    }

    pub const fn generation(&self) -> Option<Generation> {
        self.generation
    }

    pub const fn reason(&self) -> &QuiesceReason {
        &self.reason
    }

    pub const fn joining(&self) -> Option<StopTarget> {
        self.joining
    }

    pub const fn outstanding(&self) -> &BTreeSet<StopTarget> {
        &self.outstanding
    }

    pub(crate) fn close(&mut self) -> Option<StopTarget> {
        if matches!(self.reason, QuiesceReason::Closing) {
            return None;
        }
        self.reason = QuiesceReason::Closing;
        if self.joining.is_none() {
            self.joining = Some(StopTarget::EnrollmentStore);
            return self.joining;
        }
        None
    }

    pub(crate) fn accept(&mut self, target: StopTarget, result: StopResult) -> Step {
        if self.joining != Some(target) {
            return if result == StopResult::Joined && self.outstanding.remove(&target) {
                Step::LateJoin
            } else {
                Step::Ignored
            };
        }
        if result == StopResult::TimedOut {
            self.outstanding.insert(target);
        }
        self.joining = match target {
            StopTarget::Unit { generation, piece } => match piece {
                UnitPiece::Driver => Some(StopTarget::Unit {
                    generation,
                    piece: UnitPiece::Custody,
                }),
                UnitPiece::Custody => Some(StopTarget::Unit {
                    generation,
                    piece: UnitPiece::Settings,
                }),
                UnitPiece::Settings => Some(StopTarget::Unit {
                    generation,
                    piece: UnitPiece::LeaseAdapter,
                }),
                UnitPiece::LeaseAdapter if matches!(self.reason, QuiesceReason::Closing) => {
                    Some(StopTarget::EnrollmentStore)
                }
                UnitPiece::LeaseAdapter => None,
            },
            StopTarget::EnrollmentStore => None,
        };
        Step::Advanced
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Advanced,
    LateJoin,
    Ignored,
}
