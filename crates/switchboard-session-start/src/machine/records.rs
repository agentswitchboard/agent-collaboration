use fleet_identity_protocol::{EndFleetSession, EndSessionTarget, RouterEndCause};

use super::{
    AcceptedStart, AfterClose, CleanupOutcome, FinishedOutcome, NOT_EXPECTED, Outputs,
    RecordedStop, Recording, Reduced, SnapshotFacts, SnapshotKind, Stage, StartDirective,
    StartSnapshot, StopReason, WindDown,
};
use crate::{
    CloseResult, ConversationFact, EndLeg, EndNotSent, FailureReason, PointerFact, SaveResult,
};

impl AcceptedStart {
    pub(super) fn snapshot_facts(&self) -> Box<SnapshotFacts> {
        Box::new(SnapshotFacts {
            envelope: self.envelope.clone(),
            attempt: self.attempt,
            facts: self.facts.clone(),
        })
    }

    pub(super) fn save(&mut self, snapshot: StartSnapshot, outputs: &mut Outputs<'_>) -> Reduced {
        self.stage = Stage::Saving(snapshot.clone());
        outputs.directive(StartDirective::Save {
            expected: self.revision,
            snapshot,
        })
    }

    pub(super) fn saved(&mut self, result: SaveResult, outputs: &mut Outputs<'_>) -> Reduced {
        let Stage::Saving(snapshot) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        let kind = snapshot.kind();
        let revision = match result {
            SaveResult::Saved(revision) => revision,
            SaveResult::NotCommitted => {
                return self.record_lost(RecordedStop::NotCommitted, kind, outputs);
            }
            SaveResult::RevisionMismatch => {
                return self.record_lost(RecordedStop::Conflict, kind, outputs);
            }
            SaveResult::OutcomeNotKnown => {
                return self.record_lost(RecordedStop::NotKnown, kind, outputs);
            }
        };
        self.revision = Some(revision);
        self.recording = Recording::Recorded;
        self.facts = snapshot.start().facts.clone();
        match snapshot {
            StartSnapshot::CreateIntended(_) => {
                self.stage = Stage::CreateSent;
                outputs.directive(StartDirective::SendCreate(
                    self.envelope.create_request(self.attempt),
                ))
            }
            StartSnapshot::LaunchIntended(_) => {
                self.stage = Stage::Launching;
                outputs.directive(StartDirective::Launch)
            }
            StartSnapshot::Held(_) => {
                self.stage = Stage::SettingUp;
                outputs.directive(StartDirective::SetUp)
            }
            StartSnapshot::BindIntended(_) => self.send_bind(outputs),
            StartSnapshot::CleaningUp { outcome, .. } => self.run_cleanup(outcome.clone(), outputs),
            StartSnapshot::CreateUnknown(_) => {
                self.stage = Stage::CreateUnknown;
                self.tell_unknown(outputs);
                Ok(())
            }
            StartSnapshot::BindUnknown(_) => {
                self.stage = Stage::BindUnknown;
                self.tell_unknown(outputs);
                Ok(())
            }
            StartSnapshot::Failed { reason, .. } => {
                self.finish(FinishedOutcome::Failed(reason.clone()), outputs);
                Ok(())
            }
            StartSnapshot::Settled(_) => {
                self.finish(FinishedOutcome::Settled, outputs);
                Ok(())
            }
            StartSnapshot::Fenced(_) => {
                self.finish(FinishedOutcome::Fenced, outputs);
                Ok(())
            }
            StartSnapshot::Unknown { reason, .. } => {
                self.finish(FinishedOutcome::Unknown(*reason), outputs);
                Ok(())
            }
            StartSnapshot::Opened { .. } => Err(NOT_EXPECTED),
        }
    }

    pub(super) fn record_lost(
        &mut self,
        stop: RecordedStop,
        while_saving: SnapshotKind,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let why = match stop {
            RecordedStop::NotCommitted => {
                self.recording = Recording::NotRecorded;
                StopReason::RecordNotCommitted
            }
            RecordedStop::Conflict => {
                self.recording = Recording::Conflict;
                StopReason::RecordConflict
            }
            RecordedStop::NotKnown => {
                self.recording = Recording::NotKnown;
                StopReason::RecordNotKnown
            }
        };
        if while_saving == SnapshotKind::LaunchIntended {
            let Stage::Saving(StartSnapshot::LaunchIntended(snapshot)) = &self.stage else {
                return Err(NOT_EXPECTED);
            };
            let PointerFact::Committed(issue) = &snapshot.facts.pointer else {
                return Err(NOT_EXPECTED);
            };
            self.facts.pointer = if stop == RecordedStop::NotCommitted {
                PointerFact::None
            } else {
                PointerFact::PossiblyCommitted(issue.clone())
            };
        }
        if self.facts.end == EndLeg::Due {
            self.facts.end = EndLeg::NotSent(EndNotSent::RecordUnavailable);
        }
        if while_saving == SnapshotKind::CreateIntended
            && self.revision.is_none()
            && stop == RecordedStop::NotCommitted
        {
            self.finish(
                FinishedOutcome::Failed(FailureReason::CouldNotSave),
                outputs,
            );
        } else if matches!(self.facts.conversation, ConversationFact::Held(_)) {
            self.stage = Stage::Closing(AfterClose::Stop { why, while_saving });
            outputs.directive(StartDirective::CloseConversation)?;
        } else {
            self.finish(FinishedOutcome::Stopped { why, while_saving }, outputs);
        }
        Ok(())
    }

    pub(super) fn save_cleanup(
        &mut self,
        outcome: CleanupOutcome,
        end: EndLeg,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        self.facts.end = end;
        self.save(
            StartSnapshot::CleaningUp {
                start: self.snapshot_facts(),
                outcome,
            },
            outputs,
        )
    }

    fn run_cleanup(&mut self, outcome: CleanupOutcome, outputs: &mut Outputs<'_>) -> Reduced {
        if self.facts.end != EndLeg::Due {
            return self.wind_down(WindDown::Cleanup(outcome), outputs);
        }
        let reservation = self.facts.reservation.as_ref().ok_or(NOT_EXPECTED)?;
        self.stage = Stage::Ending(outcome);
        outputs.directive(StartDirective::SendEnd(EndFleetSession {
            target: EndSessionTarget::CentralId {
                session_id: reservation.session_id.clone(),
            },
            cause: RouterEndCause::GivenUp {},
        }))
    }

    pub(super) fn wind_down(
        &mut self,
        destination: WindDown,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        if matches!(self.facts.conversation, ConversationFact::Held(_)) {
            self.stage = Stage::Closing(AfterClose::WindDown(destination));
            outputs.directive(StartDirective::CloseConversation)
        } else {
            self.save_outcome(destination, outputs)
        }
    }

    fn save_outcome(&mut self, destination: WindDown, outputs: &mut Outputs<'_>) -> Reduced {
        let start = self.snapshot_facts();
        let snapshot = if self.facts.settlement.is_some() {
            StartSnapshot::Settled(start)
        } else {
            match destination {
                WindDown::Cleanup(CleanupOutcome::Failed(reason)) => {
                    StartSnapshot::Failed { start, reason }
                }
                WindDown::Cleanup(CleanupOutcome::Settled) => StartSnapshot::Settled(start),
                WindDown::Fenced => StartSnapshot::Fenced(start),
                WindDown::Unknown(reason) => StartSnapshot::Unknown { start, reason },
            }
        };
        self.save(snapshot, outputs)
    }

    pub(super) fn finish(&mut self, outcome: FinishedOutcome, outputs: &mut Outputs<'_>) {
        self.stage = Stage::Finished(outcome);
        self.tell_terminal(outputs);
    }

    pub(super) fn close_result(
        &mut self,
        result: CloseResult,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::Closing(after) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        self.facts.conversation = match result {
            CloseResult::Closed => ConversationFact::Closed,
            CloseResult::CloseFailed => ConversationFact::CloseFailed,
            CloseResult::OutcomeNotKnown => ConversationFact::NotKnown,
        };
        match after {
            AfterClose::WindDown(destination) => self.save_outcome(destination.clone(), outputs)?,
            AfterClose::Stop { why, while_saving } => {
                self.finish(
                    FinishedOutcome::Stopped {
                        why: *why,
                        while_saving: *while_saving,
                    },
                    outputs,
                );
            }
        }
        Ok(())
    }
}
