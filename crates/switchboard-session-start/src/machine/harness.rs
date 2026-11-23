use super::{AcceptedStart, NOT_EXPECTED, Outputs, RecordedStop, Reduced, Stage, WindDown};
use crate::{
    ConversationFact, EndLeg, EndNotDue, EndNotSent, EnrollmentEnded, FailureReason,
    FinishedOutcome, HoldLostDuring, LaunchFact, LaunchNotTaken, LaunchResult, OpeningNotCommitted,
    OpeningOutcome, PointerFact, PutSecretResult, Recording, SecretFact, SetUpNotTaken,
    SetUpResult, SnapshotKind, StartSnapshot, UnknownReason,
};

impl AcceptedStart {
    pub(super) fn secret_result(
        &mut self,
        result: PutSecretResult,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::StoringSecret(issue) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            PutSecretResult::Stored => {
                self.facts.secret = SecretFact::Stored(issue.clone());
                let mut launch_intent = self.snapshot_facts();
                launch_intent.facts.pointer = PointerFact::Committed(issue.clone());
                launch_intent.facts.launch = LaunchFact::Sent;
                self.save(StartSnapshot::LaunchIntended(launch_intent), outputs)
            }
            PutSecretResult::NotTaken(EnrollmentEnded) => self.wind_down(WindDown::Fenced, outputs),
            PutSecretResult::NotStored => {
                self.cleanup_failure(FailureReason::SecretNotStored, EndLeg::Due, outputs)
            }
            PutSecretResult::OutcomeNotKnown => {
                self.facts.secret = SecretFact::PossiblyStored(issue.clone());
                self.cleanup_failure(FailureReason::SecretStoreNotKnown, EndLeg::Due, outputs)
            }
        }
    }

    pub(super) fn launch_result(
        &mut self,
        result: LaunchResult,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::Launching = self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            LaunchResult::Started { harness_session_id } => {
                self.facts.conversation = ConversationFact::Held(Some(harness_session_id));
                self.save(StartSnapshot::Held(self.snapshot_facts()), outputs)
            }
            LaunchResult::NotTaken(why) => {
                self.facts.launch = LaunchFact::NotSent;
                match why {
                    LaunchNotTaken::ReservationElapsed => {
                        self.facts.end = EndLeg::NotDue(EndNotDue::RanOutAtCentral);
                        self.fail(FailureReason::ReservationRanOut, outputs)
                    }
                    LaunchNotTaken::LinkNotReady => {
                        self.facts.end = EndLeg::NotSent(EndNotSent::LinkNotReady);
                        self.fail(FailureReason::NotLaunched, outputs)
                    }
                    LaunchNotTaken::EnrollmentEnded => self.wind_down(WindDown::Fenced, outputs),
                }
            }
            LaunchResult::FailedNoConversation => {
                self.cleanup_failure(FailureReason::LaunchFailed, EndLeg::Due, outputs)
            }
            LaunchResult::FailedDuplicateId => {
                self.facts.conversation = ConversationFact::Held(None);
                self.cleanup_failure(FailureReason::DuplicateHarnessId, EndLeg::Due, outputs)
            }
            LaunchResult::OutcomeNotKnown => {
                self.facts.conversation = ConversationFact::NotKnown;
                self.wind_down(WindDown::Unknown(UnknownReason::Launch), outputs)
            }
        }
    }

    pub(super) fn set_up_result(
        &mut self,
        result: SetUpResult,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::SettingUp = self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            SetUpResult::Done => {
                self.save(StartSnapshot::BindIntended(self.snapshot_facts()), outputs)
            }
            SetUpResult::Failed => {
                self.cleanup_failure(FailureReason::SetUpFailed, EndLeg::Due, outputs)
            }
            SetUpResult::NotTaken(SetUpNotTaken::HoldLost) => {
                self.facts.conversation = ConversationFact::NotKnown;
                self.cleanup_failure(
                    FailureReason::HoldLost {
                        during: HoldLostDuring::SetUp,
                    },
                    EndLeg::Due,
                    outputs,
                )
            }
            SetUpResult::OutcomeNotKnown => {
                self.wind_down(WindDown::Unknown(UnknownReason::SetUp), outputs)
            }
            SetUpResult::NotTaken(SetUpNotTaken::EnrollmentEnded) => {
                self.wind_down(WindDown::Fenced, outputs)
            }
        }
    }

    pub(super) fn opening_result(
        &mut self,
        result: OpeningOutcome,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::Opening(session) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            OpeningOutcome::Committed(revision) => {
                self.revision = Some(revision);
                self.stage = Stage::Opened(session.clone());
                self.tell_terminal(outputs);
                Ok(())
            }
            OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded) => {
                self.wind_down(WindDown::Fenced, outputs)
            }
            OpeningOutcome::NotCommitted(OpeningNotCommitted::PreparationFailed) => {
                self.cleanup_failure(FailureReason::OpeningNotPrepared, EndLeg::Due, outputs)
            }
            OpeningOutcome::NotCommitted(OpeningNotCommitted::RevisionMismatch) => {
                self.record_lost(RecordedStop::Conflict, SnapshotKind::Opened, outputs)
            }
            OpeningOutcome::NotCommitted(OpeningNotCommitted::RecordUnavailable) => {
                self.record_lost(RecordedStop::NotCommitted, SnapshotKind::Opened, outputs)
            }
            OpeningOutcome::Uncertain => {
                self.recording = Recording::NotKnown;
                self.finish(FinishedOutcome::Unknown(UnknownReason::Opening), outputs);
                Ok(())
            }
        }
    }
}
