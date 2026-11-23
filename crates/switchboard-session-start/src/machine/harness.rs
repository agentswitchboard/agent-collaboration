use super::{AcceptedStart, NOT_EXPECTED, Outputs, RecordedStop, Reduced, Stage, WindDown};
use crate::{
    ConversationFact, EndLeg, EndNotDue, EndNotSent, FailureReason, FinishedOutcome,
    HoldLostDuring, LaunchFact, LaunchNotTaken, LaunchResult, OpeningNotCommitted, OpeningOutcome,
    PointerFact, PutSecretNotTaken, PutSecretResult, Recording, SecretFact, SetUpNotTaken,
    SetUpResult, SnapshotKind, SnapshotPhase, StartFault, UnknownReason,
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
                self.facts.stored.secret = SecretFact::Stored(issue.clone());
                let mut launch_intent = self.snapshot_facts();
                launch_intent.facts.stored.pointer = PointerFact::Committed(issue.clone());
                launch_intent.facts.stored.launch = LaunchFact::Intended;
                self.save_with_facts(SnapshotPhase::LaunchIntended, launch_intent, outputs)
            }
            PutSecretResult::NotTaken(PutSecretNotTaken::EnrollmentEnded) => {
                self.facts.stored.secret = SecretFact::None;
                self.wind_down(WindDown::Fenced, outputs)
            }
            PutSecretResult::NotStored
            | PutSecretResult::NotTaken(PutSecretNotTaken::PendingSecretMissing) => {
                self.facts.stored.secret = SecretFact::None;
                self.cleanup_failure(FailureReason::SecretNotStored, EndLeg::Due, outputs)
            }
            PutSecretResult::OutcomeNotKnown => {
                self.cleanup_failure(FailureReason::SecretStoreNotKnown, EndLeg::Due, outputs)
            }
            PutSecretResult::HeldWithDifferentValue => {
                Self::report(
                    StartFault::SecretIssueCollision(self.fault_context()),
                    outputs,
                );
                self.cleanup_failure(FailureReason::SecretIssueCollision, EndLeg::Due, outputs)
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
                self.facts.stored.launch = LaunchFact::Started;
                self.facts.stored.harness_session_id = Some(harness_session_id);
                self.facts.stored.conversation = ConversationFact::Held;
                self.save(SnapshotPhase::Held, outputs)
            }
            LaunchResult::NotTaken(why) => {
                self.facts.stored.launch = LaunchFact::ProvenNotTaken(why);
                match why {
                    LaunchNotTaken::ReservationElapsed => {
                        self.facts.stored.end = EndLeg::NotDue(EndNotDue::RanOutAtCentral);
                        self.fail(FailureReason::ReservationRanOut, outputs)
                    }
                    LaunchNotTaken::LinkNotReady => {
                        self.facts.stored.end = EndLeg::NotSent(EndNotSent::LinkNotReady);
                        self.fail(FailureReason::NotLaunched, outputs)
                    }
                    LaunchNotTaken::EnrollmentEnded => self.wind_down(WindDown::Fenced, outputs),
                }
            }
            LaunchResult::FailedNoConversation => {
                self.facts.stored.launch = LaunchFact::FailedNoConversation;
                self.cleanup_failure(FailureReason::LaunchFailed, EndLeg::Due, outputs)
            }
            LaunchResult::FailedDuplicateId => {
                self.facts.stored.launch = LaunchFact::Started;
                self.facts.stored.conversation = ConversationFact::Held;
                self.cleanup_failure(FailureReason::DuplicateHarnessId, EndLeg::Due, outputs)
            }
            LaunchResult::OutcomeNotKnown => {
                self.facts.stored.launch = LaunchFact::NotKnown;
                self.facts.stored.conversation = ConversationFact::NotKnown;
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
            SetUpResult::Done => self.save(SnapshotPhase::BindIntended, outputs),
            SetUpResult::Failed => {
                self.cleanup_failure(FailureReason::SetUpFailed, EndLeg::Due, outputs)
            }
            SetUpResult::NotTaken(SetUpNotTaken::HoldLost) => {
                self.facts.stored.conversation = ConversationFact::NotKnown;
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
            OpeningOutcome::NotCommitted(OpeningNotCommitted::ConnectionRetired) => {
                self.save(SnapshotPhase::BindUnknown, outputs)
            }
            OpeningOutcome::NotCommitted(OpeningNotCommitted::HoldLost) => {
                self.facts.stored.conversation = ConversationFact::NotKnown;
                self.cleanup_failure(
                    FailureReason::HoldLost {
                        during: HoldLostDuring::Opening,
                    },
                    EndLeg::Due,
                    outputs,
                )
            }
            OpeningOutcome::Uncertain => {
                self.recording = Recording::NotKnown;
                self.finish(FinishedOutcome::Unknown(UnknownReason::Opening), outputs);
                Ok(())
            }
        }
    }
}
