use fleet_identity_protocol::CreateSessionAttempt;
use workspace_ids::{CentralSessionId, SessionId, SessionRef};

use crate::{
    ConversationFact, CorruptStartRecord, EndLeg, EndNotDue, Exposure, FailureReason,
    IntendedOutcome, PointerFact, Reservation, SecretFact, SecretIssue, StartEnvelope, StartFacts,
    StartScope, StoredFacts, UnknownReason,
};

#[cfg(test)]
#[path = "snapshot_test.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotKind {
    CreateIntended,
    CreateUnknown,
    Reserved,
    LaunchIntended,
    Held,
    BindIntended,
    BindUnknown,
    CleaningUp,
    Opened,
    Failed,
    Settled,
    Fenced,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotFacts {
    pub envelope: StartEnvelope,
    pub attempt: CreateSessionAttempt,
    pub facts: StartFacts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedSession {
    pub address: SessionRef,
    pub session_id: CentralSessionId,
    pub harness_session_id: SessionId,
    pub issue: SecretIssue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupOutcome {
    Failed(FailureReason),
    Settled,
}

/// The phase of a start record. Reservation and launch metadata live in the facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotPhase {
    CreateIntended,
    CreateUnknown,
    Reserved,
    LaunchIntended,
    Held,
    BindIntended,
    BindUnknown,
    CleaningUp(CleanupOutcome),
    Opened(OpenedSession),
    Failed(FailureReason),
    Settled,
    Fenced,
    Unknown(UnknownReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSnapshot {
    pub envelope: StartEnvelope,
    pub attempt: CreateSessionAttempt,
    pub facts: StoredFacts,
    pub phase: SnapshotPhase,
}

/// Checked durable start evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartSnapshot {
    start: Box<SnapshotFacts>,
    phase: SnapshotPhase,
}

impl StartSnapshot {
    pub fn from_stored(
        scope: &StartScope,
        stored: StoredSnapshot,
    ) -> Result<Self, CorruptStartRecord> {
        if stored.envelope.scope() != scope {
            return Err(CorruptStartRecord::ScopeMismatch);
        }
        Self::checked(
            Box::new(SnapshotFacts {
                envelope: stored.envelope,
                attempt: stored.attempt,
                facts: StartFacts::from_stored(stored.facts)?,
            }),
            stored.phase,
        )
    }

    pub fn to_stored(&self) -> StoredSnapshot {
        StoredSnapshot {
            envelope: self.start.envelope.clone(),
            attempt: self.start.attempt,
            facts: self.start.facts.as_stored().clone(),
            phase: self.phase.clone(),
        }
    }

    pub(crate) fn checked(
        start: Box<SnapshotFacts>,
        phase: SnapshotPhase,
    ) -> Result<Self, CorruptStartRecord> {
        let snapshot = Self { start, phase };
        snapshot.validate(snapshot.start.envelope.scope())?;
        Ok(snapshot)
    }

    pub(crate) fn validate(&self, scope: &StartScope) -> Result<(), CorruptStartRecord> {
        let start = self.start();
        if start.envelope.scope() != scope {
            return Err(CorruptStartRecord::ScopeMismatch);
        }
        let facts = &start.facts;
        facts.validate_issues()?;
        for issue in facts
            .secret_issue()
            .into_iter()
            .chain(facts.pointer_issue())
        {
            if issue.enrollment != scope.enrollment {
                return Err(CorruptStartRecord::IssueEnrollmentMismatch);
            }
            if issue.attempt != start.attempt {
                return Err(CorruptStartRecord::IssueAttemptMismatch);
            }
        }
        if facts
            .reservation
            .as_ref()
            .is_some_and(|reservation| reservation.attempt != start.attempt)
        {
            return Err(CorruptStartRecord::ReservationAttemptMismatch);
        }
        match &self.phase {
            SnapshotPhase::Opened(association) => {
                validate_opened(&start.envelope, facts, association)
            }
            SnapshotPhase::Reserved => {
                if facts.reservation.is_none() {
                    return Err(CorruptStartRecord::ReservedWithoutReservation);
                }
                if !matches!(facts.secret, SecretFact::PossiblyStored(_)) {
                    return Err(CorruptStartRecord::ReservedWithoutSecretIntent);
                }
                Ok(())
            }
            SnapshotPhase::CreateIntended
            | SnapshotPhase::CreateUnknown
            | SnapshotPhase::LaunchIntended
            | SnapshotPhase::Held
            | SnapshotPhase::BindIntended
            | SnapshotPhase::BindUnknown
            | SnapshotPhase::CleaningUp(_)
            | SnapshotPhase::Failed(_)
            | SnapshotPhase::Settled
            | SnapshotPhase::Fenced
            | SnapshotPhase::Unknown(_) => Ok(()),
        }
    }

    pub const fn kind(&self) -> SnapshotKind {
        match &self.phase {
            SnapshotPhase::CreateIntended => SnapshotKind::CreateIntended,
            SnapshotPhase::CreateUnknown => SnapshotKind::CreateUnknown,
            SnapshotPhase::Reserved => SnapshotKind::Reserved,
            SnapshotPhase::LaunchIntended => SnapshotKind::LaunchIntended,
            SnapshotPhase::Held => SnapshotKind::Held,
            SnapshotPhase::BindIntended => SnapshotKind::BindIntended,
            SnapshotPhase::BindUnknown => SnapshotKind::BindUnknown,
            SnapshotPhase::CleaningUp(_) => SnapshotKind::CleaningUp,
            SnapshotPhase::Opened(_) => SnapshotKind::Opened,
            SnapshotPhase::Failed(_) => SnapshotKind::Failed,
            SnapshotPhase::Settled => SnapshotKind::Settled,
            SnapshotPhase::Fenced => SnapshotKind::Fenced,
            SnapshotPhase::Unknown(_) => SnapshotKind::Unknown,
        }
    }

    pub fn start(&self) -> &SnapshotFacts {
        &self.start
    }

    pub const fn phase(&self) -> &SnapshotPhase {
        &self.phase
    }

    pub fn reserved(&self) -> Option<(&Reservation, &SecretIssue)> {
        if self.kind() != SnapshotKind::Reserved {
            return None;
        }
        let facts = &self.start().facts;
        let SecretFact::PossiblyStored(issue) = &facts.secret else {
            return None;
        };
        Some((facts.reservation.as_ref()?, issue))
    }

    pub(crate) fn intended_outcome(&self) -> Option<IntendedOutcome> {
        match self.phase() {
            SnapshotPhase::Failed(reason) => Some(IntendedOutcome::Failed(reason.clone())),
            SnapshotPhase::Settled | SnapshotPhase::CleaningUp(CleanupOutcome::Settled) => {
                Some(IntendedOutcome::Settled)
            }
            SnapshotPhase::Fenced => Some(IntendedOutcome::Fenced),
            SnapshotPhase::Unknown(reason) => Some(IntendedOutcome::Unknown(*reason)),
            SnapshotPhase::CleaningUp(CleanupOutcome::Failed(reason)) => {
                Some(IntendedOutcome::CleaningUp(reason.clone()))
            }
            SnapshotPhase::CreateIntended
            | SnapshotPhase::CreateUnknown
            | SnapshotPhase::Reserved
            | SnapshotPhase::LaunchIntended
            | SnapshotPhase::Held
            | SnapshotPhase::BindIntended
            | SnapshotPhase::BindUnknown
            | SnapshotPhase::Opened(_) => None,
        }
    }

    #[must_use]
    pub fn must_be_retained(&self) -> bool {
        match self.phase() {
            SnapshotPhase::CreateIntended
            | SnapshotPhase::CreateUnknown
            | SnapshotPhase::Reserved
            | SnapshotPhase::LaunchIntended
            | SnapshotPhase::Held
            | SnapshotPhase::BindIntended
            | SnapshotPhase::BindUnknown
            | SnapshotPhase::CleaningUp(_) => true,
            SnapshotPhase::Opened(_)
            | SnapshotPhase::Failed(_)
            | SnapshotPhase::Settled
            | SnapshotPhase::Fenced
            | SnapshotPhase::Unknown(_) => {
                let facts = &self.start().facts;
                let central_clear = facts.create_exposure == Exposure::None
                    || facts.settlement.is_some()
                    || matches!(
                        facts.end,
                        EndLeg::NoSuchStart | EndLeg::NotDue(EndNotDue::NoSuchStart)
                    );
                let conversation_clear = matches!(
                    facts.conversation,
                    ConversationFact::None | ConversationFact::Closed
                );
                let secret_clear =
                    facts.secret == SecretFact::None && facts.pointer == PointerFact::None;
                !(central_clear && conversation_clear && secret_clear)
            }
        }
    }
}

fn validate_opened(
    envelope: &StartEnvelope,
    facts: &StartFacts,
    association: &OpenedSession,
) -> Result<(), CorruptStartRecord> {
    if facts.conversation != ConversationFact::Held {
        return Err(CorruptStartRecord::OpenedWithoutHeldConversation);
    }
    let PointerFact::Committed(issue) = &facts.pointer else {
        return Err(CorruptStartRecord::OpenedWithoutCommittedPointer);
    };
    if association.address.machine_id != *envelope.machine() {
        return Err(CorruptStartRecord::AssociationMachineMismatch);
    }
    if facts.harness_session_id.as_ref() != Some(&association.harness_session_id)
        || association.address.session_id != association.harness_session_id
    {
        return Err(CorruptStartRecord::AssociationHarnessMismatch);
    }
    if association.session_id != issue.session_id {
        return Err(CorruptStartRecord::AssociationSessionMismatch);
    }
    if &association.issue != issue {
        return Err(CorruptStartRecord::AssociationIssueMismatch);
    }
    Ok(())
}
