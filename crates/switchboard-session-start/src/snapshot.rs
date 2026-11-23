use fleet_identity_protocol::CreateSessionAttempt;
use workspace_ids::{CentralSessionId, SessionId, SessionRef};

use crate::{
    ConversationFact, CorruptStartRecord, EndLeg, EndNotDue, Exposure, FactName, FailureReason,
    IntendedOutcome, LaunchFact, PointerFact, Reservation, SecretFact, SecretIssue, SnapshotKind,
    StartEnvelope, StartFacts, StartScope, StoredFacts, UnknownReason, facts::settlement_fact,
};

#[cfg(test)]
#[path = "snapshot_test.rs"]
mod tests;

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
        stored.validate(scope)?;
        Ok(Self {
            start: Box::new(SnapshotFacts {
                envelope: stored.envelope,
                attempt: stored.attempt,
                facts: StartFacts {
                    stored: stored.facts,
                },
            }),
            phase: stored.phase,
        })
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
        validate_phase(
            scope,
            &self.start.envelope,
            self.start.facts.as_stored(),
            PhaseToCheck::Stored(&self.phase, self.start.attempt),
        )
        .map(|_| ())
    }

    pub(crate) fn for_opening(
        envelope: &StartEnvelope,
        facts: &StartFacts,
        address: &SessionRef,
    ) -> Result<(Self, OpenedSession), CorruptStartRecord> {
        let (association, attempt) = validate_phase(
            envelope.scope(),
            envelope,
            facts.as_stored(),
            PhaseToCheck::Opening(address),
        )?
        .ok_or(CorruptStartRecord::OpenedWithoutActiveProof)?;
        let opened = Self {
            start: Box::new(SnapshotFacts {
                envelope: envelope.clone(),
                attempt,
                facts: facts.clone(),
            }),
            phase: SnapshotPhase::Opened(association.clone()),
        };
        Ok((opened, association))
    }

    pub const fn kind(&self) -> SnapshotKind {
        self.phase.kind()
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
}

impl SnapshotPhase {
    const fn kind(&self) -> SnapshotKind {
        match self {
            Self::CreateIntended => SnapshotKind::CreateIntended,
            Self::CreateUnknown => SnapshotKind::CreateUnknown,
            Self::Reserved => SnapshotKind::Reserved,
            Self::LaunchIntended => SnapshotKind::LaunchIntended,
            Self::Held => SnapshotKind::Held,
            Self::BindIntended => SnapshotKind::BindIntended,
            Self::BindUnknown => SnapshotKind::BindUnknown,
            Self::CleaningUp(_) => SnapshotKind::CleaningUp,
            Self::Opened(_) => SnapshotKind::Opened,
            Self::Failed(_) => SnapshotKind::Failed,
            Self::Settled => SnapshotKind::Settled,
            Self::Fenced => SnapshotKind::Fenced,
            Self::Unknown(_) => SnapshotKind::Unknown,
        }
    }
}

impl StoredSnapshot {
    fn validate(&self, scope: &StartScope) -> Result<(), CorruptStartRecord> {
        validate_phase(
            scope,
            &self.envelope,
            &self.facts,
            PhaseToCheck::Stored(&self.phase, self.attempt),
        )
        .map(|_| ())
    }

    #[must_use]
    pub fn must_be_retained(&self, scope: &StartScope) -> bool {
        if self.validate(scope).is_err() {
            return true;
        }
        match self.phase {
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
                let facts = &self.facts;
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

enum PhaseToCheck<'a> {
    Stored(&'a SnapshotPhase, CreateSessionAttempt),
    Opening(&'a SessionRef),
}

type CheckedOpening = (OpenedSession, CreateSessionAttempt);

/// The record boundary for decoding, restore, opening, and pruning.
fn validate_phase(
    scope: &StartScope,
    envelope: &StartEnvelope,
    facts: &StoredFacts,
    candidate: PhaseToCheck<'_>,
) -> Result<Option<CheckedOpening>, CorruptStartRecord> {
    if envelope.scope() != scope {
        return Err(CorruptStartRecord::ScopeMismatch);
    }
    let attempt = match candidate {
        PhaseToCheck::Stored(phase, attempt) => {
            validate_stored_phase_facts(phase, facts)?;
            Some(attempt)
        }
        PhaseToCheck::Opening(_) => None,
    };
    facts.validate_issues()?;
    for issue in facts
        .secret_issue()
        .into_iter()
        .chain(facts.pointer_issue())
    {
        if issue.enrollment != scope.enrollment {
            return Err(CorruptStartRecord::IssueEnrollmentMismatch);
        }
        if attempt.is_some_and(|attempt| issue.attempt != attempt) {
            return Err(CorruptStartRecord::IssueAttemptMismatch);
        }
    }
    if let (Some(reservation), Some(attempt)) = (&facts.reservation, attempt)
        && reservation.attempt != attempt
    {
        return Err(CorruptStartRecord::ReservationAttemptMismatch);
    }
    if let Some(held) = &facts.settlement {
        let agrees = match &facts.end {
            EndLeg::Settled(new) => settlement_fact(new) == *held,
            EndLeg::AlreadySettled(previous) => previous == held,
            EndLeg::NotDue(_)
            | EndLeg::Due
            | EndLeg::NoSuchStart
            | EndLeg::NotSent(_)
            | EndLeg::NotApplied(_)
            | EndLeg::NotKnown
            | EndLeg::AuthorityRefused => true,
        };
        if !agrees {
            return Err(CorruptStartRecord::EndSettlementDisagrees);
        }
    }
    match candidate {
        PhaseToCheck::Stored(SnapshotPhase::Opened(stored), _) => {
            let (checked, _) = check_opened(envelope, facts, &stored.address)?;
            if stored.harness_session_id != checked.harness_session_id {
                return Err(CorruptStartRecord::AssociationHarnessMismatch);
            }
            if stored.session_id != checked.session_id {
                return Err(CorruptStartRecord::AssociationSessionMismatch);
            }
            if stored.issue != checked.issue {
                return Err(CorruptStartRecord::AssociationIssueMismatch);
            }
            Ok(None)
        }
        PhaseToCheck::Stored(_, _) => Ok(None),
        PhaseToCheck::Opening(address) => check_opened(envelope, facts, address).map(Some),
    }
}

fn validate_stored_phase_facts(
    phase: &SnapshotPhase,
    facts: &StoredFacts,
) -> Result<(), CorruptStartRecord> {
    match phase {
        SnapshotPhase::Reserved => {
            if facts.reservation.is_none() {
                return Err(CorruptStartRecord::ReservedWithoutReservation);
            }
            if !matches!(facts.secret, SecretFact::PossiblyStored(_)) {
                return Err(CorruptStartRecord::ReservedWithoutSecretIntent);
            }
            Ok(())
        }
        SnapshotPhase::LaunchIntended
        | SnapshotPhase::Held
        | SnapshotPhase::BindIntended
        | SnapshotPhase::BindUnknown => require_launch_facts(phase.kind(), facts),
        SnapshotPhase::Settled if facts.settlement.is_none() => {
            Err(CorruptStartRecord::SettledWithoutSettlement)
        }
        SnapshotPhase::CleaningUp(CleanupOutcome::Settled) if facts.settlement.is_none() => {
            Err(CorruptStartRecord::PhaseFactMissing {
                phase: SnapshotKind::CleaningUp,
                fact: FactName::CleanupReason,
            })
        }
        SnapshotPhase::CreateIntended
        | SnapshotPhase::CreateUnknown
        | SnapshotPhase::CleaningUp(_)
        | SnapshotPhase::Opened(_)
        | SnapshotPhase::Failed(_)
        | SnapshotPhase::Settled
        | SnapshotPhase::Fenced
        | SnapshotPhase::Unknown(_) => Ok(()),
    }
}

fn require_launch_facts(
    phase: SnapshotKind,
    facts: &StoredFacts,
) -> Result<(), CorruptStartRecord> {
    let launch_intended = phase == SnapshotKind::LaunchIntended;
    let required = [
        (facts.reservation.is_some(), FactName::Reservation),
        (
            matches!(facts.secret, SecretFact::Stored(_)),
            FactName::StoredSecret,
        ),
        (
            matches!(facts.pointer, PointerFact::Committed(_)),
            FactName::CommittedPointer,
        ),
        (
            !launch_intended || facts.launch == LaunchFact::Intended,
            FactName::LaunchIntended,
        ),
        (
            launch_intended || facts.launch == LaunchFact::Started,
            FactName::LaunchStarted,
        ),
        (
            launch_intended || facts.harness_session_id.is_some(),
            FactName::HarnessSessionId,
        ),
        (
            launch_intended || facts.conversation == ConversationFact::Held,
            FactName::HeldConversation,
        ),
    ];
    if let Some((_, fact)) = required.into_iter().find(|(holds, _)| !holds) {
        return Err(CorruptStartRecord::PhaseFactMissing { phase, fact });
    }
    Ok(())
}

fn check_opened(
    envelope: &StartEnvelope,
    facts: &StoredFacts,
    address: &SessionRef,
) -> Result<CheckedOpening, CorruptStartRecord> {
    let reservation = facts
        .reservation
        .as_ref()
        .ok_or(CorruptStartRecord::PointerWithoutReservation)?;
    if facts.conversation != ConversationFact::Held {
        return Err(CorruptStartRecord::OpenedWithoutHeldConversation);
    }
    let PointerFact::Committed(issue) = &facts.pointer else {
        return Err(CorruptStartRecord::OpenedWithoutCommittedPointer);
    };
    if !matches!(&facts.secret, SecretFact::Stored(stored) if stored == issue) {
        return Err(CorruptStartRecord::OpenedWithoutStoredSecret);
    }
    if facts.launch != LaunchFact::Started {
        return Err(CorruptStartRecord::OpenedWithoutLaunch);
    }
    if facts.bind_exposure != Exposure::Witnessed {
        return Err(CorruptStartRecord::OpenedWithoutWitnessedBind);
    }
    if address.machine_id != *envelope.machine() {
        return Err(CorruptStartRecord::AssociationMachineMismatch);
    }
    let harness_session_id = facts
        .harness_session_id
        .as_ref()
        .ok_or(CorruptStartRecord::AssociationHarnessMismatch)?;
    if &address.session_id != harness_session_id {
        return Err(CorruptStartRecord::AssociationHarnessMismatch);
    }
    let active = facts
        .active_proof
        .as_ref()
        .ok_or(CorruptStartRecord::OpenedWithoutActiveProof)?;
    if &active.address != address {
        return Err(CorruptStartRecord::OpenedActiveProofAddressMismatch);
    }
    if facts.settlement.is_some() {
        return Err(CorruptStartRecord::OpenedWithSettlement);
    }
    if facts.end != EndLeg::NotDue(EndNotDue::NothingToEnd) {
        return Err(CorruptStartRecord::OpenedWithEndLeg);
    }
    Ok((
        OpenedSession {
            address: address.clone(),
            session_id: reservation.session_id.clone(),
            harness_session_id: harness_session_id.clone(),
            issue: issue.clone(),
        },
        reservation.attempt,
    ))
}
