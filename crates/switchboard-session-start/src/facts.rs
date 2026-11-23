use fleet_identity_protocol::{
    CreateSessionAttempt, EnrollmentId, NewEndSettlement, SessionSettlement, SessionTime,
};
use workspace_ids::{CentralSessionId, SessionId};

use crate::{CorruptStartRecord, LaunchNotTaken};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretIssue {
    pub enrollment: EnrollmentId,
    pub session_id: CentralSessionId,
    pub attempt: CreateSessionAttempt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Exposure {
    None,
    MayHaveReached,
    Witnessed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reservation {
    pub session_id: CentralSessionId,
    pub reserved_until: SessionTime,
    pub attempt: CreateSessionAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretFact {
    None,
    Stored(SecretIssue),
    PossiblyStored(SecretIssue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PointerFact {
    None,
    Committed(SecretIssue),
    PossiblyCommitted(SecretIssue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchFact {
    NotIntended,
    Intended,
    ProvenNotTaken(LaunchNotTaken),
    FailedNoConversation,
    Started,
    NotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConversationFact {
    None,
    Held,
    Closed,
    CloseFailed,
    NotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndNotDue {
    NothingToEnd,
    RanOutAtCentral,
    BindMayHaveReached,
    AssociationDisputed,
    NoSuchStart,
    Settled,
    CentralFault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndNotSent {
    LinkNotReady,
    EnrollmentEnded,
    RecordUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndLeg {
    NotDue(EndNotDue),
    Due,
    Settled(NewEndSettlement),
    AlreadySettled(SessionSettlement),
    NoSuchStart,
    NotSent(EndNotSent),
    NotApplied,
    NotKnown,
    AuthorityRefused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Recording {
    Recorded,
    NotRecorded,
    NotKnown,
    Conflict,
}

/// Parts an adapter reads before validating a start record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredFacts {
    pub create_exposure: Exposure,
    pub reservation: Option<Reservation>,
    pub secret: SecretFact,
    pub pointer: PointerFact,
    pub launch: LaunchFact,
    pub harness_session_id: Option<SessionId>,
    pub conversation: ConversationFact,
    pub bind_exposure: Exposure,
    pub settlement: Option<SessionSettlement>,
    pub end: EndLeg,
}

impl Default for StoredFacts {
    fn default() -> Self {
        Self {
            create_exposure: Exposure::None,
            reservation: None,
            secret: SecretFact::None,
            pointer: PointerFact::None,
            launch: LaunchFact::NotIntended,
            harness_session_id: None,
            conversation: ConversationFact::None,
            bind_exposure: Exposure::None,
            settlement: None,
            end: EndLeg::NotDue(EndNotDue::NothingToEnd),
        }
    }
}

/// Validated start evidence. Only the reducer can update it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartFacts {
    pub(crate) stored: StoredFacts,
}

impl StartFacts {
    pub fn from_stored(stored: StoredFacts) -> Result<Self, CorruptStartRecord> {
        let facts = Self { stored };
        facts.validate_issues()?;
        Ok(facts)
    }

    pub const fn as_stored(&self) -> &StoredFacts {
        &self.stored
    }

    pub(crate) fn initial() -> Self {
        Self {
            stored: StoredFacts::default(),
        }
    }

    pub(crate) fn secret_issue(&self) -> Option<&SecretIssue> {
        match &self.stored.secret {
            SecretFact::None => None,
            SecretFact::Stored(issue) | SecretFact::PossiblyStored(issue) => Some(issue),
        }
    }

    pub(crate) fn pointer_issue(&self) -> Option<&SecretIssue> {
        match &self.stored.pointer {
            PointerFact::None => None,
            PointerFact::Committed(issue) | PointerFact::PossiblyCommitted(issue) => Some(issue),
        }
    }

    pub(crate) fn validate_issues(&self) -> Result<(), CorruptStartRecord> {
        let secret = self.secret_issue();
        let pointer = self.pointer_issue();
        let Some(reservation) = &self.stored.reservation else {
            if pointer.is_some() {
                return Err(CorruptStartRecord::PointerWithoutReservation);
            }
            if secret.is_some() {
                return Err(CorruptStartRecord::SecretWithoutReservation);
            }
            return Ok(());
        };
        for issue in secret.into_iter().chain(pointer) {
            if issue.session_id != reservation.session_id {
                return Err(CorruptStartRecord::IssueSessionMismatch);
            }
            if issue.attempt != reservation.attempt {
                return Err(CorruptStartRecord::IssueAttemptMismatch);
            }
        }
        Ok(())
    }
}

impl std::ops::Deref for StartFacts {
    type Target = StoredFacts;

    fn deref(&self) -> &Self::Target {
        self.as_stored()
    }
}
