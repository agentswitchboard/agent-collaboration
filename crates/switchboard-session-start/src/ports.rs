use fleet_identity_protocol::SessionSecret;
use workspace_ids::SessionRef;

use crate::{
    ConversationFact, CorruptStartRecord, OpenedSession, PointerFact, SecretIssue, SnapshotFacts,
    SnapshotPhase, StartEnvelope, StartFacts, StartScope, StartSnapshot,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartRevision(u64);

impl StartRevision {
    pub const fn new(revision: u64) -> Self {
        Self(revision)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpeningCommit {
    association: OpenedSession,
    opened: StartSnapshot,
}

impl OpeningCommit {
    pub fn new(
        envelope: &StartEnvelope,
        facts: &StartFacts,
        address: SessionRef,
    ) -> Result<Self, CorruptStartRecord> {
        if facts.conversation != ConversationFact::Held {
            return Err(CorruptStartRecord::OpenedWithoutHeldConversation);
        }
        let PointerFact::Committed(issue) = &facts.pointer else {
            return Err(CorruptStartRecord::OpenedWithoutCommittedPointer);
        };
        let reservation = facts
            .reservation
            .as_ref()
            .ok_or(CorruptStartRecord::PointerWithoutReservation)?;
        let harness_session_id = facts
            .harness_session_id
            .as_ref()
            .ok_or(CorruptStartRecord::AssociationHarnessMismatch)?;
        let association = OpenedSession {
            address,
            session_id: reservation.session_id.clone(),
            harness_session_id: harness_session_id.clone(),
            issue: issue.clone(),
        };
        let opened = StartSnapshot::checked(
            Box::new(SnapshotFacts {
                envelope: envelope.clone(),
                attempt: reservation.attempt,
                facts: facts.clone(),
            }),
            SnapshotPhase::Opened(association.clone()),
        )?;
        Ok(Self {
            association,
            opened,
        })
    }

    pub const fn association(&self) -> &OpenedSession {
        &self.association
    }

    pub const fn opened(&self) -> &StartSnapshot {
        &self.opened
    }

    pub const fn issue(&self) -> &SecretIssue {
        &self.association.issue
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpeningNotCommitted {
    EnrollmentEnded,
    RevisionMismatch,
    RecordUnavailable,
    PreparationFailed,
    ConnectionRetired,
    HoldLost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpeningOutcome {
    Committed(StartRevision),
    NotCommitted(OpeningNotCommitted),
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartRecordError {
    #[error("the start record is unavailable")]
    Unavailable,
    #[error("the start record is corrupt")]
    CorruptRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartSaveRefusal {
    #[error("the start snapshot was not committed")]
    NotCommitted,
    #[error("the start record revision differs")]
    RevisionMismatch,
    #[error("the start snapshot write outcome is unknown")]
    OutcomeNotKnown,
}

/// The durable record for one start.
pub trait StartRecord {
    fn load(
        &self,
        scope: &StartScope,
    ) -> Result<Option<(StartRevision, StartSnapshot)>, StartRecordError>;

    /// Writes the snapshot only if the stored revision is `expected`.
    /// An absent revision requires that there be no record.
    fn save(
        &self,
        scope: &StartScope,
        expected: Option<StartRevision>,
        snapshot: &StartSnapshot,
    ) -> Result<StartRevision, StartSaveRefusal>;

    /// Decides the association, start result and visibility in one write.
    /// The opening authority checks the enrollment, revision, the connection
    /// that delivered Active, and custody of the original conversation handle
    /// at the one write. Lost custody takes priority over a retired connection.
    /// The route, lane and first events are prepared privately before this
    /// write. Readers use the published visibility decision.
    fn commit_opening(
        &self,
        scope: &StartScope,
        expected: StartRevision,
        opening: &OpeningCommit,
    ) -> OpeningOutcome;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SecretPutRefusal {
    #[error("the issue already holds a different secret")]
    IssueAlreadyHeld,
    #[error("the secret was not stored")]
    NotStored,
    #[error("the secret write outcome is unknown")]
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SecretReadError {
    #[error("there is no secret for the issue")]
    NotFound,
    #[error("the secret store is locked")]
    Locked,
    #[error("the secret store is unavailable")]
    Unavailable,
}

/// Holds secret values outside the machine and its record.
pub trait SessionSecretStore {
    /// Stores the secret once for the issue. Repeating the same value succeeds.
    fn put(&self, issue: &SecretIssue, secret: SessionSecret) -> Result<(), SecretPutRefusal>;

    fn read(&self, issue: &SecretIssue) -> Result<SessionSecret, SecretReadError>;
}
