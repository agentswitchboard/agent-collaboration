use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use central_engine_model::{EngineTime, EventTag, RefusalTag, StateTag};
use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionAttempt,
    CreateSessionDigest, CreateSessionKind, CreateSessionRefusal, EndFleetSessionReply,
    EndedSessionCause, EnrollmentId, HarnessKind, ProjectRef, RouterEndCause, SessionSecret,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use subtle::ConstantTimeEq;
use workspace_ids::{CentralSessionId, MachineId, SessionId, SessionOperationId};
use zeroize::Zeroizing;

#[derive(Clone, Eq, PartialEq)]
pub struct SessionSecretHash(pub(crate) [u8; 32]);

impl SessionSecretHash {
    pub(crate) fn from_secret(secret: &SessionSecret) -> Result<Self, SessionMachineRefusal> {
        let mut bytes = Zeroizing::new([0; 32]);
        URL_SAFE_NO_PAD
            .decode_slice(secret.expose_secret(), bytes.as_mut())
            .ok()
            .filter(|written| *written == bytes.len())
            .ok_or(SessionMachineRefusal::InvalidCandidateSecret)?;
        Ok(Self(Sha256::digest(bytes.as_ref()).into()))
    }

    pub(crate) fn matches(&self, secret: &SessionSecret) -> bool {
        Self::from_secret(secret).is_ok_and(|candidate| bool::from(self.0.ct_eq(&candidate.0)))
    }
}

impl fmt::Debug for SessionSecretHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionSecretHash([REDACTED])")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordedSessionCreator {
    Operator,
    Session(CentralSessionId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionReservation {
    pub session_id: CentralSessionId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
    pub operation_id: SessionOperationId,
    pub contents_digest: CreateSessionDigest,
    pub creator: RecordedSessionCreator,
    pub agent_program: HarnessKind,
    pub kind: CreateSessionKind,
    pub reserved_until: EngineTime,
    pub project: ProjectRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservedSession {
    pub reservation: SessionReservation,
    pub attempt: CreateSessionAttempt,
    pub secret_hash: SessionSecretHash,
}

impl ReservedSession {
    pub(crate) fn allows_rotation(
        &self,
        digest: &CreateSessionDigest,
        attempt: CreateSessionAttempt,
    ) -> bool {
        self.reservation.contents_digest == *digest && attempt > self.attempt
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundSession {
    pub reserved: ReservedSession,
    pub harness_session_id: SessionId,
    pub bound_at: EngineTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionState {
    Missing,
    Reserved(ReservedSession),
    Active(BoundSession),
    Abandoned {
        reserved: ReservedSession,
        cause: AbandonedSessionCause,
        at: EngineTime,
    },
    Ended {
        bound: BoundSession,
        cause: EndedSessionCause,
        at: EngineTime,
    },
}

impl SessionState {
    pub fn read_at(&self, now: EngineTime, enrollment_ended_at: Option<EngineTime>) -> Self {
        match self {
            Self::Reserved(reserved) => {
                let deadline = reserved.reservation.reserved_until;
                if let Some(ended_at) = enrollment_ended_at {
                    let (cause, at) = if ended_at < deadline {
                        (AbandonedSessionCause::MachineRevoked {}, ended_at)
                    } else {
                        (AbandonedSessionCause::TimeRanOut {}, deadline)
                    };
                    Self::Abandoned {
                        reserved: reserved.clone(),
                        cause,
                        at,
                    }
                } else if now >= deadline {
                    Self::Abandoned {
                        reserved: reserved.clone(),
                        cause: AbandonedSessionCause::TimeRanOut {},
                        at: deadline,
                    }
                } else {
                    self.clone()
                }
            }
            Self::Active(bound) => {
                if let Some(at) = enrollment_ended_at {
                    Self::Ended {
                        bound: bound.clone(),
                        cause: EndedSessionCause::MachineRevoked {},
                        at,
                    }
                } else {
                    self.clone()
                }
            }
            Self::Missing | Self::Abandoned { .. } | Self::Ended { .. } => self.clone(),
        }
    }

    pub fn reservation(&self) -> Option<&SessionReservation> {
        match self {
            Self::Missing => None,
            Self::Reserved(reserved) | Self::Abandoned { reserved, .. } => {
                Some(&reserved.reservation)
            }
            Self::Active(bound) | Self::Ended { bound, .. } => Some(&bound.reserved.reservation),
        }
    }
}

impl StateTag for SessionState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Reserved(_) => "reserved",
            Self::Active(_) => "active",
            Self::Abandoned { .. } => "abandoned",
            Self::Ended { .. } => "ended",
        }
    }
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Abandoned { .. } | Self::Ended { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewSessionAdmission {
    Permitted(Box<SessionReservation>),
    Refused(CreateSessionRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionCreatorAdmission {
    Admitted,
    NoSecret,
    NoSuchSession,
    SessionEnded,
    WrongSecret,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AddressAvailability {
    Free,
    Taken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionEndIdentity {
    CentralId,
    OperationDigest(CreateSessionDigest),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionEvent {
    Create {
        caller: SessionCreatorAdmission,
        first: Option<NewSessionAdmission>,
        contents_digest: CreateSessionDigest,
        attempt: CreateSessionAttempt,
        /// Supplied by the service, which never persists the plaintext.
        candidate_secret: Option<SessionSecret>,
    },
    Bind {
        harness_session_id: SessionId,
        secret: SessionSecret,
        address: AddressAvailability,
    },
    End {
        by: SessionEndIdentity,
        cause: RouterEndCause,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionApply {
    pub event: SessionEvent,
    pub enrollment_ended_at: Option<EngineTime>,
}

impl EventTag for SessionApply {
    fn event_tag(&self) -> &'static str {
        match self.event {
            SessionEvent::Create { .. } => "create",
            SessionEvent::Bind { .. } => "bind",
            SessionEvent::End { .. } => "end",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionReceipt {
    Create(CreateFleetSessionReply),
    Bind(BindFleetSessionReply),
    End(EndFleetSessionReply),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SessionMachineRefusal {
    TimeOutOfRange,
    ContradictoryReservation,
    MissingCreateAdmission,
    MissingCandidateSecret,
    InvalidCandidateSecret,
}

impl RefusalTag for SessionMachineRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::TimeOutOfRange => "time_out_of_range",
            Self::ContradictoryReservation => "contradictory_reservation",
            Self::MissingCreateAdmission => "missing_create_admission",
            Self::MissingCandidateSecret => "missing_candidate_secret",
            Self::InvalidCandidateSecret => "invalid_candidate_secret",
        }
    }
}
