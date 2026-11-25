//! Observations and effect requests at the pure start machine's boundary.

use fleet_identity_protocol::{CreateSessionAttempt, SessionSettlement};
use state_machine_core::{EventTag, RefusalTag, TransitionError};
use workspace_ids::{CentralSessionId, SessionId, SessionRef};

use crate::{
    BindRefusal, CentralEndPurpose, CentralEndUnavailable, CreateRefusal, FirstInputIntent,
    FirstInputObservation, InvalidStartSnapshot, IssuedSessionProof, ObservedSettlement,
    SealedSessionSecretRef, StartProgress, StartSnapshot,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartReadiness {
    Ready,
    LinkUnavailable,
    EnrollmentEnded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessLaunchObservation {
    Held(SessionId),
    Allocated(SessionId),
    SetupFailed(SessionId),
    ProviderIdCollision(SessionId),
    Rejected,
    ProvenNotSent,
    AnswerLost,
}

/// A Reserved answer enters the machine only after its issue was sealed. The
/// driver commits the returned BindIntended snapshot and sealed bytes together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateObservation {
    Reserved(IssuedSessionProof),
    Active {
        central_id: CentralSessionId,
        address: SessionRef,
    },
    Superseded,
    NotSent,
    AnswerLost,
    Refused(CreateRefusal),
    Settled(SessionSettlement),
    Contradicted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindObservation {
    Active(SessionRef),
    Retry,
    Refused(BindRefusal),
    Settled(SessionSettlement),
    Contradicted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessCloseObservation {
    Closed,
    ExactOwnerUnavailable,
    Refused,
    AnswerLost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CentralEndObservation {
    Settled(ObservedSettlement),
    Unavailable(CentralEndUnavailable),
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartEvent {
    Advance(StartReadiness),
    Recover(StartReadiness),
    Launched(HarnessLaunchObservation),
    FirstInputAnswered {
        harness: SessionId,
        input: FirstInputIntent,
        observation: FirstInputObservation,
    },
    CreateAnswered {
        attempt: CreateSessionAttempt,
        observation: CreateObservation,
    },
    BindAnswered {
        issue: SealedSessionSecretRef,
        observation: BindObservation,
    },
    EndForGood,
    HarnessCloseAnswered(HarnessCloseObservation),
    CentralEndAnswered {
        target: CentralSessionId,
        observation: CentralEndObservation,
    },
}

impl EventTag for StartEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Advance(_) => "advance",
            Self::Recover(_) => "recover",
            Self::Launched(_) => "launched",
            Self::FirstInputAnswered { .. } => "first_input_answered",
            Self::CreateAnswered { .. } => "create_answered",
            Self::BindAnswered { .. } => "bind_answered",
            Self::EndForGood => "end_for_good",
            Self::HarnessCloseAnswered(_) => "harness_close_answered",
            Self::CentralEndAnswered { .. } => "central_end_answered",
        }
    }
}

/// The driver may act on these effects only after it confirms the next snapshot's write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartEffect {
    LaunchHeld,
    SubmitFirstInput {
        harness: SessionId,
        input: FirstInputIntent,
    },
    VerifyFirstInput {
        harness: SessionId,
        input: FirstInputIntent,
    },
    CreateCentral(CreateSessionAttempt),
    BindCentral(IssuedSessionProof),
    PublishHeld(SessionId),
    RestoreExact(SessionId),
    RetireAdmission,
    CloseExact(SessionId),
    EndCentral {
        id: CentralSessionId,
        purpose: CentralEndPurpose,
    },
    AwaitReadiness,
    RetryAfterBackoff,
    ReportOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartTransitionRefusal {
    #[error("this observation does not belong to the current start phase")]
    WrongPhase,
    #[error("a create attempt cannot be constructed")]
    InvalidAttempt,
    #[error(transparent)]
    InvalidSnapshot(#[from] InvalidStartSnapshot),
}

impl RefusalTag for StartTransitionRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::WrongPhase => "wrong_phase",
            Self::InvalidAttempt => "invalid_attempt",
            Self::InvalidSnapshot(_) => "invalid_snapshot",
        }
    }
}

pub(super) type MachineResult =
    Result<(StartSnapshot, Vec<StartEffect>), TransitionError<StartTransitionRefusal>>;

pub(super) fn move_to(
    snapshot: &StartSnapshot,
    progress: StartProgress,
    effects: Vec<StartEffect>,
) -> MachineResult {
    let next = snapshot.advance(progress).map_err(snapshot_refusal)?;
    Ok((next, effects))
}

pub(super) fn snapshot_refusal(
    error: InvalidStartSnapshot,
) -> TransitionError<StartTransitionRefusal> {
    TransitionError::Domain(StartTransitionRefusal::InvalidSnapshot(error))
}

pub(super) fn wrong_phase() -> MachineResult {
    Err(TransitionError::Domain(StartTransitionRefusal::WrongPhase))
}
