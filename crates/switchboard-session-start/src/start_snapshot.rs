//! The durable lifecycle facts owned by one Mac session start.

use fleet_identity_protocol::{
    CreateFleetSession, CreateSessionAttempt, CreateSessionKind, CreateSessionRefusal,
    EnrollmentId, HarnessKind, NewEndSettlement, OrganizationId, SessionCreator, SessionSettlement,
    SessionTime,
};
use state_machine_core::StateTag;
use workspace_ids::{CentralSessionId, MachineId, SessionId, SessionOperationId, SessionRef};

use crate::{DurableFirstInput, FirstInputIntent, LaunchProgress, MakePlan};

/// The start ID also identifies its Central Create operation. Its scope never changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartScope {
    pub operation_id: SessionOperationId,
    pub organization: OrganizationId,
    pub machine: MachineId,
    pub enrollment: EnrollmentId,
}

/// The proof issue address inside the start row. The bytes are sealed by the store adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedSessionSecretRef {
    pub start_id: SessionOperationId,
    pub central_id: CentralSessionId,
    pub attempt: CreateSessionAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IssuedSessionProof {
    pub secret: SealedSessionSecretRef,
    pub reserved_until: SessionTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistrationProgress {
    NeedCreate,
    CreateIntended(CreateSessionAttempt),
    BindIntended(IssuedSessionProof),
    /// Contradictory evidence is retained for investigation and cannot authorize publication.
    Blocked(RegistrationContradiction),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistrationContradiction {
    UnexpectedActive {
        central_id: CentralSessionId,
        address: SessionRef,
    },
    CreateReply,
    BindAddress {
        issued: IssuedSessionProof,
        address: SessionRef,
    },
    BindReply {
        issued: IssuedSessionProof,
    },
    AttemptLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartRefusal {
    LinkUnavailable,
    EnrollmentEnded,
    HarnessNotSent,
    InitialInputUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchUncertainty {
    AnswerLost,
    RouterRestarted,
    RecoveredAllocation {
        harness: SessionId,
    },
    FirstInputUnconfirmed {
        harness: SessionId,
        input: FirstInputIntent,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClosingReason {
    SetupFailed,
    CreateRefused(CreateRefusal),
    BindRefused(BindRefusal),
    RegistrationSettled,
    EnrollmentEnded,
    EndedForGood,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateRefusal {
    Policy(CreateSessionRefusal),
    OperationConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindRefusal {
    WrongSecret,
    AlreadyBoundToDifferentId,
    AddressTaken,
    NoSuchStart,
}

/// Evidence or debt for closing the exact harness conversation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessCloseProgress {
    Pending,
    AnswerLost,
    Closed,
    ExactOwnerUnavailable,
    Refused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralEndPurpose {
    AbandonReservation,
    EndBoundSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CentralCloseProgress {
    NotOwed,
    SettledWithoutIssuingHere(SessionSettlement),
    /// A Create may have committed, but this retired enrollment cannot reconcile it.
    UnverifiedCreate,
    Owed {
        secret: SealedSessionSecretRef,
        purpose: CentralEndPurpose,
        certainty: EndCallCertainty,
    },
    Settled {
        secret: SealedSessionSecretRef,
        settlement: ObservedSettlement,
    },
    Unavailable {
        secret: SealedSessionSecretRef,
        reason: CentralEndUnavailable,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndCallCertainty {
    Pending,
    RetryNeeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralEndUnavailable {
    EnrollmentEnded,
    NoSuchStart,
    TargetRefused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedSettlement {
    New(NewEndSettlement),
    Existing(SessionSettlement),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosingSession {
    pub harness: SessionId,
    pub reason: ClosingReason,
    pub harness_close: HarnessCloseProgress,
    pub central_close: CentralCloseProgress,
}

/// Final evidence keeps identity and outcome. It has no usable sealed proof reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndOutcome {
    LaunchRejected,
    ProviderIdCollision {
        reported: SessionId,
    },
    Closed {
        harness: SessionId,
        reason: ClosingReason,
        harness_close: HarnessCloseProgress,
        central: EndedCentralSession,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndedCentralSession {
    NoSession,
    SettledWithoutIssuingHere(SessionSettlement),
    Settled {
        id: CentralSessionId,
        evidence: ObservedSettlement,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartProgress {
    Asked,
    Launching(LaunchProgress),
    Registering {
        harness: SessionId,
        registration: RegistrationProgress,
    },
    Open {
        harness: SessionId,
        secret: SealedSessionSecretRef,
    },
    Closing(ClosingSession),
    Ended(EndOutcome),
    Refused(StartRefusal),
    LaunchUnknown(LaunchUncertainty),
}

/// The phase payload a store may load only after it validates its fields and scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartSnapshot {
    scope: StartScope,
    harness_kind: HarnessKind,
    make_plan: MakePlan,
    first_input: Option<DurableFirstInput>,
    progress: StartProgress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidStartSnapshot {
    #[error("the sealed proof belongs to a different start")]
    WrongProofOwner,
    #[error("an ended session still has an unfinished harness close")]
    UnfinishedHarnessClose,
    #[error("a closing session already has both final outcomes")]
    FinishedClosure,
    #[error("the Make phase or durable input evidence conflicts with the initiating request")]
    WrongFirstInput,
}

impl StartSnapshot {
    #[must_use]
    pub const fn asked(scope: StartScope, harness_kind: HarnessKind, make_plan: MakePlan) -> Self {
        Self {
            scope,
            harness_kind,
            make_plan,
            first_input: None,
            progress: StartProgress::Asked,
        }
    }

    pub fn restore(
        scope: StartScope,
        harness_kind: HarnessKind,
        make_plan: MakePlan,
        first_input: Option<DurableFirstInput>,
        progress: StartProgress,
    ) -> Result<Self, InvalidStartSnapshot> {
        validate_make(&make_plan, first_input.as_ref(), &progress)?;
        let secret = match &progress {
            StartProgress::Registering {
                registration: RegistrationProgress::BindIntended(issued),
                ..
            } => Some(&issued.secret),
            StartProgress::Registering {
                registration:
                    RegistrationProgress::Blocked(
                        RegistrationContradiction::BindAddress { issued, .. }
                        | RegistrationContradiction::BindReply { issued },
                    ),
                ..
            } => Some(&issued.secret),
            StartProgress::Open { secret, .. } => Some(secret),
            StartProgress::Closing(closing) => {
                if !matches!(
                    closing.harness_close,
                    HarnessCloseProgress::Pending | HarnessCloseProgress::AnswerLost
                ) && matches!(
                    closing.central_close,
                    CentralCloseProgress::NotOwed
                        | CentralCloseProgress::SettledWithoutIssuingHere(_)
                        | CentralCloseProgress::Settled { .. }
                ) {
                    return Err(InvalidStartSnapshot::FinishedClosure);
                }
                match &closing.central_close {
                    CentralCloseProgress::NotOwed
                    | CentralCloseProgress::SettledWithoutIssuingHere(_)
                    | CentralCloseProgress::UnverifiedCreate => None,
                    CentralCloseProgress::Owed { secret, .. }
                    | CentralCloseProgress::Settled { secret, .. }
                    | CentralCloseProgress::Unavailable { secret, .. } => Some(secret),
                }
            }
            StartProgress::Ended(EndOutcome::Closed {
                harness_close: HarnessCloseProgress::Pending | HarnessCloseProgress::AnswerLost,
                ..
            }) => {
                return Err(InvalidStartSnapshot::UnfinishedHarnessClose);
            }
            StartProgress::Asked
            | StartProgress::Launching(_)
            | StartProgress::Registering { .. }
            | StartProgress::Ended(_)
            | StartProgress::Refused(_)
            | StartProgress::LaunchUnknown(_) => None,
        };
        if secret.is_some_and(|reference| reference.start_id != scope.operation_id) {
            return Err(InvalidStartSnapshot::WrongProofOwner);
        }
        Ok(Self {
            scope,
            harness_kind,
            make_plan,
            first_input,
            progress,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &StartScope {
        &self.scope
    }

    #[must_use]
    pub const fn harness_kind(&self) -> &HarnessKind {
        &self.harness_kind
    }

    #[must_use]
    pub const fn progress(&self) -> &StartProgress {
        &self.progress
    }

    #[must_use]
    pub const fn make_plan(&self) -> &MakePlan {
        &self.make_plan
    }

    #[must_use]
    pub const fn first_input(&self) -> Option<&DurableFirstInput> {
        self.first_input.as_ref()
    }

    /// The normalized Central request for an ordinary start the local operator entrance admitted.
    #[must_use]
    pub fn create_request(&self, attempt: CreateSessionAttempt) -> CreateFleetSession {
        CreateFleetSession {
            operation_id: self.scope.operation_id.clone(),
            attempt,
            creator: SessionCreator::RouterOperator {},
            agent_program: self.harness_kind.clone(),
            session_kind: CreateSessionKind::Ordinary {},
        }
    }

    pub(super) fn advance(&self, progress: StartProgress) -> Result<Self, InvalidStartSnapshot> {
        self.advance_with_input(progress, self.first_input.clone())
    }

    pub(super) fn advance_with_input(
        &self,
        progress: StartProgress,
        first_input: Option<DurableFirstInput>,
    ) -> Result<Self, InvalidStartSnapshot> {
        Self::restore(
            self.scope.clone(),
            self.harness_kind.clone(),
            self.make_plan.clone(),
            first_input,
            progress,
        )
    }
}

fn validate_make(
    plan: &MakePlan,
    observed: Option<&DurableFirstInput>,
    progress: &StartProgress,
) -> Result<(), InvalidStartSnapshot> {
    let valid = match progress {
        StartProgress::Launching(LaunchProgress::FirstInputIntended { input, .. })
        | StartProgress::LaunchUnknown(LaunchUncertainty::FirstInputUnconfirmed {
            input, ..
        }) => {
            observed.is_none()
                && matches!(plan, MakePlan::FirstInputRequired(expected) if input == expected)
        }
        StartProgress::Launching(LaunchProgress::Allocated { .. })
        | StartProgress::LaunchUnknown(LaunchUncertainty::RecoveredAllocation { .. }) => {
            observed.is_none() && matches!(plan, MakePlan::FirstInputRequired(_))
        }
        StartProgress::Registering { harness, .. } | StartProgress::Open { harness, .. } => {
            matches_make_evidence(plan, observed, harness, true)
        }
        StartProgress::Closing(closing) => {
            let required = closing.central_close != CentralCloseProgress::NotOwed;
            matches_make_evidence(plan, observed, &closing.harness, required)
        }
        StartProgress::Ended(EndOutcome::Closed {
            harness, central, ..
        }) => matches_make_evidence(
            plan,
            observed,
            harness,
            *central != EndedCentralSession::NoSession,
        ),
        StartProgress::Asked
        | StartProgress::Launching(LaunchProgress::Intent)
        | StartProgress::LaunchUnknown(
            LaunchUncertainty::AnswerLost | LaunchUncertainty::RouterRestarted,
        )
        | StartProgress::Refused(_)
        | StartProgress::Ended(
            EndOutcome::LaunchRejected | EndOutcome::ProviderIdCollision { .. },
        ) => observed.is_none(),
    };
    if valid {
        Ok(())
    } else {
        Err(InvalidStartSnapshot::WrongFirstInput)
    }
}

fn matches_make_evidence(
    plan: &MakePlan,
    observed: Option<&DurableFirstInput>,
    harness: &SessionId,
    required: bool,
) -> bool {
    match (plan, observed) {
        (MakePlan::HeldOnLaunch, None) => true,
        (MakePlan::HeldOnLaunch, Some(_)) => false,
        (MakePlan::FirstInputRequired(expected), Some(evidence)) => {
            &evidence.input == expected && &evidence.harness == harness
        }
        (MakePlan::FirstInputRequired(_), None) => !required,
    }
}

impl StateTag for StartSnapshot {
    fn state_tag(&self) -> &'static str {
        match &self.progress {
            StartProgress::Asked => "asked",
            StartProgress::Launching(LaunchProgress::Intent) => "launching",
            StartProgress::Launching(LaunchProgress::Allocated { .. }) => "launching_allocated",
            StartProgress::Launching(LaunchProgress::FirstInputIntended { .. }) => {
                "launching_first_input_intended"
            }
            StartProgress::Registering { registration, .. } => match registration {
                RegistrationProgress::NeedCreate => "registering_need_create",
                RegistrationProgress::CreateIntended(_) => "registering_create_intended",
                RegistrationProgress::BindIntended(_) => "registering_bind_intended",
                RegistrationProgress::Blocked(_) => "registering_blocked",
            },
            StartProgress::Open { .. } => "open",
            StartProgress::Closing(_) => "closing",
            StartProgress::Ended(_) => "ended",
            StartProgress::Refused(_) => "refused",
            StartProgress::LaunchUnknown(_) => "launch_unknown",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.progress,
            StartProgress::Ended(_) | StartProgress::Refused(_) | StartProgress::LaunchUnknown(_)
        )
    }
}
