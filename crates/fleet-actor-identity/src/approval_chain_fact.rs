//! Session-owned facts used to follow an approval's parent chain.
use central_storage::StoreFailure;
use fleet_identity_protocol::CreateSessionKind;
use workspace_ids::CentralSessionId;

use crate::{BoundSession, ScopedSession, SessionState};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalChainFact {
    Live { parent: Option<CentralSessionId> },
    Ended { parent: Option<CentralSessionId> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalChainSession {
    pub scope: ScopedSession,
    pub fact: ApprovalChainFact,
}

#[derive(Debug, thiserror::Error)]
pub enum ApprovalChainFailure {
    #[error("the recorded approval session is missing")]
    Missing,
    #[error("the recorded approval session never bound")]
    Unbound,
    #[error(transparent)]
    Store(#[from] StoreFailure),
}

impl ApprovalChainSession {
    pub(crate) fn from_state(state: SessionState) -> Result<Self, ApprovalChainFailure> {
        match state {
            SessionState::Active(bound) => Ok(Self::from_bound(bound, |parent| {
                ApprovalChainFact::Live { parent }
            })),
            SessionState::Ended { bound, .. } => Ok(Self::from_bound(bound, |parent| {
                ApprovalChainFact::Ended { parent }
            })),
            SessionState::Missing => Err(ApprovalChainFailure::Missing),
            SessionState::Reserved(_) | SessionState::Abandoned { .. } => {
                Err(ApprovalChainFailure::Unbound)
            }
        }
    }

    fn from_bound(
        bound: BoundSession,
        project: impl FnOnce(Option<CentralSessionId>) -> ApprovalChainFact,
    ) -> Self {
        let reservation = bound.reserved.reservation;
        let parent = match reservation.kind {
            CreateSessionKind::Ordinary {} => None,
            CreateSessionKind::Delegate { parent } => Some(parent),
        };
        Self {
            scope: ScopedSession {
                machine_id: reservation.machine_id,
                enrollment_id: reservation.enrollment_id,
                session_id: reservation.session_id,
            },
            fact: project(parent),
        }
    }
}
