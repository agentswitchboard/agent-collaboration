//! Session-owned facts used to follow an approval's parent chain.
use central_storage::StoreFailure;
use fleet_identity_protocol::CreateSessionKind;
use workspace_ids::CentralSessionId;

use crate::{ScopedSession, SessionState};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalChainSession {
    pub scope: ScopedSession,
    pub parent: Option<CentralSessionId>,
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
        let bound = match state {
            SessionState::Active(bound) | SessionState::Ended { bound, .. } => bound,
            SessionState::Missing => return Err(ApprovalChainFailure::Missing),
            SessionState::Reserved(_) | SessionState::Abandoned { .. } => {
                return Err(ApprovalChainFailure::Unbound);
            }
        };
        let reservation = bound.reserved.reservation;
        let parent = match reservation.kind {
            CreateSessionKind::Ordinary {} => None,
            CreateSessionKind::Delegate { parent } => Some(parent),
        };
        Ok(Self {
            scope: ScopedSession {
                machine_id: reservation.machine_id,
                enrollment_id: reservation.enrollment_id,
                session_id: reservation.session_id,
            },
            parent,
        })
    }
}
