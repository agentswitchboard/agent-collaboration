//! Session-owned facts used to follow an approval's parent chain.
use central_storage::StoreFailure;
use fleet_identity_protocol::CreateSessionKind;
use workspace_ids::CentralSessionId;

use crate::{BoundSession, ScopedSession, SessionState};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalChainSession {
    Live {
        scope: ScopedSession,
        parent: Option<CentralSessionId>,
    },
    Ended {
        scope: ScopedSession,
        parent: Option<CentralSessionId>,
    },
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
            SessionState::Active(bound) => {
                let (scope, parent) = recorded_chain_member(bound);
                Ok(Self::Live { scope, parent })
            }
            SessionState::Ended { bound, .. } => {
                let (scope, parent) = recorded_chain_member(bound);
                Ok(Self::Ended { scope, parent })
            }
            SessionState::Missing => Err(ApprovalChainFailure::Missing),
            SessionState::Reserved(_) | SessionState::Abandoned { .. } => {
                Err(ApprovalChainFailure::Unbound)
            }
        }
    }
}

fn recorded_chain_member(bound: BoundSession) -> (ScopedSession, Option<CentralSessionId>) {
    let reservation = bound.reserved.reservation;
    let parent = match reservation.kind {
        CreateSessionKind::Ordinary {} => None,
        CreateSessionKind::Delegate { parent } => Some(parent),
    };
    let scope = ScopedSession {
        machine_id: reservation.machine_id,
        enrollment_id: reservation.enrollment_id,
        session_id: reservation.session_id,
    };
    (scope, parent)
}
