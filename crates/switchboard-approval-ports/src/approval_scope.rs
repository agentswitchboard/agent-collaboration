use std::num::NonZeroU64;
use workspace_ids::RequestId;

/// The opaque identity the harness allocates for one managed session under one live owner.
///
/// All asks from that binding use the same identity. Another harness or a replacement owner
/// gets a fresh identity even when its native session ID matches.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LocalSessionBindingId(RequestId);

impl LocalSessionBindingId {
    pub(crate) const fn from_owner_nonce(owner_nonce: RequestId) -> Self {
        Self(owner_nonce)
    }
}

/// A local turn identity the harness supplies from the turn proved at admission.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ApprovalTurnId(RequestId);

impl ApprovalTurnId {
    /// Validates the existing local turn UUIDv7 without minting another identity.
    pub fn parse(value: &str) -> Result<Self, InvalidApprovalTurnId> {
        value
            .parse()
            .map(Self)
            .map_err(|_| InvalidApprovalTurnId::InvalidUuid)
    }

    /// Returns the canonical UUIDv7 of the already-proved turn.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Failure the turn identity parser returns for a noncanonical or non-UUIDv7 value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidApprovalTurnId {
    #[error("approval turn identity must be a canonical RFC UUIDv7")]
    InvalidUuid,
}

/// The exact local session, admitted session run and turn the harness fixes at admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalScope {
    session_binding: LocalSessionBindingId,
    session_run: NonZeroU64,
    turn_id: ApprovalTurnId,
}

impl ApprovalScope {
    /// Captures the harness's proved scope. A form keeps the turn current at admission.
    ///
    /// ```compile_fail,E0308
    /// use std::num::NonZeroU64;
    /// use switchboard_approval_ports::{ApprovalScope, ApprovalTurnId};
    /// use workspace_ids::SessionId;
    /// fn wrong_scope(native_id: SessionId, run: NonZeroU64, turn: ApprovalTurnId) {
    ///     ApprovalScope::from_admitted_turn(native_id, run, turn);
    /// }
    /// ```
    #[must_use]
    pub const fn from_admitted_turn(
        session_binding: LocalSessionBindingId,
        session_run: NonZeroU64,
        turn_id: ApprovalTurnId,
    ) -> Self {
        Self {
            session_binding,
            session_run,
            turn_id,
        }
    }

    #[must_use]
    pub const fn session_binding(&self) -> &LocalSessionBindingId {
        &self.session_binding
    }

    #[must_use]
    pub const fn session_run(&self) -> NonZeroU64 {
        self.session_run
    }

    #[must_use]
    pub const fn turn_id(&self) -> &ApprovalTurnId {
        &self.turn_id
    }
}

/// The harness's opaque name for one wait, made from a fresh harness-allocated UUIDv7.
///
/// ```compile_fail,E0277
/// use switchboard_approval_ports::WaitingRequestId;
/// let _: WaitingRequestId = 42_u64.into();
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WaitingRequestId(RequestId);

impl WaitingRequestId {
    pub(crate) const fn from_admission(local_nonce: RequestId) -> Self {
        Self(local_nonce)
    }
}
