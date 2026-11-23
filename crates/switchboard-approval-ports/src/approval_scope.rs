use std::num::NonZeroU64;
use workspace_ids::RequestId;

/// The opaque identity the session owner allocates for one managed session under that owner.
///
/// The owner registers the handle with custody before giving it to the harness. The harness
/// echoes it on each ask. A replacement owner gets a fresh handle for the same session.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LocalSessionBindingId(RequestId);

/// Binding constructors the Host gives to the session owner at composition.
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::HarnessApprovalValues;
/// use workspace_ids::RequestId;
/// fn wrong_owner(harness: &HarnessApprovalValues, nonce: RequestId) {
///     harness.allocate_session_binding(nonce);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::CustodyApprovalValues;
/// use workspace_ids::RequestId;
/// fn wrong_owner(custody: &CustodyApprovalValues, nonce: RequestId) {
///     custody.allocate_session_binding(nonce);
/// }
/// ```
#[derive(Debug)]
pub struct SessionOwnerApprovalValues {
    _private: (),
}

impl SessionOwnerApprovalValues {
    pub(crate) const fn for_composition() -> Self {
        Self { _private: () }
    }

    /// Wraps a fresh UUIDv7 the session owner allocated for its exact managed association.
    #[must_use]
    pub const fn allocate_session_binding(&self, owner_nonce: RequestId) -> LocalSessionBindingId {
        LocalSessionBindingId(owner_nonce)
    }
}

/// Custody implements this registration and retirement port for the session owner.
pub trait SessionBindingCustodyPort {
    /// The exact managed association supplied by the session owner and resolved by the
    /// session-record leaf. Its representation keeps provider-native IDs and transport handles private.
    type ManagedAssociation;

    /// Associates this fresh binding with the exact managed session under its current owner.
    ///
    /// Registration is ordered with admissions. The owner completes it before handing the
    /// binding to the harness, and custody uses this association when consulting the session record.
    fn register_session_binding(
        &self,
        binding: LocalSessionBindingId,
        association: Self::ManagedAssociation,
    );

    /// Invalidates this exact binding on owner replacement or session close.
    ///
    /// Retirement is ordered with admissions and cannot invalidate a replacement binding.
    fn retire_session_binding(&self, binding: LocalSessionBindingId);
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

/// The registered session binding, admitted session run and turn the harness fixes at admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalScope {
    session_binding: LocalSessionBindingId,
    session_run: NonZeroU64,
    turn_id: ApprovalTurnId,
}

impl ApprovalScope {
    /// Captures the harness's proved scope using the binding supplied by the session owner.
    /// A form keeps the turn current at admission.
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
