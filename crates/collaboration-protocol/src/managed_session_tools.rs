//! Contracts between the managed start owner and the session-only MCP listener.

use std::{future::Future, pin::Pin};

use fleet_identity_protocol::SessionSecret;
use workspace_ids::{ManagedToolBindingId, RouterRunId, SessionOperationId, SessionRef};

/// The identity of one proposed or live tool binding, even before Claude returns its ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedSessionToolBinding {
    pub start: SessionOperationId,
    pub router_run: RouterRunId,
    pub binding: ManagedToolBindingId,
}

/// An independently minted local bearer. This is never the proof issued by Central.
/// Its underlying value zeroizes and redacts debug output.
pub struct ManagedToolBearer(pub SessionSecret);

/// A prepared loopback URL and its separate bearer header for ACP session setup.
/// The descriptor and bearer live in memory for this binding only.
pub struct ManagedToolDescriptor {
    pub url: String,
    pub bearer: ManagedToolBearer,
}

/// Why the private listener could not prepare a descriptor for this owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManagedToolPreparationError {
    #[error("the managed tool listener is unavailable")]
    ListenerUnavailable,
    #[error("this managed tool binding has retired")]
    Retired,
    #[error("the managed tool credential could not be generated")]
    CredentialUnavailable,
}

/// Prepares a descriptor before a provider ID exists. Repeating the same binding
/// returns its same descriptor. A new owner must use a new binding identity.
/// Preparation permits protocol discovery and grants no session tool effects.
pub trait PrepareManagedToolDescriptor: Send + Sync {
    fn prepare_managed_descriptor(
        &self,
        binding: &ManagedSessionToolBinding,
    ) -> Result<ManagedToolDescriptor, ManagedToolPreparationError>;
}

/// Keeps this binding admitted until a tool reaches its existing effect-admission point.
/// The owner fences subsequent admission at retirement. Dropping the lease releases this one.
pub trait ManagedToolAdmissionLease: Send {}

/// The start owner's answer for an authenticated descriptor's exact binding.
pub enum ManagedToolResolution {
    /// Discovery is allowed. No tool invocation can act as this session yet.
    Pending,
    /// The durable Open state, current link and exact live owner all agree.
    Open {
        address: SessionRef,
        lease: Box<dyn ManagedToolAdmissionLease>,
    },
    /// This binding can never serve tools again, even if the session is reattached.
    Retired,
}

/// A transient or corrupt local lookup cannot confer authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManagedToolResolveError {
    #[error("the managed session's owning state is unavailable")]
    Unavailable,
    #[error("the managed session's durable binding is invalid")]
    CorruptBinding,
}

/// Completes one lookup and reserves admission only for a current Open owner.
pub type ResolveManagedStartFuture<'lookup> = Pin<
    Box<
        dyn Future<Output = Result<ManagedToolResolution, ManagedToolResolveError>>
            + Send
            + 'lookup,
    >,
>;

/// Resolves the start behind a descriptor whose bearer the private listener
/// already verified. There is no lookup by a caller-supplied harness ID.
pub trait ManagedStartResolver: Send + Sync {
    fn resolve_managed_start<'lookup>(
        &'lookup self,
        binding: &'lookup ManagedSessionToolBinding,
    ) -> ResolveManagedStartFuture<'lookup>;
}
