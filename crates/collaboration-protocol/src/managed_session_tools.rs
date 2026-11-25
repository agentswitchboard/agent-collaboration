//! Contracts between the managed start owner and the session-only MCP listener.

use std::{fmt, future::Future, pin::Pin};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use workspace_ids::{ManagedToolBindingId, RouterRunId, SessionOperationId, SessionRef};
use zeroize::Zeroizing;

/// The identity of one proposed or live tool binding, even before the harness returns its ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedSessionToolBinding {
    pub start: SessionOperationId,
    pub router_run: RouterRunId,
    pub binding: ManagedToolBindingId,
}

/// The local bearer the Router mints for one managed tool binding. Its header
/// value zeroizes on drop and is omitted from debug output.
#[derive(Clone)]
pub struct ManagedToolBearer(Zeroizing<String>);

impl ManagedToolBearer {
    /// Encodes fresh bytes drawn from the Router's cryptographic random source.
    #[must_use]
    pub fn from_random_bytes(bytes: &[u8; 32]) -> Self {
        Self(Zeroizing::new(URL_SAFE_NO_PAD.encode(bytes)))
    }

    /// Returns the bearer value for ACP header construction or request verification.
    #[must_use]
    pub fn expose_header_value(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ManagedToolBearer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ManagedToolBearer([REDACTED])")
    }
}

/// A prepared loopback URL and its separate bearer header for harness session setup.
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
/// The harness adapter must enforce exclusive routing for the Router's tools.
/// Cursor's ACP client accepts the HTTP descriptor and headers, but can retain
/// a same-name configured server when its per-session client fails to load.
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

#[cfg(test)]
#[path = "managed_session_tools_test.rs"]
mod tests;
