//! The link driver's lease-source boundary.

use std::{future::Future, pin::Pin, time::Duration};

use central_link_client::EnrollmentEnd;
use central_link_protocol::MachineTokenText;

use crate::link_policy::{LeaseVersion, LeaseWindow};

/// A committed access token with a lease-source-owned binding and elapsed usability window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkLease<Binding> {
    pub window: LeaseWindow<Binding>,
    pub token: MachineTokenText,
}

/// The lease owner reports these outcomes without including credentials or server text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseFailure {
    Unavailable { retry_after: Duration },
    EnrollmentEnded,
    Fault,
}

/// A lease acquisition tied to the caller's exact enrollment binding.
pub type LeaseFuture<'source, Binding> =
    Pin<Box<dyn Future<Output = Result<LinkLease<Binding>, LeaseFailure>> + Send + 'source>>;

/// The enrollment owner supplies committed leases and retires the exact credential
/// binding (organization, machine and enrollment). That owner completes any
/// already-started sign-in rotation even if a socket drops its waiting future.
pub trait LeaseSource<Binding>: Send + Sync {
    fn acquire<'source>(
        &'source self,
        binding: &'source Binding,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, Binding>;

    /// Submit the exact retirement to the enrollment owner before returning.
    fn end(&self, binding: &Binding, cause: EnrollmentEnd);
}
