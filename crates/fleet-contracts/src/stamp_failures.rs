//! Closed answers for the two private organization-stamp calls.
use central_storage::StoreFailure;

/// A provision call cannot report a floor failure.
///
/// ```
/// use fleet_contracts::{DigestFailure, AuthorizationRevisionFailure, ProvisionFailure};
/// let _ = (ProvisionFailure::StaleStatement, DigestFailure::Unprovisioned,
///          AuthorizationRevisionFailure::OrganizationFloorMissing);
/// ```
///
/// ```compile_fail,E0599
/// use fleet_contracts::ProvisionFailure;
/// let _ = ProvisionFailure::OrganizationFloorMissing;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProvisionFailure {
    #[error("organization mismatch")]
    OrganizationMismatch,
    #[error("invalid organization statement")]
    InvalidStatement,
    #[error("stale organization statement")]
    StaleStatement,
    #[error("organization sign-in unavailable")]
    Unavailable,
    #[error("organization sign-in outcome unknown")]
    OutcomeUnknown,
}

impl From<StoreFailure> for ProvisionFailure {
    fn from(failure: StoreFailure) -> Self {
        match failure {
            StoreFailure::AnswerLost => Self::OutcomeUnknown,
            StoreFailure::Unavailable
            | StoreFailure::Corrupt { .. }
            | StoreFailure::AlarmUnpublishable { .. } => Self::Unavailable,
        }
    }
}

/// A digest read has no unknown-write answer.
///
/// ```compile_fail,E0599
/// use fleet_contracts::DigestFailure;
/// let _ = DigestFailure::OutcomeUnknown;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DigestFailure {
    #[error("organization mismatch")]
    OrganizationMismatch,
    #[error("organization is unprovisioned")]
    Unprovisioned,
    #[error("organization sign-in unavailable")]
    Unavailable,
}
