use std::fmt;
use std::future::Future;

use zeroize::Zeroizing;

use crate::{
    BindingGeneration, CentralEndpointIds, EnrollmentBinding, EnrollmentEnd, EnrollmentId,
    StoredEnrollment,
};

/// A secret from sign-in or the injected store. Only an explicit handoff can expose its bytes.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretValue(Zeroizing<String>);

impl SecretValue {
    #[must_use]
    pub fn new(secret: String) -> Option<Self> {
        let secret = Zeroizing::new(secret);
        (!secret.is_empty()).then_some(Self(secret))
    }

    /// Exposes the value for an authorized wire or credential-store operation.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

/// The client reads these credential fields from sign-in’s successful token response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshCredential {
    pub enrollment_id: EnrollmentId,
    pub central: CentralEndpointIds,
    pub refresh_token: SecretValue,
}

/// A credential store adapter reports failed operations without carrying secret-bearing errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("the enrollment credential could not be read")]
    Read,
    #[error("the enrollment credential could not be replaced")]
    Replace,
    #[error("the enrollment credential could not be removed")]
    Remove,
}

/// The store reports whether the expected generation still owned a mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalWrite {
    Applied,
    Superseded,
}

/// The store reports whether writing can produce a credential another process can read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreDurability {
    Durable,
    MemoryOnly,
}

/// Composition supplies this coherent, generation-fenced store for one Mac.
pub trait CredentialStore: Send + Sync {
    /// Whether an interactive command can report a durable enrollment.
    fn durability(&self) -> StoreDurability;

    fn read(&self) -> impl Future<Output = Result<StoredEnrollment, CredentialStoreError>> + Send;

    /// Atomically commits the complete binding at expected + 1 if expected is current.
    /// On error or cancellation, no uncommitted write may become visible later.
    fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Removes the current secret and records an ending at expected + 1.
    fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Records an ending at expected + 1 if physical secret removal failed.
    /// A reader of the marker must never return the old binding.
    fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;
}
