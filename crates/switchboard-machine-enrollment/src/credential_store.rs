use std::fmt;
use std::future::Future;

use zeroize::Zeroizing;

use crate::{CentralEndpointIds, EnrollmentId};

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

/// The durable credential assembled by the client from sign-in's successful token response.
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

/// The caller supplies this store for the current Mac's enrollment.
pub trait CredentialStore: Send + Sync {
    fn read(
        &self,
    ) -> impl Future<Output = Result<Option<RefreshCredential>, CredentialStoreError>> + Send;
    /// Commits the replacement before returning success. On failure the old value remains.
    fn replace(
        &self,
        credential: RefreshCredential,
    ) -> impl Future<Output = Result<(), CredentialStoreError>> + Send;
    fn remove(&self) -> impl Future<Output = Result<(), CredentialStoreError>> + Send;
}
