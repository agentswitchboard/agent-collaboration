use std::fmt;

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

/// The client reads these credential fields from sign-in’s successful token response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshCredential {
    pub enrollment_id: EnrollmentId,
    pub central: CentralEndpointIds,
    pub refresh_token: SecretValue,
}
