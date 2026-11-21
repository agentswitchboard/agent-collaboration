//! Closed failures shared by stamp decoding and verified identity admission.
use crate::IssuerSignatureRefusal;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FleetOrganizationStatementRefusal {
    #[error("Fleet organization statement is malformed")]
    Malformed,
    #[error("Fleet organization statement has the wrong token type")]
    WrongTokenType,
    #[error("Fleet organization statement algorithm is unsupported")]
    UnsupportedAlgorithm,
    #[error("Fleet organization statement signature is invalid")]
    BadSignature,
    #[error("Fleet organization statement has the wrong purpose")]
    WrongPurpose,
    #[error("Fleet organization statement version is unsupported")]
    UnsupportedVersion,
    #[error("Fleet organization statement has the wrong issuer")]
    WrongIssuer,
    #[error("Fleet organization statement has the wrong audience")]
    WrongAudience,
    #[error("Fleet organization statement names a different organization")]
    OrganizationMismatch,
    #[error("Fleet organization statement outlives the 120-day policy")]
    LifetimeExceedsPolicy,
    #[error("Fleet organization statement has expired")]
    Expired,
    #[error("Fleet organization statement is not yet valid")]
    NotYetValid,
    #[error("Fleet organization statement trust configuration is invalid")]
    InvalidConfiguration,
    #[error("Fleet organization statement verification is unavailable: {0}")]
    VerificationUnavailable(IssuerSignatureRefusal),
}

/// A stored facts read authenticates the signed profile without time admission.
///
/// ```compile_fail,E0599
/// use fleet_contracts::StoredStatementRefusal;
/// let _ = (StoredStatementRefusal::Expired, StoredStatementRefusal::NotYetValid);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoredStatementRefusal {
    #[error("Fleet organization statement is malformed")]
    Malformed,
    #[error("Fleet organization statement has the wrong token type")]
    WrongTokenType,
    #[error("Fleet organization statement algorithm is unsupported")]
    UnsupportedAlgorithm,
    #[error("Fleet organization statement signature is invalid")]
    BadSignature,
    #[error("Fleet organization statement has the wrong purpose")]
    WrongPurpose,
    #[error("Fleet organization statement version is unsupported")]
    UnsupportedVersion,
    #[error("Fleet organization statement has the wrong issuer")]
    WrongIssuer,
    #[error("Fleet organization statement has the wrong audience")]
    WrongAudience,
    #[error("Fleet organization statement names a different organization")]
    OrganizationMismatch,
    #[error("Fleet organization statement outlives the 120-day policy")]
    LifetimeExceedsPolicy,
    #[error("Fleet organization statement trust configuration is invalid")]
    InvalidConfiguration,
    #[error("Fleet organization statement verification is unavailable: {0}")]
    VerificationUnavailable(IssuerSignatureRefusal),
}

impl From<StoredStatementRefusal> for FleetOrganizationStatementRefusal {
    fn from(value: StoredStatementRefusal) -> Self {
        match value {
            StoredStatementRefusal::Malformed => Self::Malformed,
            StoredStatementRefusal::WrongTokenType => Self::WrongTokenType,
            StoredStatementRefusal::UnsupportedAlgorithm => Self::UnsupportedAlgorithm,
            StoredStatementRefusal::BadSignature => Self::BadSignature,
            StoredStatementRefusal::WrongPurpose => Self::WrongPurpose,
            StoredStatementRefusal::UnsupportedVersion => Self::UnsupportedVersion,
            StoredStatementRefusal::WrongIssuer => Self::WrongIssuer,
            StoredStatementRefusal::WrongAudience => Self::WrongAudience,
            StoredStatementRefusal::OrganizationMismatch => Self::OrganizationMismatch,
            StoredStatementRefusal::LifetimeExceedsPolicy => Self::LifetimeExceedsPolicy,
            StoredStatementRefusal::InvalidConfiguration => Self::InvalidConfiguration,
            StoredStatementRefusal::VerificationUnavailable(reason) => {
                Self::VerificationUnavailable(reason)
            }
        }
    }
}
