//! The issuer failed to provide usable verification or apply the signature check.
use fleet_runtime_ports::{KeyFetchFailure, VerifyFailure};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IssuerKeysUnavailable {
    #[error("the issuer's key source could not be reached")]
    Fetch { reason: KeyFetchFailure },
    #[error("the issuer's public key set is malformed")]
    MalformedKeySet,
    #[error("the issuer key cache is unavailable")]
    CacheUnavailable,
    #[error("the receiver's key-cache clock is unavailable")]
    InvalidClock,
}

/// An unknown key is an invalid signature, rather than an unavailable check.
///
/// ```compile_fail,E0599
/// use fleet_contracts::{IssuerKeysUnavailable, IssuerSignatureRefusal};
/// let _ = IssuerSignatureRefusal::Keys(IssuerKeysUnavailable::UnknownKey);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IssuerSignatureRefusal {
    #[error(transparent)]
    Keys(#[from] IssuerKeysUnavailable),
    #[error("the issuer-signature capability is unavailable")]
    Crypto { reason: VerifyFailure },
}
