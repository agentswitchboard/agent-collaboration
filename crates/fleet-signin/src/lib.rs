//! The organization's stamp and configured issuer trust.
#![forbid(unsafe_code)]

mod issuer_key_cache;
mod issuer_key_resolution;
mod issuer_signature_verifier;
pub mod machine_token;
pub mod organization_authority;

#[cfg(feature = "test-support")]
pub use issuer_key_cache::IssuerCacheOperation;
pub use issuer_key_cache::IssuerKeyCache;
pub use issuer_signature_verifier::IssuerSignatureVerifier;

mod stamp_standing_cache;
pub use stamp_standing_cache::{StampStanding, StandingCache};

pub mod organization_change;
pub mod stamp_store;

mod stamp_signin_service;
pub use stamp_signin_service::{SignInService, StampVerification, StoredStampVerificationFailure};
