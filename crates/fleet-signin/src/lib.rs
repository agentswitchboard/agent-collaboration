//! The organization's stamp and configured issuer trust.
#![forbid(unsafe_code)]

mod approval_person_admission;
pub mod approval_person_wire;
mod fleet_permission;

pub use approval_person_admission::{
    AdmittedApprovalPersonOperation, ApprovalApplyingContext, ApprovalPersonAdmission,
    ApprovalPersonVerificationInput, VerifiedApprovalDecideRetry, VerifiedApprovalPersonRequest,
};
pub use fleet_permission::FleetPermission;
pub use person_proof::FactorStanding;

mod fence_revision;
pub use fence_revision::{FenceRevision, FenceRevisionError};
pub mod person_proof;

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
