//! The shared cryptographic person proof; organization transactions own application.

mod admission;
mod compact_jwt;
mod link_attachment;
mod link_token;
mod refusal;
pub use link_attachment::PersonSocketAttachment;
pub use link_token::{CheckedPersonLink, PersonLinkFacts, PersonLinkSecondFactor};
mod signed_claims;
mod verifier;

pub use admission::{
    AdmissionRefusal, AdmittedRequest, FloorRevision, OrganizationFloor, OrganizationFloorState,
    PersonAdmissionFacts, PersonFloor, PersonFloorState, SECOND_FACTOR_WINDOW_SECONDS,
    admission_before_person, decide_admission, request_time_refusal,
};
pub use compact_jwt::{sha256_url, uuid_v7};
pub use refusal::PersonRequestRefusal;
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
pub use signed_claims::{PersonClaims, PersonTokenHeader, ProofClaims, ProofHeader};
pub use verifier::{
    CheckedPersonProof, PersonProofFacts, PersonProofVerifier, PersonRequestPolicy,
    in_request_window,
};
