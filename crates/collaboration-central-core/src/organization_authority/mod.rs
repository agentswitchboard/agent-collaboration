//! Core's composition of signed-person admission and Fleet trust.
mod identity;
pub(crate) mod machine_login;
mod sign_in_verification;

pub use identity::{OrganizationIdentity, OrganizationIdentityRefusal, ProvisioningStampAction};
pub use sign_in_verification::{SignInVerification, SignInVerifiers};
