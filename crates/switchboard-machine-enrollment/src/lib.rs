//! Machine device authorization and credential ownership on the Router side.
pub use workspace_ids::MachineId;

mod approved_enrollment_save;
mod credential_store;
mod credential_value;
mod device_state;
mod discovery_answer;
mod enrollment_attempt;
mod enrollment_binding;
mod enrollment_identity;
mod enrollment_outcome;
mod refresh_lease;
mod refresh_owner;
mod renewal_admission;
mod renewal_machine;
mod response_object;
mod retirement_fence;
mod retirement_machine;
mod sign_in_answers;
mod sign_in_client;

pub use credential_store::{
    ConditionalWrite, CredentialStore, CredentialStoreError, KeyedEnrollment, StoreDurability,
};
pub use credential_value::{RefreshCredential, SecretValue};
pub use enrollment_binding::{
    BindingGeneration, EnrollmentBinding, EnrollmentContext, EnrollmentKey, EnrollmentMetadata,
    EnrollmentReceipt, InvalidDeployment, SignInDeployment, StoredEnrollment,
};
pub use enrollment_identity::{
    EnrollmentId, EnrollmentIdentifierError, OrganizationId, RouterClientId,
};
pub use enrollment_outcome::{DecodeAnswerError, EnrollmentEnd, InvalidReason};
pub use refresh_lease::{AccessLease, AccessRequest, LeaseConfigurationFault, LeaseUnavailable};
pub use refresh_owner::{RefreshOwner, RefreshSource};
pub use retirement_fence::RetirementFence;
pub use sign_in_answers::{
    DeviceAuthorization, DeviceInstructions, PollAnswer, RefreshAnswer, SignInFailure, TokenGrant,
    decode_device_authorization, decode_poll, decode_refresh, decode_start_failure,
};

pub use approved_enrollment_save::{ApprovedEnrollmentSaveFailure, EnrollmentSaveFailure};
pub use discovery_answer::{CentralLinkTemplate, DiscoveryAnswer, decode_discovery};
pub use enrollment_attempt::EnrollmentFailure;
#[cfg(feature = "test-support")]
pub use sign_in_client::LoopbackRequestEvent;
pub use sign_in_client::{EnrollmentRequest, SignInClient, SignInClientError};
