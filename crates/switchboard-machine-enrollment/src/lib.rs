//! Machine device authorization and credential ownership on the Router side.
pub use workspace_ids::MachineId;

mod credential_store;
mod credential_value;
mod device_state;
mod discovery_answer;
mod enrollment_attempt;
mod enrollment_binding;
mod enrollment_identity;
mod enrollment_outcome;
mod refresh_owner;
mod response_object;
mod sign_in_answers;
mod sign_in_client;

pub use credential_store::{
    ConditionalWrite, CredentialStore, CredentialStoreError, StoreDurability,
};
pub use credential_value::{RefreshCredential, SecretValue};
pub use device_state::{DeviceEnrollment, DeviceEnrollmentInput, DevicePollSchedule};
pub use enrollment_binding::{
    BindingGeneration, EnrollmentBinding, EnrollmentContext, EnrollmentKey, EnrollmentMetadata,
    EnrollmentReceipt, InvalidDeployment, SignInDeployment, StoredEnrollment,
};
pub use enrollment_identity::{
    EnrollmentId, EnrollmentIdentifierError, OrganizationId, RouterClientId,
};
pub use enrollment_outcome::{DecodeAnswerError, EnrollmentEnd, InvalidReason};
pub use refresh_owner::{
    AccessLease, AccessRequest, LeaseConfigurationFault, LeaseUnavailable, RefreshOwner,
    RefreshSource,
};
pub use sign_in_answers::{
    DeviceAuthorization, DeviceInstructions, PollAnswer, RefreshAnswer, SignInFailure, TokenGrant,
    decode_device_authorization, decode_poll, decode_refresh, decode_start_failure,
};

pub use discovery_answer::{CentralLinkTemplate, DiscoveryAnswer, decode_discovery};
pub use enrollment_attempt::EnrollmentFailure;
#[cfg(feature = "test-support")]
pub use sign_in_client::LoopbackRequestEvent;
pub use sign_in_client::{EnrollmentRequest, SignInClient, SignInClientError};
