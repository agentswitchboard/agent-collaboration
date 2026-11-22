//! Machine device authorization and credential ownership on the Router side.
mod credential_store;
mod device_state;
mod enrollment_binding;
mod enrollment_identity;
mod refresh_owner;
mod sign_in_answers;

pub use credential_store::{
    ConditionalWrite, CredentialStore, CredentialStoreError, RefreshCredential, SecretValue,
    StoreDurability,
};
pub use device_state::{
    DeviceEnrollment, DeviceEnrollmentInput, DevicePollSchedule, EnrollmentEnd,
};
pub use enrollment_binding::{
    BindingGeneration, EnrollmentBinding, EnrollmentContext, EnrollmentKey, InvalidDeployment,
    SignInDeployment, StoredEnrollment,
};
pub use enrollment_identity::{
    CentralEndpointId, CentralEndpointIds, EnrollmentId, OrganizationId, RouterClientId,
};
pub use refresh_owner::{
    AccessLease, AccessRequest, LeaseConfigurationFault, LeaseUnavailable, RefreshOwner,
    RefreshSource,
};
pub use sign_in_answers::{
    DecodeAnswerError, DeviceAuthorization, DeviceInstructions, InvalidReason, PollAnswer,
    RefreshAnswer, SignInFailure, TokenGrant, decode_device_authorization, decode_discovery,
    decode_poll, decode_refresh, decode_start_failure,
};
