//! Keys for the supervisor’s public operation failure projection.

use super::PromptRefusal;
use provider_session_machines::harness_lifecycle::{
    AppliedProviderSetting, NotAppliedProviderSetting,
};
use provider_session_machines::{
    InvalidSettingSessionDisposition, ProviderSettingKind, harness_lifecycle::PermissionModeReport,
    session_owner::ProviderWireFailure,
};
use session_event_model::FailedProviderSetting;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum OperationFailureCause {
    HarnessCoded(ProviderWireFailure),
    ResponseUnavailable,
    PromptRefused(PromptRefusal),
    CompactionUnconfirmed,
    PromptOutputLimitExceeded,
    MessageTooLarge,
    InvalidSetting {
        setting: ProviderSettingKind,
        value: String,
        advertised: Vec<String>,
        provider_session_id: String,
        disposition: InvalidSettingSessionDisposition,
    },
    CreatedWithoutSettings {
        provider_session_id: String,
        applied: Box<[AppliedProviderSetting]>,
        failed: FailedProviderSetting,
        not_applied: Box<[NotAppliedProviderSetting]>,
        permission_mode: Box<PermissionModeReport>,
    },
}
