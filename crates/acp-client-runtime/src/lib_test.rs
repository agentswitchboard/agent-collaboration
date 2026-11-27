#[test]
fn public_paths_name_the_neutral_harness_and_session_types() {
    let _: Option<switchboard_harness_contracts::AutoRealization> =
        None::<acp_client_runtime::AutoRealization>;
    let _: Option<switchboard_harness_contracts::ExternalProviderCreatedSession> =
        None::<acp_client_runtime::ExternalProviderCreatedSession>;
    let _: Option<switchboard_harness_contracts::ProviderSessionActivity> =
        None::<acp_client_runtime::ProviderSessionActivity>;
    let _: Option<switchboard_harness_contracts::InvalidSettingSessionDisposition> =
        None::<acp_client_runtime::InvalidSettingSessionDisposition>;
    let _: Option<session_event_model::ProviderTurnInput> =
        None::<acp_client_runtime::ProviderTurnInput>;
    let _: Option<session_event_model::ExternalProviderApprovalRefusalReason> =
        None::<acp_client_runtime::ExternalProviderApprovalRefusalReason>;
    let _: Option<session_event_model::FailedProviderSetting> =
        None::<acp_client_runtime::FailedProviderSetting>;
}
