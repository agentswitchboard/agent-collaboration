use super::*;
use crate::{
    AutoPermissionMode, ModelMetadataSource, PreparedCompact, ProviderError, ProviderFacts,
    ProviderInitializationAuthentication, ProviderUpdate,
};
use agent_client_protocol::schema::v1::NewSessionResponse;
use provider_session_machines::{
    ModelReasoningMetadata, ModelReasoningUnknown, ProviderModeObservation, ProviderSettingChoice,
    ProviderSettingsCatalogRecord, SessionGeneration,
    session_owner::{AuthorizationTarget, OwnerIncarnation},
};
use serde_json::{Value, json};
use session_event_model::{InputId, PromptContent, ProviderSubmission};
use switchboard_harness_contracts::ReservedReply;

struct CapturedWorkReply(std::sync::mpsc::Sender<WorkReply>);

impl ReservedReply<WorkReply> for CapturedWorkReply {
    fn send(self: Box<Self>, reply: WorkReply) {
        self.0.send(reply).expect("test owns reply receiver");
    }
}

pub(crate) struct ConnectionCatalogueBehavior;

impl crate::ProviderBehavior for ConnectionCatalogueBehavior {
    fn initialization_authentication(&self) -> ProviderInitializationAuthentication {
        ProviderInitializationAuthentication::NotRequested
    }

    fn can_reuse_setting_confirmation(&self, _: ProviderSettingKind) -> bool {
        false
    }

    fn model_metadata_source(&self) -> ModelMetadataSource {
        ModelMetadataSource::ConnectionRequest {
            method: "listModels",
        }
    }

    fn decode_model_metadata(&self, _: &Value) -> ModelReasoningMetadata {
        ModelReasoningMetadata::default()
    }

    fn auto_permission_mode(&self) -> AutoPermissionMode {
        AutoPermissionMode::UNREALIZED
    }

    fn observe(&self, _: ProviderUpdate) -> Result<ProviderFacts, ProviderError> {
        Ok(ProviderFacts::default())
    }

    fn prepare(&self, _: ProviderSubmission) -> Result<Vec<PromptContent>, ProviderError> {
        Err(ProviderError::Unsupported)
    }

    fn compact(&self) -> Result<PreparedCompact, ProviderError> {
        Err(ProviderError::Unsupported)
    }
}

fn settle_catalogue_reply(
    kind: ProviderSettingKind,
    wanted: &str,
    current_mode: &str,
    reasoning: ModelReasoningUnknown,
) -> (SettingObservation, Arc<ProviderSettingsCatalogRecord>) {
    let options = json!([
        {
            "id": "model", "name": "Model", "category": "model", "type": "select",
            "currentValue": "model-b", "options": [{"value": "model-b", "name": "Model B"}]
        },
        {
            "id": "fast", "name": "Fast", "category": "model_config", "type": "boolean",
            "currentValue": true
        }
    ]);
    let session: NewSessionResponse = serde_json::from_value(json!({
        "sessionId": "settings-fixture", "configOptions": options
    }))
    .expect("fixture session response");
    let mut catalogue = crate::provider_settings_catalog_codec::catalog_from_session_response(
        &session,
        &ConnectionCatalogueBehavior,
        &ModelReasoningMetadata::Unknown(reasoning),
    );
    catalogue.current_mode = Some(current_mode.into());
    catalogue.mode_observation = ProviderModeObservation::Unconfirmed;
    catalogue.modes = vec![ProviderSettingChoice {
        value: current_mode.into(),
        label: format!("Fixture {current_mode}"),
    }];
    let expected = catalogue.clone().into_record();
    assert_eq!(expected.config_options.len(), 2);
    let authorization = DispatchAuthorization {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        run: SessionGeneration::from_number(1),
        input_id: InputId::generate(),
        authorization_seq: 1,
        target: AuthorizationTarget::Session,
    };
    let (send_reply, receive_reply) = std::sync::mpsc::channel();
    let offered = CorrelatedSetting {
        shape: SettingResponseShape {
            catalog: catalogue,
            kind,
            sent: wanted.into(),
            current: None,
            reads_value: true,
            accepted_fallback: None,
            mode_confirmation: None,
        },
        correlation: SettingCorrelation::Explicit {
            authorization: authorization.clone(),
            completion: Box::new(CapturedWorkReply(send_reply)),
        },
    };

    offered.settle(
        Some(Ok(json!({"configOptions": options}))),
        &ConnectionCatalogueBehavior,
    );

    let WorkReply::Dispatch(DispatchResult::SettingFinished {
        authorization: observed_authorization,
        observation,
    }) = receive_reply.try_recv().expect("work reply")
    else {
        panic!("explicit setting must yield a correlated dispatch result");
    };
    assert_eq!(observed_authorization, authorization);
    (observation, expected)
}

#[test]
fn applied_reply_transfers_the_catalogue_acp_held() {
    let (observation, expected) = settle_catalogue_reply(
        ProviderSettingKind::Model,
        "model-b",
        "agent",
        ModelReasoningUnknown::UnsupportedSource,
    );

    let SettingObservation::AppliedWithReadback(readback) = observation else {
        panic!("model selection was applied");
    };
    assert_eq!(readback.catalogue, expected);
}

#[test]
fn refused_reply_transfers_the_catalogue_acp_held() {
    let (observation, expected) = settle_catalogue_reply(
        ProviderSettingKind::Fast,
        "off",
        "plan",
        ModelReasoningUnknown::Malformed,
    );

    let SettingObservation::Refused { catalogue, .. } = observation else {
        panic!("fast remained on after the off request");
    };
    assert_eq!(catalogue, Some(expected));
}
