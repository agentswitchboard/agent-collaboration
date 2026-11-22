use super::*;
use crate::{LocalCollaboration, LocalFuture};
use collaboration_protocol::{
    ConversationCancelRequest, ConversationCreateRequest, ConversationLoadRequest,
    ConversationOperationSubmission, ConversationOperationWaitRequest,
    ConversationOperationWaitResult, ConversationPromptRequest, EndpointInventory, UuidIdentity,
};
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

const SERVICE_ID: &str = "00000000-0000-7000-8000-000000000001";
const SERVICE_EPOCH: &str = "00000000-0000-7000-8000-000000000002";

struct ScriptedWatch {
    requests: Mutex<Vec<BoundedObservationRequest>>,
    restart: bool,
}

fn unexpected<T>() -> LocalFuture<'static, T> {
    Box::pin(async { Err(ClientError::Protocol("unexpected conversation operation")) })
}

impl LocalCollaboration for ScriptedWatch {
    fn service_id(&self) -> UuidIdentity {
        SERVICE_ID.to_owned().try_into().unwrap()
    }

    fn endpoints(&self) -> Result<EndpointInventory, ClientError> {
        serde_json::from_value(json!({"sequence":1,"serviceEpoch":SERVICE_EPOCH,
            "endpoints":[{"endpoint":{"serviceId":SERVICE_ID,"endpointId":"claude-local"},
            "label":"Fixture","availability":{"state":"available","observedAt":"2026-11-21T00:00:00Z"},
            "channels":[{"kind":"externalProvider","transport":"stdioAcp",
              "bindingId":"fixture","bindingGeneration":1,
              "runtime":{"provider":"claudeCode","runtimeName":"fixture"},
              "capabilities":[{"name":"create","status":"supported","evidence":"advertised"}]}]}]
        })).map_err(|_| ClientError::Protocol("invalid scripted endpoint inventory"))
    }

    fn create_provider_conversation(
        &self,
        _: ConversationCreateRequest,
    ) -> LocalFuture<'_, ConversationOperationSubmission> {
        unexpected()
    }
    fn load_provider_conversation(
        &self,
        _: ConversationLoadRequest,
    ) -> LocalFuture<'_, ConversationOperationSubmission> {
        unexpected()
    }
    fn prompt_provider_conversation(
        &self,
        _: ConversationPromptRequest,
    ) -> LocalFuture<'_, ConversationOperationSubmission> {
        unexpected()
    }
    fn cancel_provider_conversation_operation(
        &self,
        _: ConversationCancelRequest,
    ) -> LocalFuture<'_, ConversationOperationSubmission> {
        unexpected()
    }
    fn wait_for_provider_conversation_operation(
        &self,
        _: ConversationOperationWaitRequest,
    ) -> LocalFuture<'_, ConversationOperationWaitResult> {
        unexpected()
    }

    fn observe_provider_session(
        &self,
        request: BoundedObservationRequest,
        _: Option<ObservationEventSink>,
    ) -> LocalFuture<'_, BoundedObservationResult> {
        Box::pin(async move {
            let mut requests = self.requests.lock().unwrap();
            let (events, end_reason) = if requests.is_empty() {
                assert!(request.after_sequence.is_none() && request.service_epoch.is_none());
                (
                    vec![json!({"kind":"snapshotEnd","cursor":{"epoch":7,"afterSequence":0}})],
                    ObservationEndReason::DeadlineReached,
                )
            } else {
                assert_eq!(request.after_sequence, Some(0));
                assert_eq!(request.epoch, Some(7));
                assert_eq!(
                    String::from(request.service_epoch.clone().unwrap()),
                    SERVICE_EPOCH
                );
                if self.restart {
                    (
                        vec![json!({"kind":"resyncRequired"})],
                        ObservationEndReason::ResyncRequired,
                    )
                } else {
                    (
                        vec![
                            json!({"sequence":1,"event":{"kind":"itemCompleted","itemId":"first"}}),
                        ],
                        ObservationEndReason::ResultLimitReached,
                    )
                }
            };
            let target = request.target.clone();
            requests.push(request);
            let service_epoch = if requests.len() > 1 && self.restart {
                "00000000-0000-7000-8000-000000000003"
            } else {
                SERVICE_EPOCH
            };
            Ok(BoundedObservationResult {
                target,
                generation: serde_json::from_value(
                    json!({"serviceEpoch":service_epoch,"generation":1}),
                )
                .unwrap(),
                attached: true,
                events,
                end_reason,
                continuation_gap: true,
                epoch: Some(7),
            })
        })
    }
}

// Rejects a follower that resumes only from numbered records or omits the original Host's identity.
#[tokio::test]
async fn a_marker_only_call_supplies_the_guarded_follow_position() {
    for restart in [false, true] {
        let router = Arc::new(ScriptedWatch {
            requests: Default::default(),
            restart,
        });
        let access = CollaborationAccess::local(Path::new("."), router.clone());
        let mut follower = SessionObservation::attach_by_ids_with_context(
            &access,
            "claude-local".to_owned().try_into().unwrap(),
            "watch-fixture".to_owned().try_into().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            follower.next_message().await.unwrap()["kind"],
            "snapshotEnd"
        );

        let next = follower.next_message().await.unwrap();

        if restart {
            assert_eq!(next, json!({"kind":"resyncRequired"}));
            assert!(follower.next_message().await.is_err());
        } else {
            assert_eq!(next["sequence"], 1);
        }
        assert_eq!(router.requests.lock().unwrap().len(), 2);
    }
}
