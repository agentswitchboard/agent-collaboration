use super::*;
use crate::external_provider_runtime::acp_scripted_fixture::{launch_fixture_agent, manual_clock};
use crate::session_owner_calls::OwnedSettlement;
use acp_client_runtime::ProviderTurnInput;
use collaboration_protocol::SessionId;
use provider_session_machines::session_owner::{
    PromptCompletion, ProviderWireFailure, ProviderWireFailureKind,
};
use session_event_model::{InputId, LocalCause, PromptContent, ProviderSubmission, StopReason};
use std::path::PathBuf;
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

fn test_owner_input() -> OwnerInput {
    OwnerInput {
        generation: SessionGeneration::from_number(1),
        operation: Uuid::now_v7(),
        input: InputId::generate(),
        turn: TurnId(Uuid::now_v7()),
    }
}

async fn fixture_backend(path: &std::path::Path) -> (ExternalProviderSupervisor, SessionRef) {
    let launch = launch_fixture_agent("host-supervisor-agent.ts", vec!["create".to_owned()]);
    let clock = manual_clock();
    let runtime = crate::ExternalProviderRuntime::initialize(
        launch,
        Arc::new(acp_provider_adapters::StandardAcpBehavior),
        clock.clone(),
    )
    .await
    .expect("ACP fixture initialization");
    let binding = super::super::tests::binding();
    let target = SessionRef {
        endpoint: binding.endpoint.clone(),
        session_id: SessionId::try_from("fixture-session".to_owned()).expect("fixture session"),
    };
    let store = ProviderOperationStore::open(path)
        .await
        .expect("operation store");
    let supervisor = ExternalProviderSupervisor::new(
        vec![runtime.into_binding(binding)],
        Arc::new(Mutex::new(store)),
        clock,
    )
    .expect("supervisor");
    supervisor
        .open_provider_fixture(&target.endpoint, PathBuf::from("/tmp"))
        .await
        .expect("open the real owner connection");
    (supervisor, target)
}

#[tokio::test]
async fn unsupported_media_keeps_the_same_failure_through_prompt_and_delivery() {
    let root = tempfile::tempdir().expect("fixture root");
    let (supervisor, target) = fixture_backend(&root.path().join("operations.sqlite")).await;
    let request = || ProviderPromptContentsRequest {
        operation_id: OperationId::generate(),
        input_id: InputId::generate(),
        target: target.clone(),
        requested_by: target.clone().into(),
        approver: target.clone().into(),
        input: ProviderTurnInput::Prompt(ProviderSubmission::plain(vec![
            PromptContent::image("image/png".to_owned(), "aGVsbG8=".to_owned(), None)
                .expect("valid image block"),
        ])),
        busy: PromptBusyPolicy::RefuseBusy,
    };

    let prompt = request();
    let prompt_operation = prompt.operation_id.clone();
    let prompt_failure = supervisor
        .submit_owner_prompt(prompt)
        .await
        .expect_err("image refused");
    let delivery = request();
    let delivery_operation = delivery.operation_id.clone();
    let delivery_failure = supervisor
        .submit_owned_delivery(delivery, MessageDelivery::Auto)
        .await
        .expect_err("image refused for delivery");
    for (failure, operation) in [
        (prompt_failure, prompt_operation),
        (delivery_failure, delivery_operation),
    ] {
        assert_eq!(
            (
                failure.kind,
                failure.stage,
                failure.effect,
                failure.provider_code,
                String::from(failure.message)
            ),
            (
                ConversationOperationFailureKind::UnsupportedCapability,
                ConversationOperationFailureStage::Validation,
                ProviderOperationEffect::None,
                None,
                "unsupportedContent{image}".to_owned()
            ),
        );
        assert_eq!(failure.operation_id, Some(operation));
        assert_eq!(failure.target, Some(target.clone()));
    }
    supervisor.shutdown().await.expect("fixture shutdown");
}

#[tokio::test]
async fn retained_prompt_converts_coded_and_local_causes_to_exact_failures() {
    let root = tempfile::tempdir().expect("fixture root");
    let (supervisor, target) = fixture_backend(&root.path().join("operations.sqlite")).await;
    let operation = OperationId::generate();
    let (_, binding) = supervisor
        .runtime_binding(&target.endpoint, &operation, Some(target.clone()))
        .expect("fixture binding");
    let coded = ProviderWireFailure {
        kind: ProviderWireFailureKind::InvalidParams,
        code: -32602,
        correlation_id: uuid::uuid!("01981f2b-d79c-7abc-8000-000000000002"),
    };
    let completed = |local_cause| SessionOwnerReply::PromptCompleted {
        input: test_owner_input(),
        completion: PromptCompletion {
            stop_reason: StopReason::EndTurn,
            local_cause,
            response: String::new(),
        },
    };
    let cases = [
        (
            SessionOwnerReply::PromptFailed {
                input: test_owner_input(),
                remote: RemoteEffect::None,
                failure: coded,
            },
            OperationFailureCause::HarnessCoded(coded),
            ConversationOperationFailureKind::InvalidRequest,
            ConversationOperationFailureStage::Validation,
            ProviderOperationEffect::None,
            Some(-32602),
            "provider ACP parameters are invalid (provider code -32602; reference 01981f2b-d79c-7abc-8000-000000000002)",
        ),
        (
            completed(Some(LocalCause::Other("compactionUnconfirmed".to_owned()))),
            OperationFailureCause::CompactionUnconfirmed,
            ConversationOperationFailureKind::ProviderRejected,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Applied,
            None,
            "explicit compaction was not confirmed",
        ),
        (
            completed(Some(LocalCause::OutputOverflow)),
            OperationFailureCause::PromptOutputLimitExceeded,
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            None,
            "provider prompt output exceeded the retained output limit; cancellation was requested and settled",
        ),
    ];
    for (reply, cause, kind, stage, effect, code, expected) in cases {
        assert_eq!(reported_prompt_failure(&reply), Some(cause));
        let result = retained_prompt(
            binding.connection.as_ref(),
            operation.clone(),
            target.clone(),
            OwnedSettlement {
                reply,
                saved: true,
                turn_approver: None,
            },
        );
        let ProviderOperationCompletion::Owned {
            result: Err(failure),
            target: returned,
            saved: true,
        } = result
        else {
            panic!("expected the retained failure");
        };
        assert_eq!(returned, target);
        assert_eq!(
            (
                failure.kind,
                failure.stage,
                failure.effect,
                failure.provider_code
            ),
            (kind, stage, effect, code)
        );
        assert_eq!(String::from(failure.message), expected);
    }
    assert_eq!(reported_prompt_failure(&completed(None)), None);
    assert_eq!(
        reported_prompt_failure(&completed(Some(LocalCause::Other("other".to_owned())))),
        None
    );
    supervisor.shutdown().await.expect("fixture shutdown");
}

#[test]
fn oversized_prompt_reply_remains_a_settlement_failure() {
    let target = super::super::tests::requester();
    assert_eq!(
        message_size_failure(true),
        Some(OperationFailureCause::MessageTooLarge)
    );
    assert_eq!(message_size_failure(false), None);
    let oversized = unsettled_prompt_failure(OperationId::generate(), &target, true);
    assert_eq!(
        (
            oversized.kind,
            oversized.stage,
            oversized.effect,
            oversized.provider_code,
            String::from(oversized.message)
        ),
        (
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Settlement,
            ProviderOperationEffect::Unknown,
            None,
            "provider frame exceeded the configured transport limit".to_owned()
        ),
    );
    let ordinary = unsettled_prompt_failure(OperationId::generate(), &target, false);
    assert_eq!(
        (
            ordinary.kind,
            ordinary.stage,
            ordinary.effect,
            ordinary.provider_code,
            String::from(ordinary.message)
        ),
        (
            ConversationOperationFailureKind::OutcomeUnknown,
            ConversationOperationFailureStage::Dispatch,
            ProviderOperationEffect::Unknown,
            None,
            "provider dispatch outcome is unknown".to_owned()
        ),
    );
}
