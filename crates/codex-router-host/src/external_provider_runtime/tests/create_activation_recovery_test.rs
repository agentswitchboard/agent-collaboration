//! Recover a provider-created session whose local activation was refused.

use super::*;
use crate::{
    ExternalProviderSupervisor, session_owner_door::SessionOwnerDoor,
    session_owner_scripted_harness_test::harness_refusing_first_activation,
};
use collaboration_contracts::ProviderConversationBackend;
use collaboration_protocol::{
    ConversationCreateRequest, ConversationLoadRequest, ConversationOperationFailureKind,
    ConversationOperationFailureStage, ConversationOperationSettlement,
    ConversationOperationWaitOutput, ConversationOperationWaitRequest, OperationId,
    PositiveSeconds, ProviderIdentity, ProviderOperationEffect, ProviderOperationStage,
    ProviderRequestedPolicy, ProviderRequestedSettings, ProviderWorkingDirectory, RouterAccess,
    SessionId, SessionRef,
};
use provider_session_machines::session_lifecycle_machine::SessionLifecycle;
use switchboard_session_store::ProviderOperationStore;

#[tokio::test]
async fn a_refused_create_activation_keeps_the_applied_session_for_public_restore() {
    let HubRuntime {
        runtime,
        hub,
        hub_target,
        _root: root,
        ..
    } = hub_runtime().await;
    let runtime = Arc::try_unwrap(runtime).expect("fixture runtime is exclusively owned");
    let clock = runtime.client.runtime_clock();
    let store = hub.store_for_fixture();
    let endpoint =
        serde_json::from_value(serde_json::to_value(hub_target.endpoint).expect("board endpoint"))
            .expect("provider endpoint");
    let binding = collaboration_protocol::ProviderBindingIdentity {
        endpoint,
        ..crate::session_store_lane::tests::mock_binding()
    };
    let target = SessionRef {
        endpoint: binding.endpoint.clone(),
        session_id: SessionId::try_from("scripted-session".to_owned())
            .expect("scripted session ID"),
    };
    let board = ProviderIdentity::session_to_board(&target).expect("board session");
    let actor = ProviderIdentity::from(target.clone());
    let directory = ProviderWorkingDirectory::try_from(root.path().display().to_string())
        .expect("working directory");
    let policy = ProviderRequestedPolicy {
        access: RouterAccess::WriteRestricted,
    };
    let door = Arc::new(SessionOwnerDoor::new(
        None,
        harness_refusing_first_activation,
        clock.clone(),
        store.clone(),
        hub.clone(),
        runtime.retirement(),
        Default::default(),
    ));
    assert!(runtime.owner_door.set(door.clone()).is_ok());
    let host = ExternalProviderSupervisor::with_fixture_hub(
        vec![runtime.into_binding(binding.clone())],
        store,
        hub.clone(),
        clock,
    )
    .expect("compose the production supervisor with the scripted harness");
    let wait = |operation_id| ConversationOperationWaitRequest {
        operation_id,
        timeout_seconds: PositiveSeconds::try_from(30).expect("wait budget"),
    };
    let create = OperationId::generate();
    host.create(ConversationCreateRequest {
        operation_id: create.clone(),
        endpoint: binding.endpoint,
        generation: None,
        working_directory: directory.clone(),
        created_by: actor.clone(),
        approver: actor.clone(),
        requested_policy: policy.clone(),
        settings: Some(ProviderRequestedSettings {
            mode: Some("auto".to_owned()),
            ..Default::default()
        }),
        work_roots: Vec::new(),
    })
    .await
    .expect("the public create was admitted");
    let refused = host
        .wait(wait(create.clone()))
        .await
        .expect_err("a refused activation must not succeed as a public create");
    assert_eq!(refused.kind, ConversationOperationFailureKind::Unavailable);
    assert_eq!(refused.stage, ConversationOperationFailureStage::Settlement);
    assert_eq!(refused.effect, ProviderOperationEffect::Applied);
    assert_eq!(refused.operation_id, Some(create.clone()));
    assert_eq!(
        refused.target,
        Some(target.clone()),
        "the existing failure answer names the known created session"
    );
    assert!(String::from(refused.message).contains("could not activate it"));
    let owner = door
        .existing(&target)
        .expect("the adopted owner remains registered");
    let failed = owner.snapshots().borrow().clone();
    assert!(matches!(
        failed.lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    let history = hub
        .attach(board)
        .await
        .expect("the saved session is attachable");
    assert!(
        !history.snapshot.iter().any(|event| matches!(
            event.event,
            SessionEvent::StateChanged {
                state: SessionState::Idle
            }
        )),
        "a refused create activation must not publish Live"
    );

    let mut persisted = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("reopen the owning SQLite store");
    let saved = persisted
        .session_record(&target)
        .await
        .expect("read saved session")
        .expect("applied create kept its session");
    assert_eq!(saved.target, target);
    assert_eq!(saved.working_directory, directory);
    let operation = persisted
        .inspect(&create)
        .await
        .expect("read original operation")
        .expect("attempt recorded");
    assert_eq!(operation.stage, ProviderOperationStage::Terminal);
    assert_eq!(operation.effect, ProviderOperationEffect::Applied);
    drop(persisted);

    let load = OperationId::generate();
    host.load(ConversationLoadRequest {
        operation_id: load.clone(),
        target: target.clone(),
        generation: Some(binding.generation),
        working_directory: directory,
        requested_by: actor.clone(),
        approver: actor,
        requested_policy: policy,
    })
    .await
    .expect("public restore admits the saved session");
    let restored = host.wait(wait(load)).await.expect("later restore succeeds");
    assert!(
        matches!(restored.output, ConversationOperationWaitOutput::Available {
        settlement: ConversationOperationSettlement::Loaded { target: restored, .. },
    } if restored == target)
    );
    assert!(Arc::ptr_eq(
        &owner,
        &door.existing(&target).expect("same owner")
    ));
    let live = owner.snapshots().borrow().clone();
    assert_eq!(live.owner, failed.owner);
    assert!(matches!(live.lifecycle, SessionLifecycle::Live { .. }));
    host.shutdown().await.expect("stop the fixture Host");
}
