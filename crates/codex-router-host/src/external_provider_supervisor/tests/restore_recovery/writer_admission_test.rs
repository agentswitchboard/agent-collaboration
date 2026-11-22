use super::*;
use collaboration_protocol::ConversationOperationFailureStage;

const RETRY_RESTORE: &str = "the Router couldn't send the restore, so retry this session";

#[tokio::test]
async fn a_full_provider_writer_leaves_explicit_restore_known_unsent() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("restored-in-auto").await;
    let owner = host
        .session_owner_for(&rig.target)
        .expect("registered session");
    let held_writer = owner.hold_provider_writer_capacity();
    let operation = OperationId::generate();

    host.load(rig.load_request(operation.clone()))
        .await
        .expect("Host admission");
    let failure = host
        .wait(wait_request(operation.clone()))
        .await
        .expect_err("writer rejects restore");
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    assert_eq!(failure.stage, ConversationOperationFailureStage::Admission);
    assert_eq!(String::from(failure.message), RETRY_RESTORE);
    let row = host
        .show(ConversationOperationShowRequest {
            operation_id: operation,
        })
        .await
        .expect("durable restore row");
    assert_eq!(row.stage, ProviderOperationStage::Terminal);
    assert_eq!(row.effect, ProviderOperationEffect::None);
    assert!(
        !rig.root.path().join("wire.json").exists(),
        "no restore reached ACP"
    );

    drop(held_writer);
    rig.assert_loads(&host).await;
    host.shutdown().await.expect("fixture shutdown");
}
