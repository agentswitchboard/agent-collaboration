use super::tests::{binding, requester};
use crate::provider_operation_settlement::persist_provider_success;
use collaboration_protocol::{
    ConversationBindingIdentity, OperationId, ProviderOperationEffect, ProviderOperationKind,
    ProviderOperationStage, ProviderPromptStopReason, ProviderReconciliationState,
};
use router_clock::ManualClock;
use std::sync::Mutex;
use switchboard_session_store::{
    ProviderOperationAdmission, ProviderOperationAdmissionResult, ProviderOperationStore,
};

#[tokio::test]
async fn checked_success_stamps_the_target_and_terminal_at_separate_clock_reads() {
    let directory = tempfile::tempdir().expect("store directory");
    let mut store = ProviderOperationStore::open(&directory.path().join("operations.sqlite"))
        .await
        .expect("open store");
    let operation = OperationId::generate();
    let target = requester();
    let ProviderOperationAdmissionResult::Admitted(admitted) = store
        .admit(ProviderOperationAdmission {
            operation_id: operation.clone(),
            operation_kind: ProviderOperationKind::ConversationPrompt,
            binding: ConversationBindingIdentity::ExternalProvider { binding: binding() },
            admitted_at_ms: 100,
        })
        .await
        .expect("admit operation")
    else {
        panic!("fresh operation must be admitted");
    };
    let target_at = 250;
    let terminal_at = 500;
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(target_at).unwrap());
    let saw_target = Mutex::new(None);
    let observe_target = |record: &switchboard_session_store::ProviderOperationRecord| {
        *saw_target.lock().unwrap() = Some(record.clone());
        clock.set_utc(chrono::DateTime::from_timestamp_millis(terminal_at).unwrap());
    };
    let stop_reason = ProviderPromptStopReason::EndTurn;

    persist_provider_success(
        &mut store,
        &operation,
        Some(&target),
        None,
        Some(stop_reason.clone()),
        &clock,
        Some(&observe_target),
    )
    .await
    .expect("checked settlement");
    let terminal = store
        .inspect(&operation)
        .await
        .expect("inspect settled row");

    let mut expected_target = admitted;
    expected_target.target = Some(target);
    expected_target.updated_at_ms = target_at;
    assert_eq!(
        saw_target.into_inner().unwrap(),
        Some(expected_target.clone()),
        "exact record_target receipt"
    );
    let mut expected_terminal = expected_target;
    expected_terminal.stage = ProviderOperationStage::Terminal;
    expected_terminal.effect = ProviderOperationEffect::Applied;
    expected_terminal.reconciliation_state = ProviderReconciliationState::Confirmed;
    expected_terminal.terminal_stop_reason = Some(stop_reason);
    expected_terminal.terminal_at_ms = Some(terminal_at);
    expected_terminal.updated_at_ms = terminal_at;
    assert_eq!(
        terminal,
        Some(expected_terminal),
        "exact persisted terminal row"
    );
}
