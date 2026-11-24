use super::*;
use acp_client_runtime::session_owner_task::OwnerTaskError;
use collaboration_protocol::{ConversationLoadRequest, ConversationOperationWaitOutput};
use provider_session_machines::{
    session_lifecycle_machine::SessionLifecycle,
    session_owner::{SessionOwnerRefusal, SessionOwnerRejection},
    session_turn_machine::SessionTurn,
};

#[tokio::test]
async fn a_missing_witness_refusal_ends_the_task_and_recovers_the_session() -> FixtureResult<()> {
    recover_from_refusal(SessionOwnerRejection::IgnoredLate).await
}

#[tokio::test]
async fn an_injected_cell_refusal_ends_the_task_and_recovers_the_session() -> FixtureResult<()> {
    recover_from_refusal(SessionOwnerRejection::Refused(SessionOwnerRefusal::Cell(
        state_machine_core::TransitionError::Stale,
    )))
    .await
}

#[tokio::test]
async fn an_injected_entry_effect_refusal_ends_the_task_and_recovers_the_session()
-> FixtureResult<()> {
    recover_from_refusal(SessionOwnerRejection::Refused(
        SessionOwnerRefusal::GenerationChanged,
    ))
    .await
}

async fn recover_from_refusal(refusal: SessionOwnerRejection) -> FixtureResult<()> {
    let HeldNativeTurn {
        _root,
        runtime,
        mut native,
        clock,
        thread,
        target,
        first_operation,
        release_write,
        start_held,
        ..
    } = hold_native_turn_with("reactive-session-1", |root| {
        vec![
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../verification/fixtures/cursor-settings-agent.ts"
            )
            .to_owned(),
            "reactive".to_owned(),
            root.join("deadline-provider-wire.json")
                .display()
                .to_string(),
        ]
    })
    .await?;
    let supervisor = runtime
        .external_provider_supervisor
        .as_deref()
        .ok_or("fixture supervisor")?;
    let snapshot = supervisor
        .session_owner_for(&target)?
        .snapshots()
        .borrow()
        .clone();
    let SessionTurn::Settling(settlement) = snapshot.turn else {
        return Err(format!("expected a current settlement: {snapshot:?}").into());
    };
    let provider = supervisor
        .runtime_for(&target.endpoint)
        .ok_or("fixture provider")?;
    let stopped = provider
        .preparation_fixture
        .refuse_settlement_deadline(settlement, refusal);
    let (replacement_started, finish_recovery) = provider.preparation_fixture.pause_recovery();
    send_next_start(&mut native, &thread).await?;
    start_held.await?;
    advance_to_settlement_deadline(&clock).await?;

    let stopped = stopped.await?;
    if stopped != (OwnerTaskError::TurnSettlementDeadlineRefused { settlement }) {
        return Err(format!("expected the typed deadline task failure: {stopped:?}").into());
    }
    let response = receive_response(&mut native, 2).await?;
    if response.pointer("/error/code").and_then(Value::as_i64) != Some(-32000)
        || response.pointer("/error/data").is_some()
    {
        return Err(
            format!("the held caller did not get the owner-stopped answer: {response}").into(),
        );
    }

    release_write
        .send(())
        .map_err(|()| "terminal writer stopped early")?;
    replacement_started.await?;
    let replacement = supervisor
        .session_owner_for(&target)?
        .snapshots()
        .borrow()
        .clone();
    if replacement.owner == snapshot.owner
        || !matches!(replacement.lifecycle, SessionLifecycle::NotLoaded { .. })
    {
        return Err(format!("expected a replacement owner: {replacement:?}").into());
    }
    finish_recovery
        .send(())
        .map_err(|()| "owner recovery stopped early")?;
    let _previous = supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: first_operation,
            timeout_seconds: PositiveSeconds::try_from(2)?,
        })
        .await;

    let record = runtime
        .provider_store
        .as_ref()
        .ok_or("fixture store")?
        .lock()
        .await
        .session_record(&target)
        .await?
        .ok_or("fixture session record")?;
    let operation_id = OperationId::generate();
    supervisor
        .load(ConversationLoadRequest {
            operation_id: operation_id.clone(),
            target,
            generation: None,
            working_directory: record.working_directory,
            requested_by: record.created_by,
            approver: record.approver,
            requested_policy: record.requested_policy,
        })
        .await
        .map_err(|error| format!("replacement load refused: {error:?}"))?;
    let restored = supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id,
            timeout_seconds: PositiveSeconds::try_from(2)?,
        })
        .await
        .map_err(|error| format!("replacement load failed: {error:?}"))?;
    if !matches!(
        restored.output,
        ConversationOperationWaitOutput::Available { .. }
    ) {
        return Err(format!("replacement did not load: {restored:?}").into());
    }
    send_start(&mut native, 3, &thread).await?;
    require_native_start(&mut native, 3).await?;
    drop(provider);
    native.close(None).await?;
    runtime.shutdown().await?;
    Ok(())
}
