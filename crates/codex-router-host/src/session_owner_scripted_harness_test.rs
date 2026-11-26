use crate::{
    session_owner_calls::SessionOwnerCalls,
    session_owner_execution::SessionOwnerExecution,
    session_owner_lifecycle_reports::SessionOwnerLifecycleReports,
    session_owner_publication::SessionOwnerPublisher,
    session_owner_storage::SessionOwnerStorage,
    session_store_lane::{SessionStoreLane, tests::StoreFixture},
};
use acp_client_runtime::session_owner_task::SessionOwnerTask;
use collaboration_contracts::SessionEventHub;
use collaboration_protocol::{
    ConversationBindingIdentity, OperationId, ProviderIdentity, ProviderOperationKind, SessionRef,
};
use provider_session_machines::{
    harness_lifecycle::{ActivationSetup, HarnessProvider},
    session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia, SessionLifecycle},
    session_owner::{
        CloseOutcome, ConnectionEpoch, OwnerAddress, OwnerIncarnation, PublicationStatus,
        RestoreDisposition, RunAttempt, SessionOwnerCommand, SessionOwnerReply,
    },
};
use session_event_model::{SessionEvent, SessionState, SkillView};
use std::{num::NonZeroUsize, sync::Arc};
use switchboard_session_events::ProviderSessionEventHub;
use switchboard_session_store::ProviderOperationAdmission;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

#[path = "session_owner_scripted_harness_test_support.rs"]
mod script;
use script::{LIVE_TEXT, REPLAY_TEXTS, SCRIPTED_SESSION, ScriptObservation, ScriptedHarness};

#[tokio::test]
async fn scripted_restore_replays_history_before_activation_through_the_production_executor() {
    let fixture = StoreFixture::new().await;
    let target = fixture.record_session(SCRIPTED_SESSION).await;
    let board = ProviderIdentity::session_to_board(&target).expect("board session identity");
    let tasks = TaskTracker::new();
    let stop = CancellationToken::new();
    let (script, mut journal) = ScriptedHarness::new(tasks.clone());
    let address = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let capacity = NonZeroUsize::new(16).expect("positive mailbox capacity");
    let (owner_task, handle) =
        SessionOwnerTask::new(address, fixture.clock.clone(), capacity, capacity);
    let observations = Arc::new(SessionOwnerLifecycleReports::new(
        address,
        &handle,
        stop.clone(),
    ));
    let binding = script.bind(address, observations.clone());
    let (lane, lane_task) = SessionStoreLane::new(
        address.incarnation,
        target.clone(),
        fixture.store.clone(),
        fixture.clock.clone(),
    );
    tasks.spawn(lane_task.run_tracked(stop.clone(), tasks.clone()));
    let hub = Arc::new(ProviderSessionEventHub::new(
        fixture.store.clone(),
        fixture.clock.clone(),
    ));
    let (inbox, inbox_task) = hub.owner_inbox(board.clone(), capacity);
    inbox
        .install_incarnation(None, address.incarnation.0)
        .expect("publish for the exact owner");
    tasks.spawn(inbox_task.expect("new hub inbox worker").run(stop.clone()));
    let calls = Arc::new(SessionOwnerCalls::default());
    let execution = SessionOwnerExecution::new(
        target.clone(),
        binding,
        observations,
        SessionOwnerStorage::new(lane, tasks.clone()),
        SessionOwnerPublisher::new(inbox, fixture.clock.clone(), tasks.clone()),
        calls.clone(),
        Default::default(),
    );

    let operation =
        record_operation(&fixture, &target, ProviderOperationKind::ConversationLoad).await;
    let owner = tasks.spawn(owner_task.run(execution, stop.clone()));
    let receipt = calls.expect_settlement(operation);
    let attempt = LoadAttempt(operation);
    handle
        .try_submit(
            address,
            SessionOwnerCommand::Restore {
                attempt,
                via: RestoreVia::Load,
            },
        )
        .expect("submit load")
        .await
        .expect("load acknowledgement")
        .expect("load admission");
    let settlement = receipt.await.expect("durable load settlement");

    assert!(settlement.saved);
    let SessionOwnerReply::RestoreFinished(restored) = settlement.reply else {
        panic!("load did not finish through the owner");
    };
    assert!(matches!(
        restored.disposition,
        RestoreDisposition::Restored { .. }
    ));
    let SessionLifecycle::Live { generation } = handle.snapshots().borrow().lifecycle else {
        panic!("restored owner did not activate");
    };
    let run = RunAttempt {
        generation,
        attempt,
    };
    for expected in [
        ScriptObservation::Bound(address),
        ScriptObservation::Replayed(run),
        ScriptObservation::Activated(ActivationSetup::Restored(run)),
        ScriptObservation::SkillsReported,
        ScriptObservation::LiveReported,
    ] {
        assert_eq!(journal.recv().await, Some(expected));
    }

    let mut attachment = hub
        .attach(board.clone())
        .await
        .expect("attach to scripted history");
    let mut observed = Vec::new();
    for event in attachment.snapshot {
        observe(&mut observed, event.event);
    }
    while observed.last() != Some(&Observed::Item(LIVE_TEXT.to_owned())) {
        let event = attachment
            .receiver
            .recv()
            .await
            .expect("live scripted observation");
        observe(&mut observed, event.event);
    }
    assert_eq!(
        observed,
        [
            Observed::Item(REPLAY_TEXTS[0].into()),
            Observed::Item(REPLAY_TEXTS[1].into()),
            Observed::Idle,
            Observed::Item(LIVE_TEXT.into()),
        ],
    );
    let skills = hub
        .skill_view(
            board.clone(),
            fixture.path.parent().expect("session cwd").to_owned(),
        )
        .await
        .expect("attach to scripted skill view");
    assert!(matches!(skills.snapshot, SkillView::Available { .. }));

    let close = record_operation(&fixture, &target, ProviderOperationKind::ConversationClose).await;
    let close_receipt = calls.expect_settlement(close);
    let closing = RunAttempt {
        generation,
        attempt: CloseAttempt(close),
    };
    handle
        .try_submit(
            address,
            SessionOwnerCommand::Close {
                generation,
                attempt: closing.attempt,
            },
        )
        .expect("submit close")
        .await
        .expect("close acknowledgement")
        .expect("close admission");
    assert!(matches!(
        close_receipt.await.expect("durable close settlement").reply,
        SessionOwnerReply::CloseFinished {
            outcome: CloseOutcome::Confirmed,
            ..
        }
    ));
    assert_eq!(
        journal.recv().await,
        Some(ScriptObservation::Closed(closing))
    );
    assert_eq!(
        journal.recv().await,
        Some(ScriptObservation::Retired(generation))
    );
    handle
        .snapshots()
        .wait_for(|snapshot| {
            snapshot.projection.state == SessionState::Closed
                && snapshot.publication == PublicationStatus::Current
        })
        .await
        .expect("published closed state");
    assert_eq!(
        hub.state(board).await.expect("closed hub projection"),
        SessionState::Closed
    );

    stop.cancel();
    owner
        .await
        .expect("join the owner task")
        .expect("owner exits normally");
    tasks.close();
    tasks.wait().await;
}

async fn record_operation(
    fixture: &StoreFixture,
    target: &SessionRef,
    operation_kind: ProviderOperationKind,
) -> Uuid {
    let operation = Uuid::now_v7();
    let operation_id = OperationId::try_from(operation.to_string()).expect("operation ID");
    let mut store = fixture.store.lock().await;
    store
        .admit(ProviderOperationAdmission {
            operation_id: operation_id.clone(),
            operation_kind,
            binding: ConversationBindingIdentity::ExternalProvider {
                binding: fixture.binding.clone(),
            },
            admitted_at_ms: 0,
        })
        .await
        .expect("record operation admission");
    store
        .record_target(&operation_id, target, 0)
        .await
        .expect("record operation target");
    operation
}

#[derive(Debug, Eq, PartialEq)]
enum Observed {
    Item(String),
    Idle,
}

fn observe(log: &mut Vec<Observed>, event: SessionEvent) {
    match event {
        SessionEvent::ItemStarted { item } => {
            log.push(Observed::Item(
                item.text.expect("fixture item carries text"),
            ));
        }
        SessionEvent::StateChanged {
            state: SessionState::Idle,
        } => log.push(Observed::Idle),
        _ => {}
    }
}
