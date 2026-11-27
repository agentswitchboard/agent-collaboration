//! A provider or SDK exit leaves accepted, unsent settings with the outer writer.

use super::*;
use acp_client_runtime::{acp_transport::TransportRun, owner_wire::ProviderConnectionEnd};
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{SettingAttempt, SettingFact},
    session_owner::{OwnerSetting, ReplyAdmission},
    session_setting_machine::{SettingApplication, SettingValueId},
};

#[tokio::test]
async fn queued_setter_survives_provider_process_exit() {
    exact_setter_survives_connection_end(ProviderConnectionEnd::ChildExited).await;
}

#[tokio::test]
async fn queued_setter_survives_sdk_background_error() {
    exact_setter_survives_connection_end(ProviderConnectionEnd::SdkFailed).await;
}

#[tokio::test]
async fn queued_setter_survives_clean_sdk_return() {
    exact_setter_survives_connection_end(ProviderConnectionEnd::SdkReturned).await;
}

async fn exact_setter_survives_connection_end(expected: ProviderConnectionEnd) {
    let behaviours: &[&str] = match expected {
        ProviderConnectionEnd::ChildExited => &["exit-on-stop-file"],
        ProviderConnectionEnd::SdkFailed => &["invalid-utf8-on-release"],
        ProviderConnectionEnd::SdkReturned => &[],
    };
    let fixture = hub_runtime_with(behaviours).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let restore = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) = load_fixture_binding(
        &fixture,
        &mut binding,
        owner,
        restore,
        CancellationToken::new(),
    )
    .await;
    let wire = fixture.runtime.client.owner_wire();
    let paused_writer = wire
        .hold_ordered_writer_for_test(TransportRun {
            incarnation: owner.incarnation,
            generation: restore.generation,
        })
        .expect("queue a barrier before the writer's next dequeue");
    paused_writer
        .before_dequeue
        .reached
        .await
        .expect("writer acknowledges the dequeue barrier");
    let setting = OwnerSetting {
        change: RunAttempt {
            generation: restore.generation,
            attempt: SettingApplication(Uuid::now_v7()),
        },
        kind: ProviderSettingKind::Mode,
    };
    let (reply, mut original) = oneshot::channel();
    let mut reply = Some(reply);
    binding
        .put_setting_back(
            SettingAttempt {
                restore,
                setting,
                value: SettingValueId("ask".into()),
                catalogue: readback.catalogue,
            },
            &mut || {
                ReplyAdmission::Reserved(Box::new(Reply(
                    reply.take().expect("one exact reservation"),
                )))
            },
        )
        .expect("the real binding's setting was taken by the ordered channel");
    assert!(matches!(
        original.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    // Keeping EOF unpublished gives the process-status arm sole authority
    // to end the SDK on child exit. The malformed line is a separate error
    // on the live child's ordinary stdout.
    let eof =
        (expected == ProviderConnectionEnd::ChildExited).then(|| wire.hold_provider_eof_for_test());
    let ended = wire.observe_provider_end_for_test();
    let observation = wire.observe_writer_exit_for_test();
    let mut reap_started = observation.reap_started;
    let (shutdown, outer_settled) = fixture.runtime.client.connection_fixture_signals();
    match expected {
        ProviderConnectionEnd::ChildExited => {
            node_test_fixture::release_fixture(&fixture.receipt, ".stop")
                .expect("request the actual provider process to exit");
        }
        ProviderConnectionEnd::SdkFailed => {
            node_test_fixture::release_fixture(&fixture.receipt, ".release-invalid-utf8")
                .expect("send the actual invalid stdout line from the provider");
        }
        ProviderConnectionEnd::SdkReturned => shutdown.cancel(),
    };
    assert_eq!(
        ended
            .await
            .expect("the outer connection task dropped its SDK future"),
        expected
    );
    tokio::select! {
        biased;
        _ = &mut reap_started => panic!("outer connection must join the pending writer before entering provider reap"),
        () = outer_settled.cancelled() => panic!("outer connection must join the pending writer before entering provider reap or completing"),
        waiting = observation.join_pending => {
            waiting.expect("the outer task polled its actual writer JoinHandle to Pending");
        }
    }
    paused_writer
        .before_dequeue
        .release
        .send(())
        .expect("let the writer begin shutdown");
    paused_writer
        .during_drain
        .reached
        .await
        .expect("the closed receiver is holding the drain");
    assert_eq!(
        reap_started.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    assert!(
        !outer_settled.is_cancelled(),
        "the outer task is still waiting for the held drain"
    );
    assert!(matches!(
        original.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    paused_writer
        .during_drain
        .release
        .send(())
        .expect("let the real writer settle the admitted frame");
    let fact = tokio::select! {
        biased;
        received = &mut original => {
            let fact = received.expect("the real writer must settle the exact queued setter after SDK exit");
            reap_started.await.expect("the outer task enters provider cleanup after its writer");
            fact
        }
        reaping = &mut reap_started => {
            reaping.expect("the outer task enters provider cleanup");
            original.try_recv().unwrap_or_else(|_| panic!("the writer's exact reply must already be settled at provider reap"))
        }
    };
    assert!(
        matches!(fact, SettingFact::NotSent { restore: exact, setting: application }
        if exact == restore && application == setting)
    );
    drop(eof);
    outer_settled.cancelled().await;
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        0
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/set_mode"), 0);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
