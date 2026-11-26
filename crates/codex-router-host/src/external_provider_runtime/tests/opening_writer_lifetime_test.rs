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

async fn exact_setter_survives_connection_end(expected: ProviderConnectionEnd) {
    let behaviour = match expected {
        ProviderConnectionEnd::ChildExited => "exit-on-stop-file",
        ProviderConnectionEnd::SdkFailed => "invalid-utf8-on-release",
        ProviderConnectionEnd::SdkReturned => panic!("this test only drives the exceptional exits"),
    };
    let fixture = hub_runtime_with(&[behaviour]).await;
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
    let release = if expected == ProviderConnectionEnd::ChildExited {
        ".stop"
    } else {
        ".release-invalid-utf8"
    };
    node_test_fixture::release_fixture(&fixture.receipt, release)
        .expect("release the selected real exit");
    assert_eq!(
        ended
            .await
            .expect("the outer connection task dropped its SDK future"),
        expected
    );
    let (_, outer_settled) = fixture.runtime.client.connection_fixture_signals();
    let settled_while_held = outer_settled.is_cancelled();
    let _writer_already_gone = paused_writer.release.send(());
    let fact = original
        .await
        .expect("the real writer must settle the exact queued setter after SDK exit");
    assert!(
        matches!(fact, SettingFact::NotSent { restore: exact, setting: application }
        if exact == restore && application == setting)
    );
    assert!(
        !settled_while_held,
        "the outer connection task waits for the writer drain"
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
