//! Rule out a fabricated refusal after each observed connection end.

#[path = "taken_reply_calls.rs"]
mod calls;
#[path = "prompt_terminal_custody_test.rs"]
mod terminal_custody;

use super::*;
use acp_client_runtime::{
    acp_transport::TransportRun,
    owner_wire::{FixtureDispatchPoint, ProviderConnectionEnd, TransportEnd},
};
use calls::{ExpectedReply, TakenKind, take_call, take_work};
use provider_session_machines::session_turn_machine::TurnId;
use tokio::{
    io::{AsyncBufReadExt as _, BufReader},
    net::{UnixListener, UnixStream},
};

#[derive(Clone, Copy, Debug)]
enum ExitCause {
    Child,
    Input,
    Output,
    Shutdown,
    Callback,
}

#[derive(Clone, Copy)]
enum Scenario {
    Exit(ExitCause),
    LiveRefusal,
    BufferedSuccess,
}

#[tokio::test]
async fn taken_requests_stay_unknown_on_actual_child_exit() {
    assert_all_requests_on_exit(ExitCause::Child).await;
}

#[tokio::test]
async fn taken_requests_stay_unknown_on_actual_input_error() {
    assert_all_requests_on_exit(ExitCause::Input).await;
}

#[tokio::test]
async fn taken_requests_stay_unknown_on_a_later_output_error() {
    assert_all_requests_on_exit(ExitCause::Output).await;
}

#[tokio::test]
async fn taken_requests_stay_unknown_on_explicit_sdk_shutdown() {
    assert_all_requests_on_exit(ExitCause::Shutdown).await;
}

#[tokio::test]
async fn taken_requests_stay_unknown_on_the_fatal_callback_bridge() {
    assert_all_requests_on_exit(ExitCause::Callback).await;
}

#[tokio::test]
async fn taken_requests_preserve_live_peer_refusals() {
    for kind in TakenKind::ALL {
        assert_taken_reply(kind, Scenario::LiveRefusal).await;
    }
}

#[tokio::test]
async fn buffered_work_close_and_setter_successes_beat_the_writer_stop() {
    for kind in [
        TakenKind::Prompt,
        TakenKind::Steer,
        TakenKind::WorkSetting,
        TakenKind::Close,
        TakenKind::RestoreMode,
        TakenKind::RestoreFastOff,
    ] {
        assert_taken_reply(kind, Scenario::BufferedSuccess).await;
    }
}

#[tokio::test]
async fn a_buffered_create_success_names_the_created_session_after_writer_stop() {
    assert_taken_reply(TakenKind::Create, Scenario::BufferedSuccess).await;
}

#[tokio::test]
async fn a_buffered_load_success_keeps_the_peer_acknowledgement_after_writer_stop() {
    assert_taken_reply(TakenKind::Load, Scenario::BufferedSuccess).await;
}

#[tokio::test]
async fn a_buffered_resume_success_keeps_the_peer_acknowledgement_after_writer_stop() {
    assert_taken_reply(TakenKind::Resume, Scenario::BufferedSuccess).await;
}

async fn assert_all_requests_on_exit(cause: ExitCause) {
    for kind in TakenKind::ALL {
        assert_taken_reply(kind, Scenario::Exit(cause)).await;
    }
}

async fn notice(listener: &UnixListener) -> (String, UnixStream) {
    let (connection, _) = listener
        .accept()
        .await
        .expect("accept the peer's exact notice");
    let mut stream = BufReader::new(connection);
    let mut line = String::new();
    assert!(
        stream
            .read_line(&mut line)
            .await
            .expect("read the peer's notice")
            > 0
    );
    (line.trim_end().to_owned(), stream.into_inner())
}

async fn taken_reply_fixture(kind: TakenKind) -> (HubRuntime, UnixListener) {
    let mut listener = None;
    let fixture = hub_runtime_with_launch(|receipt| {
        let socket = receipt.with_extension("ack.sock");
        listener = Some(UnixListener::bind(&socket).expect("bind peer notice socket"));
        crate::external_provider_runtime::acp_scripted_fixture::launch_fixture_agent(
            "acp-taken-reply-agent.ts",
            [
                receipt.display().to_string(),
                socket.display().to_string(),
                kind.method().into(),
            ]
            .into(),
        )
    })
    .await;
    (
        fixture,
        listener.expect("the peer uses the exact notice socket"),
    )
}

async fn assert_taken_reply(kind: TakenKind, scenario: Scenario) {
    let (fixture, listener) = taken_reply_fixture(kind).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let catalogue = if kind.needs_saved_session() {
        let (readback, _) =
            load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new())
                .await;
        if kind.needs_live_reader() {
            let accepted = AcceptedRunSettings {
                catalogue: Arc::clone(&readback.catalogue),
                router_auto_requested: false,
            };
            binding
                .accept_settings(OpenAttempt::Restore(run), &accepted)
                .expect("prepare the actual reader");
            binding
                .activate(
                    ActivationSetup::Restored(run),
                    accepted.clone(),
                    Arc::new(AcceptEvents),
                    Box::pin(std::future::pending()),
                )
                .expect("the live work route is active");
            binding.use_settings(run.generation, accepted);
        }
        Some(readback.catalogue)
    } else {
        None
    };
    let turn = TurnId(Uuid::now_v7());
    let warm_prompt = if kind == TakenKind::Steer {
        let pending = take_work(&binding, owner, run.generation, TakenKind::Prompt, turn);
        assert_eq!(notice(&listener).await.0, "warm-prompt");
        Some(pending)
    } else {
        None
    };
    let wire = fixture.runtime.client.owner_wire();
    let buffered = matches!(scenario, Scenario::BufferedSuccess);
    if buffered && matches!(kind, TakenKind::Load | TakenKind::Resume) {
        wire.buffer_next_restore_success_for_test();
    }
    let waiting = wire.hold_taken_reply_for_test();
    let reply = take_call(&fixture, &mut binding, owner, run, kind, catalogue, turn);
    let sent: serde_json::Value =
        serde_json::from_str(&notice(&listener).await.0).expect("exact peer receipt");
    assert_eq!(sent["method"], kind.method());
    assert!(
        !sent["id"].is_null(),
        "the peer read an exact JSON-RPC request id"
    );
    if buffered {
        if kind == TakenKind::Prompt {
            assert_eq!(
                wire.current_turn(TransportRun {
                    incarnation: owner.incarnation,
                    generation: run.generation
                }),
                Some(turn),
                "the exact prompt owns its live turn before publishing stop",
            );
        }
        node_test_fixture::release_fixture(&fixture.receipt, ".confirm")
            .expect("release the real success");
        assert_eq!(notice(&listener).await.0, "reply-buffered");
    }
    waiting
        .reached
        .await
        .expect("the real writer owns the response wait");

    if buffered {
        // Mark the actual writer as stopping while the SDK remains live.
        // The later fence proves it can still attach the created session.
        fixture.runtime.retirement().cancel();
        node_test_fixture::release_fixture(&fixture.receipt, ".after-stop-fence")
            .expect("issue a real peer fence after publishing the writer stop");
        assert_eq!(notice(&listener).await.0, "sdk-live-after-stop");
        assert_eq!(
            binding.opening_is_held(OpenAttempt::Restore(run)),
            Held::NotHeld(LifecycleRefusal::ConnectionRetired),
        );
        let mut binding = Some(binding);
        if kind == TakenKind::Prompt {
            // The reply will use its original terminal envelope once the
            // reader has actually released its receive end.
            drop(binding.take());
            tasks.close();
            tasks.wait().await;
        }
        waiting
            .release
            .send(())
            .expect("let the actual selection see both ready arms");
        reply.assert(kind, ExpectedReply::BufferedSuccess).await;
        drop(binding);
        fixture.runtime.shutdown().await;
        drop(warm_prompt);
        tasks.close();
        tasks.wait().await;
        return;
    }

    if let Scenario::LiveRefusal = scenario {
        node_test_fixture::release_fixture(&fixture.receipt, ".refuse")
            .expect("release a real peer refusal");
        waiting.release.send(()).expect("let the live reply settle");
        reply.assert(kind, ExpectedReply::PeerRefusal).await;
        assert!(
            !fixture.runtime.retirement().is_cancelled(),
            "peer refusal keeps the connection live"
        );
        drop(binding);
        fixture.runtime.shutdown().await;
        drop(warm_prompt);
        tasks.close();
        tasks.wait().await;
        return;
    }

    let Scenario::Exit(cause) = scenario else {
        panic!("the successful and live cases are already handled")
    };
    let before_fallback = wire.hold_sdk_drop_before_retirement_for_test();
    let provider_end = wire.observe_provider_end_for_test();
    let eof = matches!(cause, ExitCause::Child).then(|| wire.hold_provider_eof_for_test());
    let transport_end = matches!(cause, ExitCause::Input | ExitCause::Output)
        .then(|| wire.observe_transport_end_for_test());
    let mut child_keepalive = None;
    match cause {
        ExitCause::Child => node_test_fixture::release_fixture(&fixture.receipt, ".child-exit")
            .expect("exit the actual provider"),
        ExitCause::Input => node_test_fixture::release_fixture(&fixture.receipt, ".input-error")
            .expect("fail the actual input codec"),
        ExitCause::Output => {
            node_test_fixture::release_fixture(&fixture.receipt, ".output-error")
                .expect("close the peer's actual stdin");
            let (closed, keepalive) = notice(&listener).await;
            assert_eq!(closed, "stdin-closed");
            child_keepalive = Some(keepalive);
            // The target ID already crossed the wire. This later write is
            // the one that fails while the peer preserves stdout.
            let probe = wire.observe_dispatch_for_test(FixtureDispatchPoint::Cancel);
            wire.fence_dispatch_for_test(
                TransportRun {
                    incarnation: owner.incarnation,
                    generation: run.generation,
                },
                FixtureDispatchPoint::Cancel,
            );
            probe
                .await
                .expect("later probe reached the live SDK")
                .expect("SDK accepted the outgoing probe");
        }
        ExitCause::Shutdown => fixture
            .runtime
            .client
            .connection_fixture_signals()
            .0
            .cancel(),
        ExitCause::Callback => wire.trigger_fatal_callback_for_test(),
    }
    if let Some(transport_end) = transport_end {
        assert_eq!(
            transport_end
                .await
                .expect("observe the actual failed I/O half"),
            match cause {
                ExitCause::Input => TransportEnd::ReadError,
                ExitCause::Output => TransportEnd::WriteError,
                ExitCause::Child | ExitCause::Shutdown | ExitCause::Callback =>
                    panic!("these causes are not a transport half"),
            }
        );
    }
    before_fallback
        .reached
        .await
        .expect("SDK ended before generic retirement");
    waiting
        .release
        .send(())
        .expect("poll the actual SDK result with only the source mark");
    reply.assert(kind, ExpectedReply::Lost).await;
    before_fallback
        .release
        .send(())
        .expect("allow regular provider cleanup");
    assert_eq!(
        provider_end
            .await
            .expect("observe the actual selected provider exit"),
        match cause {
            ExitCause::Child => ProviderConnectionEnd::ChildExited,
            ExitCause::Input | ExitCause::Output | ExitCause::Callback =>
                ProviderConnectionEnd::SdkFailed,
            ExitCause::Shutdown => ProviderConnectionEnd::SdkReturned,
        },
        "{kind:?} / {cause:?}"
    );
    drop(eof);
    drop(binding);
    fixture.runtime.shutdown().await;
    drop((warm_prompt, child_keepalive));
    tasks.close();
    tasks.wait().await;
}
