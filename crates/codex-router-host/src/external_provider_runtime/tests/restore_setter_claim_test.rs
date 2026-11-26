//! Observe setter custody across the binding's re-entrant reply reservation.

use super::*;
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{RestoreSettingRefusal, SettingAttempt, SettingFact},
    session_owner::{OwnerSetting, ReplyAdmission, RestoreModeOutcome},
    session_setting_machine::{SettingApplication, SettingValueId},
};

#[tokio::test]
async fn a_reentrant_accept_withdraws_the_unwritten_setter_and_its_reply() {
    exercise_setter(true).await;
}

#[tokio::test]
async fn the_declared_held_setter_accepts_one_real_reply_and_frame() {
    exercise_setter(false).await;
}

async fn exercise_setter(reenter: bool) {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new()).await;
    let accepted = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: false,
    };
    let setting = OwnerSetting {
        change: RunAttempt {
            generation: run.generation,
            attempt: SettingApplication(Uuid::now_v7()),
        },
        kind: ProviderSettingKind::Mode,
    };
    let (reply, received) = oneshot::channel();
    let mut reply = Some(reply);
    let mut reserve = || {
        if reenter {
            binding
                .accept_settings(OpenAttempt::Restore(run), &accepted)
                .expect("the reservation callback can prepare its own held opening");
        }
        ReplyAdmission::Reserved(Box::new(Reply(reply.take().expect("one reservation"))))
    };
    let result = binding.put_setting_back(
        SettingAttempt {
            restore: run,
            setting,
            value: SettingValueId("ask".into()),
            catalogue: Arc::clone(&accepted.catalogue),
        },
        &mut reserve,
    );
    if reenter {
        assert_eq!(
            result,
            Err(RestoreSettingRefusal::Harness(
                LifecycleRefusal::OpeningPrepared
            )),
            "a reservation returned after preparation is no longer eligible for the writer"
        );
        assert!(
            received.await.is_err(),
            "the withdrawn setter must not publish a reply"
        );
    } else {
        result.expect("the declared held setter must reach the writer");
        let SettingFact::Mode {
            restore,
            setting: exact,
            outcome: RestoreModeOutcome::Confirmed(value),
            ..
        } = received.await.expect("one exact provider answer")
        else {
            panic!("expected the provider's confirmed setter reply");
        };
        assert_eq!((restore, exact, value.0.as_str()), (run, setting, "ask"));
    }
    // A real close reply fences any older frame in the ordered writer.
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, closed) = oneshot::channel();
    binding
        .close(close, Box::new(Reply(reply)))
        .expect("queue the close fence");
    assert_eq!(
        closed.await.expect("provider close fence"),
        CloseFact::Closed { run: close }
    );
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        usize::from(!reenter)
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/set_mode"), 0);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_fences_the_setter_at_the_actual_writer_take() {
    setter_take_schedule(false).await;
    setter_take_schedule(true).await;
}

pub(super) async fn setter_take_schedule(taken: bool) {
    use acp_client_runtime::owner_wire::OpeningPausePoint;

    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let (readback, _) =
        load_fixture_binding(&fixture, &mut binding, owner, run, CancellationToken::new()).await;
    let accepted = AcceptedRunSettings {
        catalogue: readback.catalogue,
        router_auto_requested: false,
    };
    let wire = fixture.runtime.client.owner_wire();
    let paused = wire.pause_opening_for_test(if taken {
        OpeningPausePoint::AfterSetterTake
    } else {
        OpeningPausePoint::BeforeSetterTake
    });
    let binding = Arc::new(binding);
    let attempt = SettingAttempt {
        restore: run,
        setting: OwnerSetting {
            change: RunAttempt {
                generation: run.generation,
                attempt: SettingApplication(Uuid::now_v7()),
            },
            kind: ProviderSettingKind::Mode,
        },
        value: SettingValueId("ask".into()),
        catalogue: Arc::clone(&accepted.catalogue),
    };
    let (reply, received) = oneshot::channel();
    let pending = tokio::task::spawn_blocking({
        let binding = Arc::clone(&binding);
        move || {
            let mut reply = Some(reply);
            binding.put_setting_back(attempt, &mut || {
                ReplyAdmission::Reserved(Box::new(Reply(reply.take().expect("one reservation"))))
            })
        }
    });
    paused
        .reached
        .await
        .expect("setter reached the real writer boundary");
    binding
        .accept_settings(OpenAttempt::Restore(run), &accepted)
        .expect("prepare while the setter is staged at its actual writer boundary");
    paused.release.send(()).expect("resume the setter");
    let result = pending.await.expect("setter task");
    if taken {
        assert_eq!(result, Ok(()));
        assert!(
            matches!(received.await.expect("one original write result"), SettingFact::Mode { restore, outcome: RestoreModeOutcome::Confirmed(_), .. } if restore == run)
        );
    } else {
        assert_eq!(
            result,
            Err(RestoreSettingRefusal::Harness(
                LifecycleRefusal::OpeningPrepared
            ))
        );
        assert!(received.await.is_err());
    }
    let close = RunAttempt {
        generation: run.generation,
        attempt: CloseAttempt(Uuid::now_v7()),
    };
    let (reply, closed) = oneshot::channel();
    binding
        .close(close, Box::new(Reply(reply)))
        .expect("queue real writer fence");
    assert_eq!(
        closed.await.expect("real close"),
        CloseFact::Closed { run: close }
    );
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        usize::from(taken)
    );
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
