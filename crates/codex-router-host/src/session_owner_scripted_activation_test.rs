//! Exercise a typed activation refusal through the real owner, store and hub.

use super::*;

#[tokio::test]
async fn refused_activation_settles_restore_without_publishing_live_and_can_retry() {
    use provider_session_machines::harness_lifecycle::ActivationRefusal;
    use std::sync::atomic::AtomicBool;

    let mut setup = ScriptedSession::with_saved_session(ScriptControls {
        refuse_next_activation: Arc::new(AtomicBool::new(true)),
        ..Default::default()
    })
    .await;
    let board = ProviderIdentity::session_to_board(&setup.target).expect("board session");
    let owner = setup
        .door
        .open(setup.target.clone(), Default::default())
        .expect("owner");
    let address = owner.snapshots().borrow().owner;

    let mut history_returned = setup
        .hub
        .owner_inbox(board.clone(), NonZeroUsize::MIN)
        .0
        .capacity_returned();
    let (prepared, release) = setup.preparation.pause_restore();
    let (settled, mut history) = {
        let restore = setup.restore_settlement(&owner);
        tokio::pin!(restore);
        tokio::select! {
            ready = prepared => ready.expect("preparation succeeded before activation"),
            _ = &mut restore => panic!("restore settled before the activation fixture released preparation"),
        }
        history_returned
            .changed()
            .await
            .expect("the initial rebase applied");
        let history = setup
            .hub
            .attach(board.clone())
            .await
            .expect("baseline history after initial rebase");
        release.send(()).expect("allow the exact activation");
        (restore.await, history)
    };
    let SessionOwnerReply::RestoreFinished(restore) = settled.reply else {
        panic!("activation refusal did not settle the original restore");
    };
    assert_eq!(
        restore.disposition,
        RestoreDisposition::Retired {
            failure: RestoreFailure::LocalPrerequisite {
                stage: PreparationStage::Route
            },
            remote: RemoteEffect::Applied,
        },
        "a refused activation must not report a successful restore"
    );
    assert!(
        settled.saved,
        "the original restore failure belongs to its durable settlement"
    );
    assert_eq!(
        setup
            .observe(|event| matches!(event, ScriptObservation::ActivationRefused(..)))
            .await,
        ScriptObservation::ActivationRefused(
            ActivationSetup::Restored(restore.attempt),
            ActivationRefusal::OpeningNotHeld
        )
    );
    assert_eq!(
        setup
            .observe(|event| matches!(event, ScriptObservation::Retired(_)))
            .await,
        ScriptObservation::Retired(restore.attempt.generation)
    );
    assert!(setup.script.run_events(address.incarnation).is_none());
    assert!(
        setup.script.settings_used(address.incarnation).is_empty(),
        "activation refusal published settings for an inactive run"
    );
    assert!(
        matches!(owner.snapshots().borrow().lifecycle, SessionLifecycle::NotLoaded { last } if last == restore.attempt.generation)
    );
    assert_eq!(
        setup
            .hub
            .state(board)
            .await
            .expect("projection after refusal"),
        SessionState::Unloaded
    );
    loop {
        match history.receiver.try_recv() {
            Ok(event) => assert!(
                !matches!(
                    event.event,
                    SessionEvent::StateChanged {
                        state: SessionState::Idle
                    }
                ),
                "activation refusal published a transient Live batch"
            ),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(error) => panic!("activation refusal reset retained history: {error}"),
        }
    }

    setup.restore(&owner).await;
    setup
        .observe(|event| matches!(event, ScriptObservation::LiveReported))
        .await;
    let live = setup.owner().snapshots().borrow().clone();
    assert_eq!(live.owner, address);
    assert!(
        matches!(live.lifecycle, SessionLifecycle::Live { generation } if generation > restore.attempt.generation)
    );
    setup.door.shutdown().await;
}
