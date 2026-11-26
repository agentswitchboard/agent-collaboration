//! Exercise capture retirement and the restore answer against the real owner and hub.

use super::*;
use provider_session_machines::harness_lifecycle::EventRefusal;
use std::sync::Mutex;

#[tokio::test]
async fn retires_a_replay_blocked_after_one_capture_and_consumes_its_late_answer() {
    let (hold_replay, first_capture) = oneshot::channel();
    let mut setup = ScriptedSession::with_saved_session(ScriptControls {
        pause_replay: Arc::new(Mutex::new(Some(hold_replay))),
        ..Default::default()
    })
    .await;
    let owner = setup
        .door
        .open(setup.target.clone(), Default::default())
        .expect("register saved owner");
    let address = owner.snapshots().borrow().owner;
    let board = ProviderIdentity::session_to_board(&setup.target).expect("board session");
    let mut publication = setup
        .hub
        .owner_inbox(board.clone(), NonZeroUsize::MIN)
        .0
        .capacity_returned();
    let (settled, paused, baseline) = {
        let restore = setup.restore_settlement(&owner);
        tokio::pin!(restore);
        let paused = tokio::select! {
            paused = first_capture => paused.expect("the exact load captured its first event"),
            _ = &mut restore => panic!("restore answered before releasing first capture"),
        };
        assert!(matches!(paused.captured.as_slice(),
            [SessionEvent::ItemStarted { item }] if item.text.as_deref() == Some(REPLAY_TEXTS[0])));
        assert!(matches!(owner.snapshots().borrow().lifecycle,
            SessionLifecycle::Loading { generation, attempt, .. }
            if generation == paused.run.generation && attempt == paused.run.attempt));
        publication.changed().await.expect("initial rebase applied");
        let baseline = setup
            .hub
            .attach(board.clone())
            .await
            .expect("baseline history")
            .snapshot;
        setup
            .script
            .owner_events(address.incarnation)
            .expect("bound owner event port")
            .report_harness_gone()
            .await
            .expect("admit harness retirement");
        (restore.await, paused, baseline)
    };
    let SessionOwnerReply::RestoreFinished(result) = settled.reply else {
        panic!("expected the original restore settlement");
    };
    assert!(settled.saved);
    assert_eq!(result.attempt, paused.run);
    assert_eq!(
        result.disposition,
        RestoreDisposition::Retired {
            failure: RestoreFailure::Disconnected,
            remote: RemoteEffect::Unknown,
        }
    );
    assert_eq!(result.replay, None);
    assert_eq!(
        setup
            .observe(|event| matches!(event, ScriptObservation::Retired(_)))
            .await,
        ScriptObservation::Retired(paused.run.generation),
    );

    assert_eq!(paused.complete_late().await, Err(EventRefusal::Retired));
    // The original reserved reply is now queued. Completions are processed before commands.
    assert!(matches!(
        owner
            .admit(SessionOwnerCommand::AcceptSettings {
                generation: result.attempt.generation
            })
            .await,
        Err(OwnerDoorError::Refused { .. })
    ));
    let snapshot = owner.snapshots().borrow().clone();
    assert!(
        matches!(snapshot.lifecycle, SessionLifecycle::NotLoaded { last } if last == result.attempt.generation)
    );
    assert_eq!(snapshot.last_restore, Some(result));
    assert!(setup.script.run_events(address.incarnation).is_none());
    assert!(setup.script.settings_used(address.incarnation).is_empty());
    assert!(matches!(
        setup.observations.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        setup
            .hub
            .attach(board)
            .await
            .expect("history after old reply")
            .snapshot,
        baseline,
        "a late opening answer must not publish any of the old capture"
    );
    setup.door.shutdown().await;
}

#[tokio::test]
async fn restore_answer_observes_history_and_skills_with_hub_publication_held() {
    let setup = ScriptedSession::with_saved_session(ScriptControls::default()).await;
    let owner = setup
        .door
        .open(setup.target.clone(), Default::default())
        .expect("register saved owner");
    let board = ProviderIdentity::session_to_board(&setup.target).expect("board session");
    let mut publication = setup
        .hub
        .owner_inbox(board.clone(), NonZeroUsize::MIN)
        .0
        .capacity_returned();
    let (prepared, release) = setup.preparation.pause_restore();
    let restore = setup.restore_settlement(&owner);
    tokio::pin!(restore);
    tokio::select! {
        ready = prepared => ready.expect("restore prepared after replay capture"),
        _ = &mut restore => panic!("caller answered before releasing preparation"),
    }
    publication
        .changed()
        .await
        .expect("the initial rebase applied");
    let mut history = setup
        .hub
        .attach(board.clone())
        .await
        .expect("attached history");
    let skills = setup
        .hub
        .skill_view(
            board.clone(),
            setup
                .fixture
                .path
                .parent()
                .expect("saved working directory")
                .to_owned(),
        )
        .await
        .expect("attached skill view");
    let held = setup.hub.hold_history_for_test(board.clone()).await;
    release.send(()).expect("resume activation");

    let settled = restore.await;
    assert!(
        matches!(settled.reply, SessionOwnerReply::RestoreFinished(result)
        if matches!(result.disposition, RestoreDisposition::Restored { .. }))
    );
    // Main also lets restore answer before the hub applies history and the
    // restored skill view. A fresh attachment would wait for this same lock.
    assert!(
        matches!(
            history.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "the already attached history stays at its baseline while publication is held"
    );
    assert_eq!(*skills.receiver.borrow(), SkillView::Unknown);
    drop(held);

    let ordered = loop {
        let retained = setup
            .hub
            .attach(board.clone())
            .await
            .expect("published history");
        let mut ordered = Vec::new();
        for item in retained.snapshot {
            observe(&mut ordered, item.event);
        }
        if ordered
            .iter()
            .any(|item| matches!(item, Observed::Item(text) if text == LIVE_TEXT))
        {
            break ordered;
        }
        publication
            .changed()
            .await
            .expect("hub progress after release");
    };
    assert_eq!(
        ordered,
        vec![
            Observed::Item(REPLAY_TEXTS[0].into()),
            Observed::Item(REPLAY_TEXTS[1].into()),
            Observed::Idle,
            Observed::Item(LIVE_TEXT.into()),
        ],
        "history publishes in capture order before the live item"
    );
    assert!(matches!(
        *skills.receiver.borrow(),
        SkillView::Available { .. }
    ));
    setup.door.shutdown().await;
}
