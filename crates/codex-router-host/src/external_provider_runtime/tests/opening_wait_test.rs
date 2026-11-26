//! An outstanding ACP opening and a genuinely missing hold have different moves.

use super::*;
use provider_session_machines::harness_lifecycle::{HeldOpeningKind, LifecycleRefusal};

#[tokio::test]
async fn acp_duplicate_configure_waits_while_a_missing_hold_can_reopen() {
    let fixture = hub_runtime().await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let stop = CancellationToken::new();
    let missing = CreateAttempt(Uuid::now_v7());
    let (unused_reply, _unused_answer) = oneshot::channel();
    let not_held = binding
        .configure_created(
            missing,
            Box::new(Reply(unused_reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("the live binding has no held opening");
    assert_eq!(not_held, LifecycleRefusal::OpeningNotHeld);
    assert_eq!(not_held.valid_moves(), &[LifecycleMove::Reopen]);

    let attempt = CreateAttempt(Uuid::now_v7());
    let setup = OpenSetup {
        cwd: fixture._root.path().to_owned(),
        work_roots: Default::default(),
        scratch_folder: None,
    };
    let (reply, opening) = oneshot::channel();
    binding
        .open(
            OpenRequest::Create {
                owner,
                attempt,
                setup: setup.clone(),
                requested: RequestedProviderSettings {
                    mode: Some("ask".into()),
                    ..Default::default()
                },
            },
            Box::new(Reply(reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("the new opening on the same live binding is admitted");
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Create(attempt)),
        Held::NotHeld(LifecycleRefusal::OpeningInProgress)
    );
    let OpeningFact::Created {
        attempt: exact,
        session,
    } = opening.await.expect("provider created the session")
    else {
        panic!("expected a held ACP creation");
    };
    assert_eq!(exact, attempt);

    let Err(held) = binding.accept_settings(
        OpenAttempt::Create(attempt),
        &AcceptedRunSettings {
            catalogue: Default::default(),
            router_auto_requested: false,
        },
    ) else {
        panic!("accepting settings is premature while the exact create is held");
    };
    assert_eq!(
        held,
        LifecycleRefusal::OpeningAlreadyHeld(HeldOpeningKind::Create),
        "a held create needs its owner's configure call before it can progress"
    );
    assert_eq!(held.valid_moves(), &[LifecycleMove::ContinueHeldOpening]);

    let (reply, mut configured) = oneshot::channel();
    binding
        .configure_created(
            attempt,
            Box::new(Reply(reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("original configuration starts");
    // This test uses the current-thread runtime. Configuring is recorded
    // before the configuration task can run and before this duplicate call.
    let (duplicate_reply, _duplicate_answer) = oneshot::channel();
    let pending = binding
        .configure_created(
            attempt,
            Box::new(Reply(duplicate_reply)),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect_err("the original configuration owns this attempt");
    assert_eq!(
        pending,
        LifecycleRefusal::OpeningInProgress,
        "a duplicate configure must wait for the exact opening already in progress"
    );
    assert_eq!(pending.valid_moves(), &[LifecycleMove::WaitForOpening]);
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Create(attempt)),
        Held::NotHeld(pending)
    );
    assert!(matches!(
        configured.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let (wrong_reply, _wrong_answer) = oneshot::channel();
    assert_eq!(
        binding.configure_created(
            missing,
            Box::new(Reply(wrong_reply)),
            Box::pin(stop.clone().cancelled_owned()),
        ),
        Err(LifecycleRefusal::AttemptMismatch)
    );
    let (competing_reply, _competing_answer) = oneshot::channel();
    assert_eq!(
        binding.open(
            OpenRequest::Restore {
                owner,
                run: RunAttempt {
                    generation: SessionGeneration::from_number(1),
                    attempt: LoadAttempt(Uuid::now_v7())
                },
                session,
                via: RestoreVia::Load,
                setup,
                requested_mode: None,
            },
            Box::new(Reply(competing_reply)),
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        ),
        Err(LifecycleRefusal::AttemptMismatch)
    );

    let CreatedSetupFact::Configured {
        attempt: exact,
        report,
    } = configured
        .await
        .expect("waiting preserves the original answer")
    else {
        panic!("the original requested setting did not configure");
    };
    assert_eq!(exact, attempt);
    assert_eq!(report.effective_settings.mode.as_deref(), Some("ask"));
    assert_eq!(
        binding.opening_is_held(OpenAttempt::Create(attempt)),
        Held::Held
    );
    let (already_configured, _no_second_answer) = oneshot::channel();
    assert_eq!(
        binding.configure_created(
            attempt,
            Box::new(Reply(already_configured)),
            Box::pin(stop.clone().cancelled_owned()),
        ),
        Err(LifecycleRefusal::OperationNotOffered)
    );
    binding
        .activate(
            ActivationSetup::Created {
                attempt,
                generation: SessionGeneration::from_number(1),
            },
            AcceptedRunSettings {
                catalogue: Default::default(),
                router_auto_requested: false,
            },
            Arc::new(AcceptEvents),
            Box::pin(stop.clone().cancelled_owned()),
        )
        .expect("the original configured handoff activates");
    let (consumed, _no_third_answer) = oneshot::channel();
    assert_eq!(
        binding.configure_created(
            attempt,
            Box::new(Reply(consumed)),
            Box::pin(stop.clone().cancelled_owned()),
        ),
        Err(LifecycleRefusal::OperationNotOffered)
    );
    assert_eq!(recorded_requests(&fixture.receipt, "session/new"), 1);
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), 0);
    assert_eq!(
        recorded_requests(&fixture.receipt, "session/set_config_option"),
        1
    );
    drop(binding);
    stop.cancel();
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
