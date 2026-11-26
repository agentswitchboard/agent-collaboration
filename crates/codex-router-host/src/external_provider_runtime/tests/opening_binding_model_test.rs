//! Generated caller operations against the production ACP binding.

use super::*;
use hegel::generators as gs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelOpening {
    Idle,
    Held,
    Prepared,
    Live,
    RouteLost,
}

struct Model {
    opening: ModelOpening,
    bound: bool,
    stopped: bool,
}

impl Model {
    fn refusal(&self) -> LifecycleRefusal {
        if self.stopped {
            return LifecycleRefusal::ConnectionRetired;
        }
        match self.opening {
            ModelOpening::Idle if self.bound => LifecycleRefusal::BoundSessionNotLoaded,
            ModelOpening::Idle => LifecycleRefusal::OpeningNotHeld,
            ModelOpening::Held => LifecycleRefusal::OpeningAlreadyHeld(HeldOpeningKind::Restore),
            ModelOpening::Prepared => LifecycleRefusal::OpeningPrepared,
            ModelOpening::Live => LifecycleRefusal::OpeningActive,
            ModelOpening::RouteLost => LifecycleRefusal::BoundRouteUnavailable,
        }
    }

    fn moves(&self) -> &'static [LifecycleMove] {
        use LifecycleMove::{
            ActivateOpening, CloseActiveRun, ContinueHeldOpening, KeepActiveRun, LoadBoundSession,
            Reopen, Retire, StopOwner,
        };
        if self.stopped {
            return &[StopOwner];
        }
        match self.opening {
            ModelOpening::Idle if self.bound => &[LoadBoundSession],
            ModelOpening::Idle => &[Reopen],
            ModelOpening::Held => &[ContinueHeldOpening],
            ModelOpening::Prepared => &[ActivateOpening],
            ModelOpening::Live => &[KeepActiveRun, CloseActiveRun, Retire],
            ModelOpening::RouteLost => &[Retire],
        }
    }

    fn check_refusal(&self, refusal: LifecycleRefusal) {
        assert_eq!(refusal, self.refusal());
        assert_eq!(refusal.valid_moves(), self.moves());
    }
}

#[hegel::test(test_cases = 24)]
fn production_binding_tracks_opening_routes_payloads_and_admissible_calls(case: hegel::TestCase) {
    let steps: Vec<_> = (0..case.draw(gs::integers::<u8>().min_value(6).max_value(20)))
        .map(|_| case.draw(gs::integers::<u8>().max_value(9)))
        .collect();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(run_sequence(steps));
}

async fn run_sequence(steps: Vec<u8>) {
    let fixture = hub_runtime_with(&["advertise-close"]).await;
    let tasks = TaskTracker::new();
    let (owner, mut binding) = bind_fixture(&fixture.runtime, &tasks);
    let mut model = Model {
        opening: ModelOpening::Idle,
        bound: false,
        stopped: false,
    };
    let mut run = RunAttempt {
        generation: SessionGeneration::from_number(1),
        attempt: LoadAttempt(Uuid::now_v7()),
    };
    let stop = CancellationToken::new();
    let mut settings = None;
    let mut loads = 0;
    let mut closes = 0;
    for step in [0, 1, 2, 3, 4, 0].into_iter().chain(steps) {
        let before = model.opening;
        let identity = OpenAttempt::Restore(run);
        match step {
            0 => {
                let (reply, response) = oneshot::channel();
                let admission = binding.open(
                    restore_request(&fixture, owner, run),
                    Box::new(Reply(reply)),
                    Arc::new(AcceptEvents),
                    Box::pin(stop.clone().cancelled_owned()),
                );
                if model.stopped || before != ModelOpening::Idle {
                    model.check_refusal(admission.expect_err("no other load owns this slot"));
                    assert!(response.await.is_err());
                } else {
                    admission.expect("load a bound session on a live writer");
                    assert_eq!(
                        binding.opening_is_held(identity),
                        Held::NotHeld(LifecycleRefusal::OpeningInProgress)
                    );
                    let OpeningFact::Restored {
                        run: opened,
                        opening: RestoredOpening::Loaded { readback, .. },
                    } = response.await.expect("actual provider load")
                    else {
                        panic!("missing actual load");
                    };
                    assert_eq!(opened, run);
                    model.bound = true;
                    model.opening = ModelOpening::Held;
                    settings = Some(AcceptedRunSettings {
                        catalogue: readback.catalogue,
                        router_auto_requested: readback.router_auto_requested,
                    });
                    loads += 1;
                }
            }
            1 => {
                let empty = AcceptedRunSettings {
                    catalogue: Default::default(),
                    router_auto_requested: false,
                };
                let accepted =
                    binding.accept_settings(identity, settings.as_ref().unwrap_or(&empty));
                if !model.stopped && before == ModelOpening::Held {
                    accepted.expect("held row has the reader needed to prepare settings");
                    model.opening = ModelOpening::Prepared;
                } else {
                    let Err(refusal) = accepted else {
                        panic!("unavailable setup changed the row");
                    };
                    model.check_refusal(refusal);
                }
            }
            2 => {
                let accepted = settings.clone().unwrap_or(AcceptedRunSettings {
                    catalogue: Default::default(),
                    router_auto_requested: false,
                });
                let result = binding.activate(
                    ActivationSetup::Restored(run),
                    accepted,
                    Arc::new(AcceptEvents),
                    Box::pin(stop.clone().cancelled_owned()),
                );
                if !model.stopped && before == ModelOpening::Prepared {
                    result.expect("prepared row can transfer its reader exactly once");
                    model.opening = ModelOpening::Live;
                } else if !model.stopped && before == ModelOpening::Live {
                    assert_eq!(result, Err(ActivationRefusal::AlreadyActive));
                } else {
                    let refusal = result.expect_err("illegal activation cannot move the row");
                    assert_eq!(refusal.valid_moves(), model.moves());
                }
            }
            3 | 4 if before != ModelOpening::Held && before != ModelOpening::Prepared => {
                let close = RunAttempt {
                    generation: run.generation,
                    attempt: CloseAttempt(Uuid::now_v7()),
                };
                let wire = fixture.runtime.client.owner_wire();
                let capacity = (step == 3).then(|| wire.hold_writer_capacity_for_test());
                let (reply, settled) = oneshot::channel();
                let result = binding.close(close, Box::new(Reply(reply)));
                drop(capacity);
                if !model.stopped && before == ModelOpening::Live {
                    if step == 3 {
                        assert_eq!(result, Err(LifecycleRefusal::Writer(WriterState::Full)));
                        assert!(settled.await.is_err());
                    } else {
                        result.expect("a live route accepts close");
                        assert_eq!(
                            settled.await.expect("real close result"),
                            CloseFact::Closed { run: close }
                        );
                        closes += 1;
                        model.opening = ModelOpening::Idle;
                        settings = None;
                    }
                } else {
                    model.check_refusal(result.expect_err("no eligible route"));
                    assert!(settled.await.is_err());
                }
            }
            5 if model.bound => {
                let OpenRequest::Restore { setup, .. } = restore_request(&fixture, owner, run)
                else {
                    panic!("expected the test's load request");
                };
                let (reply, response) = oneshot::channel();
                let result = binding.open(
                    OpenRequest::Restore {
                        owner,
                        run,
                        setup,
                        session: HarnessSessionId("unrelated".into()),
                        via: RestoreVia::Load,
                        requested_mode: None,
                    },
                    Box::new(Reply(reply)),
                    Arc::new(AcceptEvents),
                    Box::pin(stop.clone().cancelled_owned()),
                );
                assert_eq!(
                    result,
                    Err(if model.stopped {
                        LifecycleRefusal::ConnectionRetired
                    } else {
                        LifecycleRefusal::AttemptMismatch
                    })
                );
                assert!(response.await.is_err());
            }
            6 if before == ModelOpening::Live && !model.stopped => {
                fixture
                    .runtime
                    .retire_provider_route_for_test(owner.incarnation, run.generation);
                model.opening = ModelOpening::RouteLost;
            }
            7 => {
                binding.retire(run.generation);
                model.opening = ModelOpening::Idle;
                settings = None;
            }
            8 if !model.stopped => {
                fixture.runtime.retirement().cancel();
                model.stopped = true;
                model.opening = ModelOpening::Idle;
                settings = None;
            }
            9 if before == ModelOpening::Idle => {
                run.attempt = LoadAttempt(Uuid::now_v7());
            }
            _ => {}
        }
        let held = binding.opening_is_held(OpenAttempt::Restore(run));
        if !model.stopped && matches!(model.opening, ModelOpening::Held | ModelOpening::Prepared) {
            assert_eq!(held, Held::Held);
        } else {
            let Held::NotHeld(refusal) = held else {
                panic!("model has no held reader");
            };
            model.check_refusal(refusal);
        }
    }
    assert_eq!(recorded_requests(&fixture.receipt, "session/load"), loads);
    assert_eq!(recorded_requests(&fixture.receipt, "session/close"), closes);
    binding.retire(run.generation);
    drop(binding);
    fixture.runtime.shutdown().await;
    tasks.close();
    tasks.wait().await;
}
