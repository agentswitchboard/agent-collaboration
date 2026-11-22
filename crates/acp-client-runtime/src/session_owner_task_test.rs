use super::*;
use provider_session_machines::session_lifecycle_machine::SessionLifecycle;
use provider_session_machines::session_owner::{
    ConnectionEpoch, CreateOutcome, CreateProgress, OwnerIncarnation, PublicationBatch,
    PublicationResult, PublicationStatus, RemoteEffect, SessionWireRequest,
};
use router_clock::ManualClock;
use std::time::Duration;
use uuid::Uuid;

struct RecordingPorts {
    observed: mpsc::UnboundedSender<OwnerEffect>,
    refuse_first_batch: bool,
}

impl SessionOwnerPorts for RecordingPorts {
    fn execute(
        &mut self,
        _: OwnerAddress,
        effect: OwnerEffect,
        _: &OwnerCompletionPort,
        _caller_wakes: &mut crate::session_owner_task::OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let immediate = match &effect {
            OwnerEffect::Publish(
                PublicationBatch::Commit { id, .. } | PublicationBatch::Rebase { id, .. },
            ) => {
                let refused = std::mem::take(&mut self.refuse_first_batch);
                Some(OwnerEvent::Publication(if refused {
                    PublicationResult::Refused(*id)
                } else {
                    PublicationResult::Applied(*id)
                }))
            }
            _ => None,
        };
        self.observed
            .send(effect)
            .map_err(|_| OwnerTaskError::ExecutorStopped)?;
        Ok(immediate)
    }
}

struct Fixture {
    clock: Arc<ManualClock>,
    address: OwnerAddress,
    handle: SessionOwnerHandle,
    effects: mpsc::UnboundedReceiver<OwnerEffect>,
    stop: CancellationToken,
    worker: tokio::task::JoinHandle<Result<(), OwnerTaskError>>,
}

impl Fixture {
    fn start(refuse_first_batch: bool) -> Self {
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let address = OwnerAddress {
            incarnation: OwnerIncarnation(Uuid::now_v7()),
            connection_epoch: ConnectionEpoch(Uuid::now_v7()),
        };
        let (task, handle) =
            SessionOwnerTask::new(address, clock.clone(), NonZeroUsize::MIN, NonZeroUsize::MIN);
        let (observed, effects) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let worker = tokio::spawn(task.run(
            RecordingPorts {
                observed,
                refuse_first_batch,
            },
            stop.clone(),
        ));
        Self {
            clock,
            address,
            handle,
            effects,
            stop,
            worker,
        }
    }

    fn submit_create(&self, operation: Uuid) -> oneshot::Receiver<Result<(), OwnerCommandRefusal>> {
        self.handle
            .try_submit(self.address, SessionOwnerCommand::Create { operation })
            .expect("the mailbox accepted the create")
    }

    async fn next_effect(&mut self) -> OwnerEffect {
        self.effects.recv().await.expect("owner emitted an effect")
    }

    async fn begin_create(&mut self) -> Uuid {
        let operation = Uuid::now_v7();
        self.submit_create(operation)
            .await
            .expect("received admission")
            .expect("accepted");
        assert!(matches!(
            self.next_effect().await,
            OwnerEffect::Publish(PublicationBatch::Rebase { .. })
        ));
        assert_eq!(
            self.next_effect().await,
            OwnerEffect::Wire(SessionWireRequest::Create { operation })
        );
        operation
    }

    async fn finish(self) {
        self.stop.cancel();
        assert_eq!(self.worker.await.expect("owner task joined"), Ok(()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_due_deadline_settles_before_the_full_business_mailbox_is_read() {
    let mut fixture = Fixture::start(false);
    let first = fixture.begin_create().await;
    let budget = Duration::from_secs(30);
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(budget))
        .await;

    let second = Uuid::now_v7();
    let admitted_after_deadline = fixture.submit_create(second);
    assert!(matches!(
        fixture.handle.try_submit(
            fixture.address,
            SessionOwnerCommand::Create {
                operation: Uuid::now_v7()
            }
        ),
        Err(OwnerMailboxUnavailable::Busy)
    ));
    fixture
        .clock
        .advance(budget)
        .expect("advance to create deadline");

    assert_eq!(
        fixture.next_effect().await,
        OwnerEffect::CreateSettled {
            operation: first,
            outcome: CreateOutcome::Failed(RemoteEffect::Unknown),
        }
    );
    admitted_after_deadline
        .await
        .expect("new create received")
        .expect("deadline freed admission");
    assert_eq!(
        fixture.next_effect().await,
        OwnerEffect::Wire(SessionWireRequest::Create { operation: second })
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_completion_is_delivered_with_a_full_business_mailbox() {
    let mut fixture = Fixture::start(false);
    let first = fixture.begin_create().await;
    let completions = fixture.handle.completion_port();
    let completed = completions
        .reserve(fixture.address)
        .expect("reserve before accepting effect");
    assert!(matches!(
        completions.reserve(fixture.address),
        Err(OwnerTaskError::CompletionCapacity)
    ));

    let second = Uuid::now_v7();
    let admitted_after_completion = fixture.submit_create(second);
    completed.send(OwnerEvent::Create(CreateProgress::Failed {
        operation: first,
        remote: RemoteEffect::None,
    }));

    assert_eq!(
        fixture.next_effect().await,
        OwnerEffect::CreateSettled {
            operation: first,
            outcome: CreateOutcome::Failed(RemoteEffect::None),
        }
    );
    admitted_after_completion
        .await
        .expect("new create received")
        .expect("completion freed admission");
    assert_eq!(
        fixture.next_effect().await,
        OwnerEffect::Wire(SessionWireRequest::Create { operation: second })
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn publication_rebases_on_the_injected_clock_without_another_command() {
    let mut fixture = Fixture::start(true);
    let mut snapshots = fixture.handle.snapshots();
    fixture.begin_create().await;
    let refused = snapshots
        .wait_for(|snapshot| matches!(snapshot.publication, PublicationStatus::Degraded { .. }))
        .await
        .expect("refusal is visible")
        .publication;
    let PublicationStatus::Degraded { since, .. } = refused else {
        panic!("publication must show the refused batch");
    };
    let budget = Duration::from_secs(10);
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(budget))
        .await;

    fixture.clock.advance(budget).expect("advance to rebase");
    let rebased = fixture.next_effect().await;
    assert!(
        matches!(rebased, OwnerEffect::Publish(PublicationBatch::Rebase { id, .. })
        if id.incarnation == since.incarnation && id.sequence > since.sequence)
    );
    snapshots
        .wait_for(|snapshot| snapshot.publication == PublicationStatus::Current)
        .await
        .expect("confirmed rebase restores Current");
    fixture.finish().await;
}

struct CreateReplySnapshotProbe {
    snapshot: watch::Receiver<SessionSnapshot>,
    reply: Option<oneshot::Sender<(SessionLifecycle, SessionLifecycle)>>,
}

impl SessionOwnerPorts for CreateReplySnapshotProbe {
    fn execute(
        &mut self,
        _: OwnerAddress,
        effect: OwnerEffect,
        _: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        Ok(match effect {
            OwnerEffect::Wire(SessionWireRequest::Create { operation }) => {
                Some(OwnerEvent::Create(CreateProgress::ProviderId {
                    operation,
                    provider_id: "fixture-session".into(),
                }))
            }
            OwnerEffect::AdoptProviderId { operation, .. } => {
                Some(OwnerEvent::Create(CreateProgress::Adopted {
                    operation,
                    facts: Box::default(),
                }))
            }
            OwnerEffect::Publish(
                PublicationBatch::Commit { id, .. } | PublicationBatch::Rebase { id, .. },
            ) => Some(OwnerEvent::Publication(PublicationResult::Applied(id))),
            OwnerEffect::CreateSettled {
                outcome: CreateOutcome::Adopted(generation),
                ..
            } => {
                // The observation runs synchronously at the actual send, not at preparation.
                let reply = self.reply.take().expect("one successful create");
                let snapshot = self.snapshot.clone();
                caller_wakes.observe_send(move || {
                    reply
                        .send((
                            snapshot.borrow().lifecycle,
                            SessionLifecycle::Live { generation },
                        ))
                        .expect("create caller still listens");
                });
                None
            }
            _ => None,
        })
    }
}

// Rejects successful create replying before callers can read the newly live session.
#[test]
fn successful_create_replies_with_live_snapshot_visible() {
    let address = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let (mut task, caller) =
        SessionOwnerTask::new(address, clock, NonZeroUsize::MIN, NonZeroUsize::MIN);
    let (reply, mut received) = oneshot::channel();
    let mut ports = CreateReplySnapshotProbe {
        snapshot: caller.snapshots(),
        reply: Some(reply),
    };

    task.process(
        OwnerMailboxEvent {
            owner: address,
            event: OwnerEvent::Command(SessionOwnerCommand::Create {
                operation: Uuid::now_v7(),
            }),
        },
        None,
        &mut ports,
    )
    .expect("drive the real owner through successful create");

    let (at_reply, expected) = received.try_recv().expect("create delivered success");
    assert_eq!(caller.snapshots().borrow().lifecycle, expected);
    assert_eq!(
        at_reply, expected,
        "successful create replied before publishing its Live snapshot"
    );
}

fn unpublished_live_owner() -> (SessionOwnerTask, SessionOwnerHandle, OwnerAddress) {
    let address = OwnerAddress {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        connection_epoch: ConnectionEpoch(Uuid::now_v7()),
    };
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let (mut task, caller) =
        SessionOwnerTask::new(address, clock, NonZeroUsize::MIN, NonZeroUsize::MIN);
    let operation = Uuid::now_v7();
    for event in [
        OwnerEvent::Command(SessionOwnerCommand::Create { operation }),
        OwnerEvent::Create(CreateProgress::ProviderId {
            operation,
            provider_id: "fixture-session".into(),
        }),
        OwnerEvent::Create(CreateProgress::Adopted {
            operation,
            facts: Box::default(),
        }),
    ] {
        task.machine
            .handle(
                OwnerMailboxEvent {
                    owner: address,
                    event,
                },
                0,
            )
            .expect("commit an owner transition without publishing the watch");
    }
    assert!(matches!(
        task.machine.snapshot().lifecycle,
        SessionLifecycle::Live { .. }
    ));
    assert!(matches!(
        caller.snapshots().borrow().lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    (task, caller, address)
}

type Acknowledgement = oneshot::Receiver<Result<(), OwnerCommandRefusal>>;

struct OutboxOrderProbe {
    order: Arc<std::sync::Mutex<Vec<&'static str>>>,
    snapshot: watch::Receiver<SessionSnapshot>,
    acknowledgement: Arc<std::sync::Mutex<Acknowledgement>>,
}

impl SessionOwnerPorts for OutboxOrderProbe {
    fn execute(
        &mut self,
        _: OwnerAddress,
        effect: OwnerEffect,
        _: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        if matches!(effect, OwnerEffect::RetireRoute(_)) {
            assert!(
                matches!(
                    self.snapshot.borrow().lifecycle,
                    SessionLifecycle::NotLoaded { .. }
                ),
                "ordinary outbox effect must still see the old snapshot"
            );
            self.order
                .lock()
                .map_err(|_| OwnerTaskError::ExecutorStopped)?
                .push("ordinary effect");
            return Ok(Some(OwnerEvent::ConnectionLost));
        }
        let wake = match effect {
            OwnerEffect::RestoreSettled { .. } => "restore result",
            OwnerEffect::InputQueued(_) => "queued acceptance",
            _ => return Err(OwnerTaskError::ExecutorStopped),
        };
        self.order
            .lock()
            .map_err(|_| OwnerTaskError::ExecutorStopped)?
            .push("prepare caller");
        let order = self.order.clone();
        let snapshot = self.snapshot.clone();
        let acknowledgement = self.acknowledgement.clone();
        caller_wakes.observe_send(move || {
            record_ordered_wake(&snapshot, &acknowledgement, &order, wake);
        });
        Ok(Some(OwnerEvent::Create(CreateProgress::Failed {
            operation: Uuid::now_v7(),
            remote: RemoteEffect::None,
        })))
    }
}

fn record_ordered_wake(
    snapshot: &watch::Receiver<SessionSnapshot>,
    acknowledgement: &std::sync::Mutex<Acknowledgement>,
    order: &std::sync::Mutex<Vec<&'static str>>,
    wake: &'static str,
) {
    assert!(
        matches!(snapshot.borrow().lifecycle, SessionLifecycle::Live { .. }),
        "caller wake preceded publication"
    );
    assert!(matches!(
        acknowledgement.lock().expect("acknowledgement").try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    order.lock().expect("order").push(wake);
}

fn assert_outbox_wake_order(
    effects: impl FnOnce(provider_session_machines::SessionGeneration) -> [OwnerEffect; 2],
    wakes: [&'static str; 2],
) {
    let (mut task, handle, owner) = unpublished_live_owner();
    let SessionLifecycle::Live { generation } = task.machine.snapshot().lifecycle else {
        panic!("prepared owner must be live");
    };
    let (accepted, acknowledgement) = oneshot::channel();
    let acknowledgement = Arc::new(std::sync::Mutex::new(acknowledgement));
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut ports = OutboxOrderProbe {
        order: order.clone(),
        snapshot: handle.snapshots(),
        acknowledgement: acknowledgement.clone(),
    };

    task.execute_outbox(
        OwnerOutbox {
            owner,
            effects: effects(generation)
                .into_iter()
                .chain([OwnerEffect::RetireRoute(generation)])
                .collect(),
        },
        Some(accepted),
        &mut ports,
        |event| {
            order.lock().expect("order").push(match event.event {
                OwnerEvent::Create(_) => "first immediate event",
                OwnerEvent::ConnectionLost => "second immediate event",
                _ => panic!("unexpected immediate event"),
            })
        },
    )
    .expect("carry out the committed outbox");

    assert_eq!(
        *order.lock().expect("order"),
        [
            "prepare caller",
            "first immediate event",
            "prepare caller",
            "first immediate event",
            "ordinary effect",
            "second immediate event",
            wakes[0],
            wakes[1]
        ]
    );
    acknowledgement
        .lock()
        .expect("acknowledgement")
        .try_recv()
        .expect("acknowledged after publication and wakes")
        .expect("command accepted");
}

fn restore_effect(generation: provider_session_machines::SessionGeneration) -> OwnerEffect {
    use provider_session_machines::session_lifecycle_machine::LoadAttempt;
    use provider_session_machines::session_owner::{
        RestoreDisposition, RestorePhase, RestoreResult, RunAttempt,
    };

    OwnerEffect::RestoreSettled {
        operation: Uuid::now_v7(),
        result: RestoreResult {
            attempt: RunAttempt {
                generation,
                attempt: LoadAttempt(Uuid::now_v7()),
            },
            phase: RestorePhase::Attempted,
            disposition: RestoreDisposition::Restored {
                earlier_unanswered: false,
            },
            observed: Default::default(),
        },
    }
}

fn queued_effect(generation: provider_session_machines::SessionGeneration) -> OwnerEffect {
    use provider_session_machines::{session_owner::OwnerInput, session_turn_machine::TurnId};

    OwnerEffect::InputQueued(OwnerInput {
        generation,
        operation: Uuid::now_v7(),
        input: session_event_model::InputId::generate(),
        turn: TurnId(Uuid::now_v7()),
    })
}

#[test]
fn restore_reply_wakes_after_the_outbox_and_publication() {
    assert_outbox_wake_order(
        |generation| [restore_effect(generation), queued_effect(generation)],
        ["restore result", "queued acceptance"],
    );
}

#[test]
fn queued_acceptance_wakes_after_the_outbox_and_publication() {
    assert_outbox_wake_order(
        |generation| [queued_effect(generation), restore_effect(generation)],
        ["queued acceptance", "restore result"],
    );
}

struct FailingEffectProbe {
    prepared: Option<oneshot::Sender<&'static str>>,
    unprepared: Option<oneshot::Sender<&'static str>>,
    recovered: mpsc::UnboundedSender<oneshot::Sender<&'static str>>,
}

impl SessionOwnerPorts for FailingEffectProbe {
    fn execute(
        &mut self,
        _: OwnerAddress,
        effect: OwnerEffect,
        _: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        if matches!(effect, OwnerEffect::RetireRoute(_)) {
            return Err(OwnerTaskError::ExecutorStopped);
        }
        let recovered = self.recovered.clone();
        let sender = self.prepared.take().expect("one prepared wake");
        caller_wakes.send_or_recover(sender, "success", move |sender| {
            let _test_gone = recovered.send(sender);
        });
        Ok(None)
    }
}

#[test]
fn failed_effect_returns_prepared_caller_to_recovery_without_publishing() {
    let (mut task, handle, owner) = unpublished_live_owner();
    let SessionLifecycle::Live { generation } = task.machine.snapshot().lifecycle else {
        panic!("prepared owner must be live");
    };
    let (prepared, mut prepared_reply) = oneshot::channel();
    let (unprepared, mut unprepared_reply) = oneshot::channel();
    let (recovered, mut recovered_senders) = mpsc::unbounded_channel();
    let (acknowledge, mut acknowledgement) = oneshot::channel();
    let mut ports = FailingEffectProbe {
        prepared: Some(prepared),
        unprepared: Some(unprepared),
        recovered,
    };

    assert_eq!(
        task.execute_outbox(
            OwnerOutbox {
                owner,
                effects: vec![
                    OwnerEffect::CreateSettled {
                        operation: Uuid::now_v7(),
                        outcome: CreateOutcome::Adopted(generation)
                    },
                    OwnerEffect::RetireRoute(generation),
                ]
            },
            Some(acknowledge),
            &mut ports,
            |_| panic!("no immediate events"),
        ),
        Err(OwnerTaskError::ExecutorStopped)
    );

    assert!(matches!(
        handle.snapshots().borrow().lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    assert_eq!(
        prepared_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );
    assert!(matches!(
        acknowledgement.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    recovered_senders
        .try_recv()
        .expect("return the prepared caller to recovery")
        .send("owner failed")
        .expect("caller receives the recovery answer");
    assert_eq!(prepared_reply.try_recv(), Ok("owner failed"));
    // A caller whose effect never ran still belongs to the failed executor.
    drop(ports.unprepared.take());
    assert_eq!(
        unprepared_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    );
}
