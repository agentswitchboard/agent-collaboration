use super::*;
use provider_session_machines::{
    SessionGeneration,
    session_lifecycle_machine::{CloseAttempt, SessionLifecycle},
    session_owner::{
        ConnectionEpoch, CreateProgress, OwnerIncarnation, PublicationBatch, PublicationFact,
        PublicationResult, PublicationStatus, SessionWireRequest,
    },
};
use router_clock::ManualClock;
use session_event_model::SessionEvent;
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, Eq, PartialEq)]
enum ObservedIngress {
    Rebase,
    Item(String),
    Close,
}

type NestedSendObservation = Rc<RefCell<Option<Poll<Result<(), OwnerTaskError>>>>>;

struct TracingPorts {
    trace: Rc<RefCell<Vec<ObservedIngress>>>,
    refuse_creation: bool,
    fail_stream: bool,
    nested_send: NestedSendObservation,
}

fn item(generation: SessionGeneration, item_id: &str) -> OwnerEvent {
    OwnerEvent::Stream {
        run: generation,
        expected_turn: None,
        event: SessionEvent::ItemCompleted {
            item_id: item_id.to_owned(),
        },
    }
}

fn poll_once<Output>(future: Pin<&mut (impl Future<Output = Output> + ?Sized)>) -> Poll<Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

impl SessionOwnerPorts for TracingPorts {
    fn execute(
        &mut self,
        owner: OwnerAddress,
        effect: OwnerEffect,
        completions: &OwnerCompletionPort,
        _: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        Ok(match effect {
            OwnerEffect::Wire(SessionWireRequest::Create { operation }) => {
                Some(OwnerEvent::Create(CreateProgress::ProviderId {
                    operation,
                    provider_id: "carrier-order-session".into(),
                }))
            }
            OwnerEffect::AdoptProviderId { operation, .. } => {
                Some(OwnerEvent::Create(CreateProgress::Configured {
                    operation,
                    facts: Box::default(),
                }))
            }
            OwnerEffect::ActivateCreated {
                operation, facts, ..
            } => Some(OwnerEvent::Create(CreateProgress::Activated {
                operation,
                facts,
            })),
            OwnerEffect::Publish(PublicationBatch::Rebase { id, .. }) => {
                self.trace.borrow_mut().push(ObservedIngress::Rebase);
                Some(OwnerEvent::Publication(PublicationResult::Applied(id)))
            }
            OwnerEffect::Publish(PublicationBatch::Commit { id, facts }) => {
                for fact in facts {
                    if let PublicationFact::Stream {
                        run,
                        event: SessionEvent::ItemCompleted { item_id },
                    } = fact
                    {
                        self.trace.borrow_mut().push(ObservedIngress::Item(item_id));
                        if self.fail_stream {
                            let retirement = CancellationToken::new();
                            let mut nested = Box::pin(completions.send_stream(
                                owner,
                                item(run, "nested"),
                                &retirement,
                            ));
                            *self.nested_send.borrow_mut() = Some(poll_once(nested.as_mut()));
                            return Err(OwnerTaskError::ExecutorStopped);
                        }
                    }
                }
                Some(OwnerEvent::Publication(
                    if std::mem::take(&mut self.refuse_creation) {
                        PublicationResult::Refused(id)
                    } else {
                        PublicationResult::Applied(id)
                    },
                ))
            }
            OwnerEffect::Wire(SessionWireRequest::Close(_)) => {
                self.trace.borrow_mut().push(ObservedIngress::Close);
                None
            }
            _ => None,
        })
    }
}

type OwnerRunFuture = Pin<Box<dyn Future<Output = Result<(), OwnerTaskError>>>>;

struct OrderingFixture {
    clock: Arc<ManualClock>,
    owner: OwnerAddress,
    generation: SessionGeneration,
    handle: SessionOwnerHandle,
    shutdown: CancellationToken,
    task: OwnerRunFuture,
    trace: Rc<RefCell<Vec<ObservedIngress>>>,
    nested_send: NestedSendObservation,
}

impl OrderingFixture {
    fn start(fail_stream: bool) -> Self {
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let owner = OwnerAddress {
            incarnation: OwnerIncarnation(Uuid::from_u128(
                0x019f_0000_0000_7000_8000_0000_0000_0001,
            )),
            connection_epoch: ConnectionEpoch(Uuid::from_u128(
                0x019f_0000_0000_7000_8000_0000_0000_0002,
            )),
        };
        let trace = Rc::new(RefCell::new(Vec::new()));
        let nested_send = Rc::new(RefCell::new(None));
        let (task, handle) = SessionOwnerTask::new(
            owner,
            clock.clone(),
            NonZeroUsize::MIN,
            NonZeroUsize::new(3).expect("three slots"),
        );
        let shutdown = CancellationToken::new();
        let mut task: OwnerRunFuture = Box::pin(task.run(
            TracingPorts {
                trace: trace.clone(),
                refuse_creation: !fail_stream,
                fail_stream,
                nested_send: nested_send.clone(),
            },
            shutdown.clone(),
        ));
        let mut accepted = handle
            .try_submit(
                owner,
                SessionOwnerCommand::Create {
                    operation: Uuid::from_u128(0x019f_0000_0000_7000_8000_0000_0000_0003),
                },
            )
            .expect("create submission");
        assert_eq!(poll_once(task.as_mut()), Poll::Pending);
        assert!(matches!(accepted.try_recv(), Ok(Ok(()))));
        let snapshot = handle.snapshots().borrow().clone();
        let SessionLifecycle::Live { generation } = snapshot.lifecycle else {
            panic!("the actual create must be live");
        };
        if !fail_stream {
            assert!(matches!(
                snapshot.publication,
                PublicationStatus::Degraded { .. }
            ));
            assert!(clock.has_not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(10))));
        }
        trace.borrow_mut().clear();
        Self {
            clock,
            owner,
            generation,
            handle,
            shutdown,
            task,
            trace,
            nested_send,
        }
    }

    fn fill_then_expire(&self) -> oneshot::Receiver<Result<(), OwnerCommandRefusal>> {
        let closing = SessionOwnerCommand::Close {
            generation: self.generation,
            attempt: CloseAttempt(Uuid::from_u128(0x019f_0000_0000_7000_8000_0000_0000_0004)),
        };
        let accepted = self
            .handle
            .try_submit(self.owner, closing.clone())
            .expect("close submission");
        assert!(matches!(
            self.handle.try_submit(self.owner, closing),
            Err(OwnerMailboxUnavailable::Busy)
        ));

        let port = self.handle.completion_port();
        let stale = OwnerAddress {
            connection_epoch: ConnectionEpoch(Uuid::nil()),
            ..self.owner
        };
        let reply = port
            .reserve(self.owner)
            .expect("reserve original owner before sending stream");
        let stale_reply = port.reserve(stale).expect("reserve unrelated address");
        let live_scope = CancellationToken::new();
        let mut observation =
            Box::pin(port.send_stream(self.owner, item(self.generation, "stream"), &live_scope));
        assert_eq!(poll_once(observation.as_mut()), Poll::Ready(Ok(())));
        reply.send(item(self.generation, "reply"));
        stale_reply.send(item(self.generation, "stale address"));
        assert!(matches!(
            port.reserve(self.owner),
            Err(OwnerTaskError::CompletionCapacity)
        ));
        self.clock
            .advance(Duration::from_secs(10))
            .expect("rebase is now due");
        accepted
    }
}

#[test]
fn owner_task_reduces_due_deadline_then_fifo_ingress_then_the_full_business_mailbox() {
    let mut fixture = OrderingFixture::start(false);
    let mut accepted = fixture.fill_then_expire();
    let poll = poll_once(fixture.task.as_mut());
    drop(fixture.task);
    assert_eq!(poll, Poll::Pending);
    assert_eq!(
        *fixture.trace.borrow(),
        [
            ObservedIngress::Rebase,
            ObservedIngress::Item("stream".into()),
            ObservedIngress::Item("reply".into()),
            ObservedIngress::Close,
        ],
        "real owner must honor deadline, FIFO ingress, then business command and reject the stale stamp"
    );
    assert!(matches!(accepted.try_recv(), Ok(Ok(()))));
    assert!(matches!(
        fixture.handle.snapshots().borrow().lifecycle,
        SessionLifecycle::Closing { .. }
    ));
}

#[test]
fn owner_task_shutdown_wins_a_ready_deadline_full_ingress_and_command() {
    let mut fixture = OrderingFixture::start(false);
    let mut accepted = fixture.fill_then_expire();
    fixture.shutdown.cancel();
    let poll = poll_once(fixture.task.as_mut());
    drop(fixture.task);

    assert_eq!(
        poll,
        Poll::Ready(Ok(())),
        "shutdown ends the owner before any other ready arm"
    );
    assert!(
        fixture.trace.borrow().is_empty(),
        "shutdown cannot promise observation of queued replies or events"
    );
    assert!(matches!(
        accepted.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}

#[test]
fn owner_task_keeps_stream_credit_while_a_real_outbox_effect_fails() {
    let mut fixture = OrderingFixture::start(true);
    let port = fixture.handle.completion_port();
    let live_scope = CancellationToken::new();
    assert_eq!(
        poll_once(
            Box::pin(port.send_stream(
                fixture.owner,
                item(fixture.generation, "outbox failure"),
                &live_scope
            ))
            .as_mut()
        ),
        Poll::Ready(Ok(()))
    );
    let result = poll_once(fixture.task.as_mut());
    drop(fixture.task);

    assert_eq!(result, Poll::Ready(Err(OwnerTaskError::ExecutorStopped)));
    assert_eq!(
        *fixture.nested_send.borrow(),
        Some(Poll::Pending),
        "the real task still owns stream credit during effect processing"
    );
}
