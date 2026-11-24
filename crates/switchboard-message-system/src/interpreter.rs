use std::{
    collections::BTreeSet,
    future::{Future, pending},
    path::PathBuf,
    sync::Arc,
};

use futures_util::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::FuturesUnordered};
use router_clock::RuntimeClock;
use state_machine_core::{EventTag as _, StateMachine as _, StateTag as _};
use switchboard_central_link::link_policy::LinkStatus;
use switchboard_link_lease::LeaseWorkerExit;
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::{
    CredentialStore as _, EnrollmentContext, EnrollmentMetadata, MachineId, SignInDeployment,
};
use switchboard_machine_enrollment_store::{EnrollmentWorkerExit, MachineEnrollmentStore};
use switchboard_message_custody::{
    custody_task::CustodyEventSink, recipient_sessions::RecipientSessionDirectory,
};
use tokio::sync::{mpsc, watch};

use crate::{
    ActiveEnrollment, Effect, EnrollmentId, EnrollmentPair, Event, Fault, Front, FrontRefusal,
    Generation, MessageSystemStatus, Metadata, ReadId, State, StopBudgets, StopTarget, Supervisor,
    UnitPiece, message_system_status,
};
use crate::{
    message_front::{ForwardingMessageFront, FrontPublication},
    message_unit::{MessageUnit, MessageUnitBuildError, OwnedLinkTask, UnitBuildInputs},
    stop_part::stop_part,
};

/// Root paths for one Router's persistent message and credential data.
#[derive(Clone)]
pub struct MessageRoots {
    pub(crate) state: PathBuf,
    pub(crate) secrets: PathBuf,
}

impl MessageRoots {
    #[must_use]
    pub fn new(state: PathBuf, secrets: PathBuf) -> Self {
        Self { state, secrets }
    }
}

/// Existing operator events emitted by the message system's pieces.
pub struct MessageSystemEvents {
    custody: Arc<dyn CustodyEventSink>,
}

impl MessageSystemEvents {
    #[must_use]
    pub fn new(custody: Arc<dyn CustodyEventSink>) -> Self {
        Self { custody }
    }
}

/// Join evidence available when the Supervisor releases its inputs.
#[derive(Debug, Eq, PartialEq)]
pub enum CloseReport {
    Closed,
    ClosedWithOutstanding { pieces: BTreeSet<StopTarget> },
    InterpreterLost,
}

pub(crate) struct MessageInputs {
    pub(crate) roots: MessageRoots,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    pub(crate) recipients: Arc<dyn RecipientSessionDirectory>,
    pub(crate) events: MessageSystemEvents,
}

const STOP_BUDGETS: StopBudgets = StopBudgets {
    driver: std::time::Duration::from_secs(5),
    custody: std::time::Duration::from_secs(5),
    settings: std::time::Duration::from_secs(2),
    lease_adapter: std::time::Duration::from_secs(5),
    enrollment_store: std::time::Duration::from_secs(5),
};

pub(crate) struct OpenedEnrollment {
    pub(crate) machine_id: MachineId,
    pub(crate) store: MachineEnrollmentStore,
    changes: watch::Receiver<u64>,
}

impl OpenedEnrollment {
    pub(crate) fn new(machine_id: MachineId, store: MachineEnrollmentStore) -> Self {
        let changes = store.subscribe_enrollment_changes();
        Self {
            machine_id,
            store,
            changes,
        }
    }
}

pub(crate) struct Interpreter {
    pub(crate) inputs: MessageInputs,
    pub(crate) front: FrontPublication,
    pub(crate) status: watch::Sender<MessageSystemStatus>,
    pub(crate) events: mpsc::UnboundedSender<Event>,
    pub(crate) queue: mpsc::UnboundedReceiver<Event>,
    link_publication: watch::Sender<LinkStatus>,
    link_status: watch::Receiver<LinkStatus>,
    enrollment: Option<OpenedEnrollment>,
    unit: Option<(Generation, MessageUnit)>,
    timer: Option<BoxFuture<'static, ReadId>>,
    exits: FuturesUnordered<BoxFuture<'static, Option<Event>>>,
    stops: FuturesUnordered<BoxFuture<'static, ()>>,
}

impl Interpreter {
    pub(crate) fn new(
        inputs: MessageInputs,
        front: FrontPublication,
        status: watch::Sender<MessageSystemStatus>,
        events: mpsc::UnboundedSender<Event>,
        queue: mpsc::UnboundedReceiver<Event>,
        enrollment: Option<OpenedEnrollment>,
    ) -> Self {
        let (link_publication, link_status) = watch::channel(LinkStatus::NotEnrolled);
        let interpreter = Self {
            inputs,
            front,
            status,
            events,
            queue,
            link_publication,
            link_status,
            enrollment,
            unit: None,
            timer: None,
            exits: FuturesUnordered::new(),
            stops: FuturesUnordered::new(),
        };
        if let Some(enrollment) = &interpreter.enrollment {
            interpreter.observe_store_exit(enrollment.store.clone());
        }
        interpreter
    }

    pub(crate) async fn run<Build: Future<Output = Result<MessageUnit, MessageUnitBuildError>>>(
        mut self,
        mut supervisor: Supervisor,
        mut effects: Vec<Effect>,
        build: impl Fn(UnitBuildInputs) -> Build,
    ) -> CloseReport {
        loop {
            for effect in effects {
                self.apply(effect, supervisor.state(), &build).await;
            }
            self.publish_status(supervisor.state());
            match supervisor.state() {
                State::Closed => return CloseReport::Closed,
                State::ClosedWithOutstanding { pieces } => {
                    return CloseReport::ClosedWithOutstanding {
                        pieces: pieces.clone(),
                    };
                }
                State::Observing { .. }
                | State::Opening { .. }
                | State::Running { .. }
                | State::Quiescing(_)
                | State::Faulted { .. } => {}
            }
            effects = loop {
                let event = tokio::select! {
                    biased;
                    event = self.queue.recv() => event,
                    read = timer_fired(&mut self.timer) => {
                        self.timer = None;
                        if supervisor.wants_metadata_read(read) {
                            let metadata = self.read_metadata().await;
                            let _ = self.events.send(Event::MetadataObserved { read, metadata });
                        }
                        None
                    }
                    () = enrollment_changed(&mut self.enrollment) => Some(Event::EnrollmentMayHaveChanged),
                    exited = self.exits.next(), if !self.exits.is_empty() => exited.flatten(),
                    _ = self.stops.next(), if !self.stops.is_empty() => None,
                    _ = self.link_status.changed() => {
                        self.publish_status(supervisor.state());
                        None
                    }
                };
                if let Some(event) = event {
                    let tag = event.event_tag();
                    match Supervisor::transition(&supervisor, event) {
                        Ok((next, effects)) => {
                            supervisor = next;
                            break effects;
                        }
                        Err(refused) => tracing::debug!(
                            event = tag,
                            state = supervisor.state_tag(),
                            ?refused,
                            "Supervisor refused message system event"
                        ),
                    }
                }
            };
        }
    }

    async fn apply<Build: Future<Output = Result<MessageUnit, MessageUnitBuildError>>>(
        &mut self,
        effect: Effect,
        state: &State,
        build: &impl Fn(UnitBuildInputs) -> Build,
    ) {
        match effect {
            Effect::ScheduleMetadataRead { read, after } => {
                let clock = self.inputs.clock.clone();
                let deadline = clock.monotonic_now().after(after);
                self.timer = Some(
                    async move {
                        clock.sleep_until(deadline).await;
                        read
                    }
                    .boxed(),
                );
            }
            Effect::StartUnit {
                generation,
                pair,
                retired_enrollments,
            } => {
                self.publish_status(state);
                self.unit = None;
                self.link_publication.send_replace(LinkStatus::NotEnrolled);
                let event = match self.unit_inputs(&pair, retired_enrollments) {
                    Some(inputs) => match build(inputs).await {
                        Ok(unit) => {
                            self.observe_unit_exits(generation, &unit);
                            self.unit = Some((generation, unit));
                            Event::BuildFinished { generation }
                        }
                        Err(_) => Event::BuildFailed { generation },
                    },
                    None => Event::BuildFailed { generation },
                };
                let _ = self.events.send(event);
            }
            Effect::StopAndJoin { target, budget } => {
                let unit = match target {
                    StopTarget::Unit { generation, .. } => self
                        .unit
                        .as_ref()
                        .filter(|(current, _)| *current == generation)
                        .map(|(_, unit)| unit),
                    StopTarget::EnrollmentStore => None,
                };
                self.stops.push(stop_part(
                    target,
                    budget,
                    unit,
                    self.enrollment.as_ref().map(|opened| &opened.store),
                    self.inputs.clock.clone(),
                    self.events.clone(),
                ));
            }
            Effect::PublishFront(front) => {
                let door = match front {
                    Front::Refusing(reason) => Err(reason),
                    Front::Forwarding(requested) => {
                        match (self.unit.as_ref(), self.enrollment.as_ref()) {
                            (Some((held, unit)), Some(enrollment)) if *held == requested => {
                                Ok(ForwardingMessageFront {
                                    calls: unit.front.clone(),
                                    machine_id: enrollment.machine_id.clone(),
                                })
                            }
                            (held, _) => {
                                tracing::error!(
                                    class = "forwarding_generation_mismatch",
                                    requested_generation = requested.0.get(),
                                    held_generation = ?held.map(|(generation, _)| generation.0.get()),
                                    "Mac message system could not publish the Supervisor's forwarding front"
                                );
                                Err(FrontRefusal::CouldNotStart)
                            }
                        }
                    }
                };
                let _ = self.front.send_replace(door);
            }
            Effect::ReportFault(fault) => {
                let class = match fault {
                    Fault::DifferentPair { .. } => "different_enrollment_pair",
                    Fault::BuildFailed { .. } => "build_failed",
                    Fault::PieceExited { .. } => "piece_exited",
                    Fault::EnrollmentStoreExited => "enrollment_store_exited",
                    Fault::StopTimedOut { .. } => "stop_timed_out",
                };
                tracing::warn!(class, "Mac message system supervisor fault");
            }
            Effect::ReleaseInputs => self.queue.close(),
        }
    }

    fn publish_status(&self, state: &State) {
        let status = message_system_status(state, &self.link_status.borrow());
        self.status.send_if_modified(|published| {
            if *published == status {
                return false;
            }
            *published = status;
            true
        });
    }

    async fn read_metadata(&mut self) -> Metadata {
        if self.enrollment.is_none() {
            let root = &self.inputs.roots.state;
            let clock = &self.inputs.clock;
            let Ok(machine_id) =
                load_or_create_machine_id(root, clock.utc_now().timestamp_millis()).await
            else {
                return Metadata::ReadFailed;
            };
            let Ok(store) = MachineEnrollmentStore::open(
                &root.join(MACHINE_LOCAL_DATABASE),
                self.inputs.roots.secrets.clone(),
                machine_id.clone(),
                clock.clone(),
            )
            .await
            else {
                return Metadata::ReadFailed;
            };
            self.observe_store_exit(store.clone());
            self.enrollment = Some(OpenedEnrollment::new(machine_id, store));
        }
        let Some(opened) = &self.enrollment else {
            return Metadata::ReadFailed;
        };
        match opened.store.read_metadata().await {
            Ok(EnrollmentMetadata::Empty) => Metadata::Empty,
            Ok(EnrollmentMetadata::Ended { .. }) => Metadata::Ended,
            Ok(EnrollmentMetadata::Active {
                key, deployment, ..
            }) => {
                let active = (|| {
                    let organization = key.organization_id.to_string().parse().ok()?;
                    let enrollment = key.enrollment_id.to_string().parse().ok()?;
                    let pair =
                        EnrollmentPair::new(organization, deployment.origin().clone()).ok()?;
                    Some(ActiveEnrollment { pair, enrollment })
                })();
                active.map_or(Metadata::ReadFailed, Metadata::Active)
            }
            Err(_) => Metadata::ReadFailed,
        }
    }

    fn unit_inputs(
        &self,
        pair: &EnrollmentPair,
        retired_enrollments: BTreeSet<EnrollmentId>,
    ) -> Option<UnitBuildInputs> {
        let opened = self.enrollment.as_ref()?;
        let deployment = SignInDeployment::new(pair.deployment().parse().ok()?).ok()?;
        let organization_id = pair.organization().to_string().parse().ok()?;
        let excluded = retired_enrollments
            .into_iter()
            .map(|id| id.to_string().parse())
            .collect::<Result<_, _>>()
            .ok()?;
        Some(UnitBuildInputs {
            root: self.inputs.roots.state.clone(),
            store: opened.store.clone(),
            context: EnrollmentContext {
                machine_id: opened.machine_id.clone(),
                organization_id,
                deployment,
            },
            excluded,
            clock: self.inputs.clock.clone(),
            recipients: self.inputs.recipients.clone(),
            custody_events: self.inputs.events.custody.clone(),
            link_status: self.link_publication.clone(),
        })
    }

    fn observe_store_exit(&self, store: MachineEnrollmentStore) {
        self.exits.push(
            async move {
                match store.worker_exit().await {
                    Ok(EnrollmentWorkerExit::Requested) => None,
                    Ok(EnrollmentWorkerExit::Autonomous) | Err(_) => {
                        Some(Event::EnrollmentStoreExited)
                    }
                }
            }
            .boxed(),
        );
    }

    fn observe_unit_exits(&self, generation: Generation, unit: &MessageUnit) {
        self.exits
            .push(task_exit(generation, UnitPiece::Driver, &unit.driver));
        self.exits
            .push(task_exit(generation, UnitPiece::Settings, &unit.settings));
        let custody = unit.custody.clone();
        self.exits.push(
            async move {
                custody.admission_stopped().await;
                Some(Event::PieceExited {
                    generation,
                    piece: UnitPiece::Custody,
                })
            }
            .boxed(),
        );
        let lease = unit.lease.clone();
        self.exits.push(
            async move {
                match lease.worker_exit().await {
                    LeaseWorkerExit::Stopped => None,
                    LeaseWorkerExit::Failed(_) | LeaseWorkerExit::Unconfirmed => {
                        Some(Event::PieceExited {
                            generation,
                            piece: UnitPiece::LeaseAdapter,
                        })
                    }
                }
            }
            .boxed(),
        );
    }
}

pub(crate) fn start_supervisor() -> (Supervisor, Vec<Effect>) {
    Supervisor::new(STOP_BUDGETS)
}

async fn timer_fired(timer: &mut Option<BoxFuture<'static, ReadId>>) -> ReadId {
    match timer {
        Some(timer) => timer.await,
        None => pending().await,
    }
}

async fn enrollment_changed(enrollment: &mut Option<OpenedEnrollment>) {
    if let Some(opened) = enrollment
        && opened.changes.changed().await.is_ok()
    {
        return;
    }
    pending().await
}

fn task_exit(
    generation: Generation,
    piece: UnitPiece,
    task: &OwnedLinkTask,
) -> BoxFuture<'static, Option<Event>> {
    let joined = task.joined.clone();
    let requested = task.stop.subscribe();
    async move {
        joined.await;
        (!*requested.borrow()).then_some(Event::PieceExited { generation, piece })
    }
    .boxed()
}
