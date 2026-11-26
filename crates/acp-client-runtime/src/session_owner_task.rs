//! Run one pure provider session owner and execute its committed outbox.

use provider_session_machines::session_owner::{
    OwnerAddress, OwnerDeadline, OwnerEffect, OwnerEvent, OwnerMailboxEvent, OwnerOutbox,
    SessionOwnerCommand, SessionOwnerMachine, SessionOwnerRejection, SessionSnapshot,
};
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};
use switchboard_harness_contracts::{ReplyAdmission, WorkReply, WriterState};
use switchboard_harness_shared_mechanics::{
    CarrierReceiver, CarrierReply, CarrierSender, reply_carrier,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OwnerMailboxUnavailable {
    #[error("session busy; send again")]
    Busy,
    #[error("the session owner stopped")]
    Stopped,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OwnerTaskError {
    #[error("the session owner has too many in-flight effects")]
    CompletionCapacity,
    #[error("an executor needed by the session owner stopped")]
    ExecutorStopped,
}

#[derive(Debug)]
pub struct OwnerCommandRefusal {
    pub rejection: SessionOwnerRejection,
    pub snapshot: Box<SessionSnapshot>,
}

struct Submission {
    address: OwnerAddress,
    command: SessionOwnerCommand,
    accepted: oneshot::Sender<Result<(), OwnerCommandRefusal>>,
}

#[derive(Clone)]
pub struct SessionOwnerHandle {
    commands: mpsc::Sender<Submission>,
    completions: OwnerCompletionPort,
    snapshots: watch::Receiver<SessionSnapshot>,
}

impl SessionOwnerHandle {
    pub fn try_submit(
        &self,
        address: OwnerAddress,
        command: SessionOwnerCommand,
    ) -> Result<oneshot::Receiver<Result<(), OwnerCommandRefusal>>, OwnerMailboxUnavailable> {
        let (accepted, decision) = oneshot::channel();
        self.commands
            .try_send(Submission {
                address,
                command,
                accepted,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => OwnerMailboxUnavailable::Busy,
                mpsc::error::TrySendError::Closed(_) => OwnerMailboxUnavailable::Stopped,
            })?;
        Ok(decision)
    }

    #[must_use]
    pub fn snapshots(&self) -> watch::Receiver<SessionSnapshot> {
        self.snapshots.clone()
    }

    #[must_use]
    pub fn completion_port(&self) -> OwnerCompletionPort {
        self.completions.clone()
    }
}

#[derive(Clone)]
pub struct OwnerCompletionPort {
    carrier: CarrierSender<OwnerAddress, OwnerEvent>,
}

pub struct OwnerCompletion {
    reply: CarrierReply<OwnerAddress, OwnerEvent>,
}

const _: fn() = {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OwnerCompletion>
};

impl OwnerCompletionPort {
    /// Reserve before offering external work. Completion then needs neither
    /// spare mailbox space nor an await, even if the business mailbox is full.
    pub fn reserve(&self, address: OwnerAddress) -> Result<OwnerCompletion, OwnerTaskError> {
        self.carrier
            .reserve_inline(address)
            .map(|reply| OwnerCompletion { reply })
            .map_err(|reason| match reason {
                WriterState::Full => OwnerTaskError::CompletionCapacity,
                WriterState::Stopped => OwnerTaskError::ExecutorStopped,
            })
    }

    pub fn reserve_work_reply(&self, address: OwnerAddress) -> ReplyAdmission<WorkReply> {
        self.carrier.reserve(address, owner_event_for_work_reply)
    }

    /// A run's SDK reader can yield for its own completion channel and stop
    /// if that run is retired.
    pub async fn send_stream(
        &self,
        address: OwnerAddress,
        event: OwnerEvent,
        retired: &CancellationToken,
    ) -> Result<(), OwnerTaskError> {
        self.carrier
            .send_stream(address, event, retired, std::convert::identity)
            .await
            .map_err(|_| OwnerTaskError::ExecutorStopped)
    }
}

impl OwnerCompletion {
    pub fn send(self, event: OwnerEvent) {
        self.reply.send(event);
    }
}

fn owner_event_for_work_reply(reply: WorkReply) -> OwnerEvent {
    match reply {
        WorkReply::Dispatch(result) => OwnerEvent::Dispatch(result),
        WorkReply::ConnectionLost => OwnerEvent::ConnectionLost,
    }
}

/// Effect offers are synchronous. An executor reserves completions before
/// accepting work it will carry out separately. Synchronous results such as
/// revoke are returned directly and reduced before reading another command.
pub trait SessionOwnerPorts {
    fn execute(
        &mut self,
        address: OwnerAddress,
        effect: OwnerEffect,
        completions: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError>;
}

/// Prepared caller deliveries for one reduction, in effect order.
pub struct OwnerCallerWakes {
    deliveries: Vec<Box<dyn FnOnce(bool) + Send>>,
}

impl OwnerCallerWakes {
    fn new() -> Self {
        Self {
            deliveries: Vec::new(),
        }
    }

    /// Prepare a result. An unpublished reduction closes the reply channel.
    pub fn send<T: Send + 'static>(&mut self, sender: oneshot::Sender<T>, value: T) {
        self.send_or_recover(sender, value, drop);
    }

    /// Prepare a result, or return its sender to its owner if publication fails.
    pub fn send_or_recover<T: Send + 'static>(
        &mut self,
        sender: oneshot::Sender<T>,
        value: T,
        recover: impl FnOnce(oneshot::Sender<T>) + Send + 'static,
    ) {
        self.deliveries.push(Box::new(move |published| {
            if published {
                let _caller_gone = sender.send(value);
            } else {
                recover(sender);
            }
        }));
    }

    #[cfg(test)]
    fn observe_send(&mut self, observe: impl FnOnce() + Send + 'static) {
        self.deliveries.push(Box::new(move |published| {
            if published {
                observe();
            }
        }));
    }

    fn publish(mut self) {
        for delivery in self.deliveries.drain(..) {
            delivery(true);
        }
    }
}

impl Drop for OwnerCallerWakes {
    fn drop(&mut self) {
        for delivery in self.deliveries.drain(..) {
            delivery(false);
        }
    }
}

struct ArmedDeadline {
    address: OwnerAddress,
    deadline: OwnerDeadline,
}

/// A caller is never told an outcome before a reader can see the state that produced it.
pub struct SessionOwnerTask {
    machine: SessionOwnerMachine,
    commands: mpsc::Receiver<Submission>,
    completion_port: OwnerCompletionPort,
    completions: CarrierReceiver<OwnerAddress, OwnerEvent>,
    snapshots: watch::Sender<SessionSnapshot>,
    deadlines: BTreeMap<ClockInstant, VecDeque<ArmedDeadline>>,
    clock: Arc<dyn RuntimeClock>,
}

impl SessionOwnerTask {
    pub fn new(
        address: OwnerAddress,
        clock: Arc<dyn RuntimeClock>,
        mailbox_capacity: NonZeroUsize,
        completion_capacity: NonZeroUsize,
    ) -> (Self, SessionOwnerHandle) {
        let machine = SessionOwnerMachine::new(address);
        let (commands, command_rx) = mpsc::channel(mailbox_capacity.get());
        let (carrier, completions) = reply_carrier(completion_capacity);
        let completion_port = OwnerCompletionPort { carrier };
        let (snapshots, snapshot_rx) = watch::channel(machine.snapshot());
        let task = Self {
            machine,
            commands: command_rx,
            completion_port: completion_port.clone(),
            completions,
            snapshots,
            deadlines: BTreeMap::new(),
            clock,
        };
        let handle = SessionOwnerHandle {
            commands,
            completions: completion_port,
            snapshots: snapshot_rx,
        };
        (task, handle)
    }

    pub async fn run(
        mut self,
        mut ports: impl SessionOwnerPorts,
        shutdown: CancellationToken,
    ) -> Result<(), OwnerTaskError> {
        let mut accepting_commands = true;
        loop {
            let next_due = self.deadlines.first_key_value().map(|(time, _)| *time);
            let clock = Arc::clone(&self.clock);
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(()),
                () = wait_for_deadline(clock.as_ref(), next_due) => {
                    if let Some(alarm) = self.pop_deadline() {
                        self.process(OwnerMailboxEvent {
                            owner: alarm.address,
                            event: OwnerEvent::Deadline(alarm.deadline),
                        }, None, &mut ports)?;
                    }
                }
                Some(message) = self.completions.recv() => {
                    message.process(|owner, event| {
                        self.process(OwnerMailboxEvent { owner, event }, None, &mut ports)
                    })?;
                }
                submission = self.commands.recv(), if accepting_commands => {
                    if let Some(Submission { address, command, accepted }) = submission {
                        self.process(OwnerMailboxEvent {
                            owner: address,
                            event: OwnerEvent::Command(command),
                        }, Some(accepted), &mut ports)?;
                    } else {
                        accepting_commands = false;
                    }
                }
            }
        }
    }

    fn process(
        &mut self,
        event: OwnerMailboxEvent,
        accepted: Option<oneshot::Sender<Result<(), OwnerCommandRefusal>>>,
        ports: &mut impl SessionOwnerPorts,
    ) -> Result<(), OwnerTaskError> {
        let mut ready = VecDeque::from([(event, accepted)]);
        while let Some((event, accepted)) = ready.pop_front() {
            let observed_at_ms = self.clock.utc_now().timestamp_millis();
            let outbox = match self.machine.handle(event, observed_at_ms) {
                Ok(outbox) => outbox,
                Err(reason) => {
                    if let Some(accepted) = accepted {
                        let _reader_gone = accepted.send(Err(OwnerCommandRefusal {
                            rejection: reason,
                            snapshot: Box::new(self.machine.snapshot()),
                        }));
                    }
                    continue;
                }
            };
            self.execute_outbox(outbox, accepted, ports, |event| {
                ready.push_back((event, None));
            })?;
        }
        Ok(())
    }

    fn execute_outbox(
        &mut self,
        outbox: OwnerOutbox,
        accepted: Option<oneshot::Sender<Result<(), OwnerCommandRefusal>>>,
        ports: &mut impl SessionOwnerPorts,
        mut enqueue: impl FnMut(OwnerMailboxEvent),
    ) -> Result<(), OwnerTaskError> {
        let committed_at = self.clock.monotonic_now();
        let address = outbox.owner;
        let mut caller_wakes = OwnerCallerWakes::new();
        for effect in outbox.effects {
            if let OwnerEffect::Arm { deadline, budget } = effect {
                self.deadlines
                    .entry(committed_at.after(budget))
                    .or_default()
                    .push_back(ArmedDeadline { address, deadline });
            } else if let Some(event) =
                ports.execute(address, effect, &self.completion_port, &mut caller_wakes)?
            {
                enqueue(OwnerMailboxEvent {
                    owner: address,
                    event,
                });
            }
        }
        self.deadlines.retain(|_, alarms| {
            alarms.retain(|alarm| self.machine.has_pending_deadline(&alarm.deadline));
            !alarms.is_empty()
        });
        let snapshot = self.machine.snapshot();
        self.snapshots.send_if_modified(|published| {
            if *published == snapshot {
                return false;
            }
            *published = snapshot;
            true
        });
        caller_wakes.publish();
        if let Some(accepted) = accepted {
            let _reader_gone = accepted.send(Ok(()));
        }
        Ok(())
    }

    fn pop_deadline(&mut self) -> Option<ArmedDeadline> {
        let mut first = self.deadlines.first_entry()?;
        let alarm = first.get_mut().pop_front();
        if first.get().is_empty() {
            first.remove();
        }
        alarm
    }
}

async fn wait_for_deadline(clock: &dyn RuntimeClock, at: Option<ClockInstant>) {
    if let Some(at) = at {
        clock.sleep_until(at).await;
    } else {
        std::future::pending().await
    }
}

#[cfg(test)]
#[path = "session_owner_task_test.rs"]
mod tests;

#[cfg(test)]
#[path = "session_owner_carrier_test.rs"]
mod carrier_tests;
