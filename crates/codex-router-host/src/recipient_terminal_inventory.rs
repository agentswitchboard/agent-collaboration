//! Serializes fresh, target-specific terminal observations for mail custody.

use claude_code_peer_messaging::{
    ClaudeCodeSessionRegistry, ManagedClaudeClaim, ManagedClaudeLaunches, PeerCensusAnswer,
    PeerCensusOutcome, PeerSessionCensus, ProcessIncarnation,
};
use collaboration_protocol::SessionId as PeerSessionId;
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use switchboard_message_custody::{
    custody_operator::CustodyOperatorEvent,
    custody_task::{CustodyEventSink, CustodyTaskEvent},
    recipient_sessions::{
        InventoryRead, RECIPIENT_LOOKUP_BUDGET, SessionId, SessionInventory,
        SessionInventoryAnswer, SessionInventoryPart,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

const PROBE_REAP_BUDGET: Duration = Duration::from_secs(1);
const UNAVAILABLE: SessionInventoryAnswer = SessionInventoryAnswer::AtLeast(0);

pub struct ClaudeTerminalSessionInventory {
    registry: Arc<ClaudeCodeSessionRegistry>,
    clock: Arc<dyn RuntimeClock>,
    events: Arc<dyn CustodyEventSink>,
    managed_launches: Option<Arc<dyn ManagedClaudeLaunches>>,
    permit: Arc<Semaphore>,
}

impl ClaudeTerminalSessionInventory {
    #[must_use]
    pub fn new(
        registry: Arc<ClaudeCodeSessionRegistry>,
        clock: Arc<dyn RuntimeClock>,
        events: Arc<dyn CustodyEventSink>,
    ) -> Self {
        Self {
            registry,
            clock,
            events,
            managed_launches: None,
            permit: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) fn with_managed_launches(
        mut self,
        launches: Option<Arc<dyn ManagedClaudeLaunches>>,
    ) -> Self {
        self.managed_launches = launches;
        self
    }

    fn lookup<'lookup>(
        &'lookup self,
        target: &'lookup SessionId,
        deadline: Option<ClockInstant>,
    ) -> InventoryRead<'lookup> {
        let Ok(target) = PeerSessionId::try_from(target.as_str().to_owned()) else {
            return Box::pin(std::future::ready(UNAVAILABLE));
        };
        let managed = self.managed_launches.as_ref().and_then(|launches| {
            launches
                .live_adapter(&target)
                .map(|adapter| ObservedManagedLaunch {
                    launches: launches.clone(),
                    adapter,
                })
        });
        Box::pin(async move {
            let registry = Arc::clone(&self.registry);
            self.lookup_with(deadline, move |permit, observation| {
                run_terminal_census(registry, target, managed, permit, observation)
            })
            .await
        })
    }

    async fn lookup_with<TObserve>(
        &self,
        cutoff: Option<ClockInstant>,
        observe: impl FnOnce(OwnedSemaphorePermit, TerminalObservation) -> TObserve,
    ) -> SessionInventoryAnswer
    where
        TObserve: Future<Output = PeerCensusOutcome> + Send + 'static,
    {
        let deadline =
            cutoff.unwrap_or_else(|| self.clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET));
        let budget = self.clock.sleep_until(deadline);
        tokio::pin!(budget);
        let permit = tokio::select! {
            biased;
            () = &mut budget => return UNAVAILABLE,
            permit = Arc::clone(&self.permit).acquire_owned() => permit,
        };
        let Ok(permit) = permit else {
            return UNAVAILABLE;
        };
        if self.clock.monotonic_now() >= deadline {
            return UNAVAILABLE;
        }

        let stop = CancellationToken::new();
        let _stop_on_drop = stop.clone().drop_guard();
        let progress = Arc::new(AtomicUsize::new(0));
        let observation = TerminalObservation {
            deadline,
            clock: Arc::clone(&self.clock),
            stop,
            progress: Arc::clone(&progress),
        };
        let work = observe(permit, observation);
        let events = Arc::clone(&self.events);
        // Cleanup outlives a dropped caller, but the single owned permit
        // bounds both the blocking filesystem work and the process probe.
        let mut worker = tokio::spawn(async move {
            let answer = match work.await {
                PeerCensusOutcome::Answer(answer) => answer,
                PeerCensusOutcome::ProbeReapDeferred { process_id, answer } => {
                    events.record_event(CustodyTaskEvent::Operator(
                        CustodyOperatorEvent::TerminalProbeReapDeferred { process_id },
                    ));
                    answer
                }
            };
            let count = if answer.complete {
                SessionInventoryAnswer::Complete(answer.at_least)
            } else {
                SessionInventoryAnswer::AtLeast(answer.at_least)
            };
            if answer.managed_claude_alias {
                count.with_managed_claude()
            } else {
                count
            }
        });
        tokio::select! {
            biased;
            answer = &mut worker => answer.unwrap_or_else(|_| SessionInventoryAnswer::AtLeast(progress.load(Ordering::Acquire))),
            () = &mut budget => SessionInventoryAnswer::AtLeast(progress.load(Ordering::Acquire)),
        }
    }
}

#[derive(Clone)]
struct TerminalObservation {
    deadline: ClockInstant,
    clock: Arc<dyn RuntimeClock>,
    stop: CancellationToken,
    progress: Arc<AtomicUsize>,
}

impl TerminalObservation {
    fn is_active(&self) -> bool {
        !self.stop.is_cancelled() && self.clock.monotonic_now() < self.deadline
    }

    async fn stopped(&self) {
        tokio::select! {
            () = self.stop.cancelled() => {},
            () = self.clock.sleep_until(self.deadline) => {},
        }
    }
}

struct ObservedManagedLaunch {
    launches: Arc<dyn ManagedClaudeLaunches>,
    adapter: ProcessIncarnation,
}

async fn run_terminal_census(
    registry: Arc<ClaudeCodeSessionRegistry>,
    target: PeerSessionId,
    managed: Option<ObservedManagedLaunch>,
    permit: OwnedSemaphorePermit,
    observation: TerminalObservation,
) -> PeerCensusOutcome {
    let scanner = observation.clone();
    let scanned = tokio::task::spawn_blocking(move || {
        let census = scanner
            .is_active()
            .then(|| PeerSessionCensus::scan(&registry, &target, || scanner.is_active()));
        (permit, census)
    })
    .await;
    let Ok((_permit, Some(census))) = scanned else {
        return incomplete_census();
    };
    if !observation.is_active() {
        return incomplete_census();
    }
    census
        .answer_with_managed_claim(
            managed.as_ref().map(|managed| ManagedClaudeClaim {
                launches: managed.launches.as_ref(),
                adapter: managed.adapter,
            }),
            || observation.is_active(),
            || observation.stopped(),
            || observation.clock.sleep_for(PROBE_REAP_BUDGET),
            |count| observation.progress.store(count, Ordering::Release),
        )
        .await
}

const fn incomplete_census() -> PeerCensusOutcome {
    PeerCensusOutcome::Answer(PeerCensusAnswer {
        at_least: 0,
        complete: false,
        managed_claude_alias: false,
    })
}

impl SessionInventory for ClaudeTerminalSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        target: &'lookup SessionId,
    ) -> InventoryRead<'lookup> {
        self.lookup(target, None)
    }

    fn lookup_parts<'lookup>(
        &'lookup self,
        target: &'lookup SessionId,
        deadline: Option<ClockInstant>,
    ) -> Vec<SessionInventoryPart<'lookup>> {
        vec![SessionInventoryPart {
            observed: UNAVAILABLE,
            read: self.lookup(target, deadline),
        }]
    }
}

#[cfg(test)]
#[path = "recipient_terminal_inventory_test.rs"]
mod tests;
