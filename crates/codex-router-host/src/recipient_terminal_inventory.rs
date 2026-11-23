//! Shares a single blocking registry census between overlapping custody lookups.

use claude_code_peer_messaging::{ClaudeCodeSessionRegistry, PeerSessionCensus};
use collaboration_protocol::SessionId as PeerSessionId;
use router_clock::RuntimeClock;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use switchboard_message_custody::recipient_sessions::{
    InventoryRead, SessionId, SessionInventory, SessionInventoryAnswer,
};
use tokio::sync::oneshot;

const PROCESS_START_BUDGET: Duration = Duration::from_secs(5);
const UNAVAILABLE: SessionInventoryAnswer = SessionInventoryAnswer::AtLeast(0);

type ScanWaiters = HashMap<PeerSessionId, Vec<oneshot::Sender<SessionInventoryAnswer>>>;

struct InFlightScan {
    waiters: Mutex<Option<ScanWaiters>>,
}

impl InFlightScan {
    fn subscribe(
        &self,
        session_id: &PeerSessionId,
    ) -> Option<oneshot::Receiver<SessionInventoryAnswer>> {
        let mut waiters = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (sender, receiver) = oneshot::channel();
        waiters
            .as_mut()?
            .entry(session_id.clone())
            .or_default()
            .push(sender);
        Some(receiver)
    }

    fn resolve(&self, census: &PeerSessionCensus, clock: &dyn RuntimeClock) {
        let runtime = tokio::runtime::Handle::current();
        let mut answers = HashMap::new();
        while let Some(target) = self.next_target(&answers) {
            let answer =
                runtime.block_on(census.answer(&target, || clock.sleep_for(PROCESS_START_BUDGET)));
            let answer = if answer.complete {
                SessionInventoryAnswer::Complete(answer.at_least)
            } else {
                SessionInventoryAnswer::AtLeast(answer.at_least)
            };
            answers.insert(target, answer);
        }
    }

    fn next_target(
        &self,
        answers: &HashMap<PeerSessionId, SessionInventoryAnswer>,
    ) -> Option<PeerSessionId> {
        let mut waiters = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = waiters.as_mut()?;
        pending.retain(|_, subscribers| {
            subscribers.retain(|sender| !sender.is_closed());
            !subscribers.is_empty()
        });
        if let Some(target) = pending.keys().find(|target| !answers.contains_key(*target)) {
            return Some(target.clone());
        }
        for (target, subscribers) in waiters.take()? {
            let answer = answers.get(&target).copied().unwrap_or(UNAVAILABLE);
            for subscriber in subscribers {
                let _ = subscriber.send(answer);
            }
        }
        None
    }
}

pub struct ClaudeTerminalSessionInventory {
    registry: Arc<ClaudeCodeSessionRegistry>,
    clock: Arc<dyn RuntimeClock>,
    in_flight: Mutex<Weak<InFlightScan>>,
}

impl ClaudeTerminalSessionInventory {
    #[must_use]
    pub fn new(registry: Arc<ClaudeCodeSessionRegistry>, clock: Arc<dyn RuntimeClock>) -> Self {
        Self {
            registry,
            clock,
            in_flight: Mutex::default(),
        }
    }

    fn query(&self, session_id: &PeerSessionId) -> oneshot::Receiver<SessionInventoryAnswer> {
        self.query_with(session_id, |scan| {
            let registry = Arc::clone(&self.registry);
            let clock = Arc::clone(&self.clock);
            tokio::task::spawn_blocking(move || {
                let census = PeerSessionCensus::scan(&registry);
                scan.resolve(&census, clock.as_ref());
            });
        })
    }

    fn query_with(
        &self,
        session_id: &PeerSessionId,
        spawn: impl FnOnce(Arc<InFlightScan>),
    ) -> oneshot::Receiver<SessionInventoryAnswer> {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(receiver) = in_flight
            .upgrade()
            .and_then(|scan| scan.subscribe(session_id))
        {
            return receiver;
        }
        let (sender, receiver) = oneshot::channel();
        let scan = Arc::new(InFlightScan {
            waiters: Mutex::new(Some(HashMap::from([(session_id.clone(), vec![sender])]))),
        });
        *in_flight = Arc::downgrade(&scan);
        spawn(scan);
        receiver
    }
}

impl SessionInventory for ClaudeTerminalSessionInventory {
    fn lookup_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> InventoryRead<'lookup> {
        Box::pin(async move {
            let Ok(session_id) = PeerSessionId::try_from(session_id.as_str().to_owned()) else {
                return UNAVAILABLE;
            };
            self.query(&session_id).await.unwrap_or(UNAVAILABLE)
        })
    }
}

#[cfg(test)]
#[path = "recipient_terminal_inventory_test.rs"]
mod tests;
