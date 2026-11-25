use std::{future::Future, pin::Pin, sync::Arc};

use collaboration_protocol::message_tools::{
    AdmittedLocalSession, MessageOperationFuture, SessionId,
};
use collaboration_service::collaboration_application::message_operations::{
    MessageCallerAdmission, MessageSessionDirectory, MessageSessionPresence, V0MessageAdmission,
};
use router_clock::RuntimeClock;
use switchboard_message_custody::{
    custody_task::{CustodyEventSink, CustodyTaskEvent},
    recipient_sessions::{
        RECIPIENT_LOOKUP_BUDGET, RecipientSessionAnswer, RecipientSessionDirectory,
    },
};
use switchboard_message_system::{MessageFront, MessageRoots, MessageSystem, MessageSystemEvents};

#[derive(Clone)]
pub(crate) struct HostMessageStartup {
    roots: MessageRoots,
    #[cfg(all(debug_assertions, feature = "runtime-test-support"))]
    fixture: Option<HostMessageSystemFixture>,
}

impl From<MessageRoots> for HostMessageStartup {
    fn from(roots: MessageRoots) -> Self {
        Self {
            roots,
            #[cfg(all(debug_assertions, feature = "runtime-test-support"))]
            fixture: None,
        }
    }
}

impl HostMessageStartup {
    #[cfg(all(debug_assertions, feature = "runtime-test-support"))]
    pub(crate) fn with_fixture(mut self, fixture: Option<HostMessageSystemFixture>) -> Self {
        self.fixture = fixture;
        self
    }

    #[cfg(all(debug_assertions, feature = "runtime-test-support"))]
    pub(super) async fn start(
        self,
        clock: Arc<dyn RuntimeClock>,
        directory: Arc<dyn RecipientSessionDirectory>,
        events: MessageSystemEvents,
    ) -> MessageSystem {
        if let Some(fixture) = self.fixture {
            return (fixture.0)(self.roots, clock, directory, events).await;
        }
        MessageSystem::start(self.roots, clock, directory, events)
    }

    #[cfg(not(all(debug_assertions, feature = "runtime-test-support")))]
    pub(super) fn start(
        self,
        clock: Arc<dyn RuntimeClock>,
        directory: Arc<dyn RecipientSessionDirectory>,
        events: MessageSystemEvents,
    ) -> std::future::Ready<MessageSystem> {
        std::future::ready(MessageSystem::start(self.roots, clock, directory, events))
    }
}

#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
type StartFixture = dyn Fn(
        MessageRoots,
        Arc<dyn RuntimeClock>,
        Arc<dyn RecipientSessionDirectory>,
        MessageSystemEvents,
    ) -> Pin<Box<dyn Future<Output = MessageSystem> + Send>>
    + Send
    + Sync;

/// Supplies an isolated, enrolled message system to the Host lifecycle test.
#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
#[derive(Clone)]
pub struct HostMessageSystemFixture(Arc<StartFixture>);

#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
impl HostMessageSystemFixture {
    pub fn new(
        start: impl Fn(
            MessageRoots,
            Arc<dyn RuntimeClock>,
            Arc<dyn RecipientSessionDirectory>,
            MessageSystemEvents,
        ) -> Pin<Box<dyn Future<Output = MessageSystem> + Send>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(Arc::new(start))
    }
}

#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
impl std::fmt::Debug for HostMessageSystemFixture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostMessageSystemFixture")
            .finish_non_exhaustive()
    }
}

#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
impl PartialEq for HostMessageSystemFixture {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(all(debug_assertions, feature = "runtime-test-support"))]
impl Eq for HostMessageSystemFixture {}

struct LocalMessageDirectory {
    directory: Arc<dyn RecipientSessionDirectory>,
    clock: Arc<dyn RuntimeClock>,
}

impl MessageSessionDirectory for LocalMessageDirectory {
    fn lookup<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = MessageSessionPresence> + Send + 'lookup>> {
        Box::pin(async move {
            let deadline = self.clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET);
            match self
                .directory
                .query_until(session_id, self.clock.as_ref(), deadline)
                .await
            {
                RecipientSessionAnswer::KnownOnce => MessageSessionPresence::KnownOnce,
                RecipientSessionAnswer::UnknownSession => MessageSessionPresence::UnknownSession,
                RecipientSessionAnswer::AmbiguousSession => {
                    MessageSessionPresence::AmbiguousSession
                }
                RecipientSessionAnswer::CouldNotTell => MessageSessionPresence::CouldNotTell,
            }
        })
    }
}

struct HostMessageAdmission {
    front: MessageFront,
    directory: Arc<LocalMessageDirectory>,
}

impl MessageCallerAdmission for HostMessageAdmission {
    fn admit(
        &self,
        reported: Option<SessionId>,
    ) -> MessageOperationFuture<'_, AdmittedLocalSession> {
        Box::pin(async move {
            let machine_id = self.front.available_machine()?;
            V0MessageAdmission::new(machine_id, self.directory.clone())
                .admit(reported)
                .await
        })
    }
}

pub(super) fn v0_message_admission(
    front: MessageFront,
    directory: Arc<dyn RecipientSessionDirectory>,
    clock: Arc<dyn RuntimeClock>,
) -> Arc<dyn MessageCallerAdmission> {
    Arc::new(HostMessageAdmission {
        front,
        directory: Arc::new(LocalMessageDirectory { directory, clock }),
    })
}

#[cfg(test)]
pub(crate) fn v0_message_admission_for_test(
    machine_id: collaboration_protocol::message_tools::MachineId,
    directory: Arc<dyn RecipientSessionDirectory>,
    clock: Arc<dyn RuntimeClock>,
) -> V0MessageAdmission {
    V0MessageAdmission::new(
        machine_id,
        Arc::new(LocalMessageDirectory { directory, clock }),
    )
}

pub(super) struct HostCustodyEvents;

impl CustodyEventSink for HostCustodyEvents {
    fn record_event(&self, event: CustodyTaskEvent) {
        match event {
            CustodyTaskEvent::Flush(report) if !report.issues.is_empty() => {
                tracing::warn!(
                    count = report.issues.len(),
                    "Mac message custody flush issues"
                );
            }
            CustodyTaskEvent::Failed(_) => tracing::warn!("Mac message custody task failed"),
            CustodyTaskEvent::Operator(_) => tracing::warn!("Mac message custody needs attention"),
            CustodyTaskEvent::Flush(_) | CustodyTaskEvent::Receipt(_) => {}
        }
    }
}
