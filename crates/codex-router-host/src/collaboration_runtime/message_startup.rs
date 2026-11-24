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
use switchboard_message_system::MessageFront;

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
