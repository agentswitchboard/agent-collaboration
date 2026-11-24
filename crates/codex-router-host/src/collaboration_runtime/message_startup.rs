use std::{path::PathBuf, sync::Arc};

use router_clock::RuntimeClock;
use switchboard_message_custody::{
    custody_task::{CustodyEventSink, CustodyTaskEvent},
    recipient_sessions::RecipientSessionDirectory,
};
use switchboard_message_unit::{MessageRuntime, MessageUnitEvents};

#[derive(Clone)]
pub(crate) struct HostMessageRoots {
    state: PathBuf,
    secrets: PathBuf,
}

impl HostMessageRoots {
    pub(crate) fn new(state: PathBuf, secrets: PathBuf) -> Self {
        Self { state, secrets }
    }

    pub(super) async fn start(
        self,
        clock: Arc<dyn RuntimeClock>,
        recipients: Arc<dyn RecipientSessionDirectory>,
        custody_events: Arc<dyn CustodyEventSink>,
    ) -> MessageRuntime {
        let events = MessageUnitEvents::new(custody_events);
        MessageRuntime::start(&self.state, self.secrets, clock, recipients, events).await
    }
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
