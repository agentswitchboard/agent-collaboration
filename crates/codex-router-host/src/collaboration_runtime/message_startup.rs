use switchboard_message_custody::custody_task::{CustodyEventSink, CustodyTaskEvent};

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
