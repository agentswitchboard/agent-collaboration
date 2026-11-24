//! The managed notice's writer fence and take-time evidence.

use acp_client_runtime::owner_wire::OwnerAcpWire;
use provider_session_machines::session_owner::{DispatchAuthorization, DispatchResult};
use std::sync::{Arc, OnceLock};
use switchboard_message_custody::handover_notice::ClaimedNotice;

#[derive(Clone)]
pub(crate) struct NoticeWriterTake {
    notice: Arc<ClaimedNotice>,
    clock: Arc<dyn router_clock::RuntimeClock>,
    taken_at: Arc<OnceLock<chrono::DateTime<chrono::Utc>>>,
}

impl NoticeWriterTake {
    pub(crate) fn new(notice: ClaimedNotice, clock: Arc<dyn router_clock::RuntimeClock>) -> Self {
        Self {
            notice: Arc::new(notice),
            clock,
            taken_at: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn taken_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.taken_at.get().copied()
    }

    pub(crate) fn take(
        &self,
        authorization: &DispatchAuthorization,
        wire: &OwnerAcpWire,
    ) -> Option<DispatchResult> {
        let mut checked_at = None;
        let result = self.notice.while_live(|| {
            wire.take_when(authorization, || {
                let taken_at = self.clock.utc_now();
                if !self.notice.can_take_at(self.clock.as_ref(), taken_at) {
                    return false;
                }
                checked_at = Some(taken_at);
                true
            })
        });
        let result = match result {
            Some(result) => result,
            None => wire.take_when(authorization, || false),
        }
        .ok()?;
        if matches!(result, DispatchResult::Taken(_))
            && let Some(taken_at) = checked_at
        {
            let _ = self.taken_at.set(taken_at);
        }
        Some(result)
    }
}
