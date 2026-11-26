//! Route untagged ACP updates to the current restore's own stream.

use crate::acp_transport::TransportRun;
use agent_client_protocol::Dispatch;
use session_event_model::session_profile_codec::STATE_METHOD;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::mpsc;

#[derive(Default)]
pub(crate) struct RestoreUpdates(Mutex<Vec<RestoreUpdateTap>>);

#[derive(Clone, Default)]
pub(crate) struct OpeningInstance(Arc<()>);

impl OpeningInstance {
    pub(crate) fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

struct RestoreUpdateTap {
    run: TransportRun,
    stamp: OpeningInstance,
    session: String,
    updates: Option<mpsc::UnboundedSender<Dispatch>>,
    awaiting_reply: bool,
}

impl RestoreUpdates {
    pub(crate) fn open(
        &self,
        run: TransportRun,
        session: String,
    ) -> (OpeningInstance, mpsc::UnboundedReceiver<Dispatch>) {
        let (send, receive) = mpsc::unbounded_channel();
        let stamp = OpeningInstance::default();
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for earlier in taps.iter_mut().filter(|tap| tap.run == run) {
            earlier.updates = None;
        }
        taps.retain(|tap| tap.updates.is_some() || tap.awaiting_reply);
        taps.push(RestoreUpdateTap {
            run,
            stamp: stamp.clone(),
            session,
            updates: Some(send),
            awaiting_reply: true,
        });
        (stamp, receive)
    }

    pub(crate) fn retire(&self, run: TransportRun) {
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(index) = taps
            .iter()
            .rposition(|tap| tap.run == run && tap.updates.is_some())
            && let Some(tap) = taps.get_mut(index)
        {
            tap.updates = None;
            if !tap.awaiting_reply {
                taps.remove(index);
            }
        }
    }

    pub(crate) fn discard_unoffered(&self, run: TransportRun, stamp: &OpeningInstance) {
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        taps.retain(|tap| !tap.names(run, stamp));
    }

    pub(crate) fn reply_settled(&self, run: TransportRun, stamp: &OpeningInstance) {
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(index) = taps.iter().position(|tap| tap.names(run, stamp))
            && let Some(tap) = taps.get_mut(index)
        {
            tap.awaiting_reply = false;
            if tap.updates.is_none() {
                taps.remove(index);
            }
        }
    }

    pub(crate) fn forward(
        &self,
        session: &str,
        current: Option<TransportRun>,
        message: Dispatch,
    ) -> Option<Dispatch> {
        let taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(tap) = current.and_then(|run| {
            taps.iter()
                .rev()
                .find(|tap| tap.run == run && tap.session == session)
        }) {
            // An untagged state notification from an earlier unanswered
            // restore cannot supply live turn evidence for its successor.
            if matches!(&message, Dispatch::Notification(notification) if notification.method() == STATE_METHOD)
                && taps.iter().any(|earlier| {
                    !earlier.stamp.is_same_as(&tap.stamp)
                        && earlier.session == session
                        && earlier.awaiting_reply
                })
            {
                return None;
            }
            if let Some(updates) = &tap.updates {
                let _owner_retired = updates.send(message);
            }
            return None;
        }
        if current.is_none() && taps.iter().any(|tap| tap.session == session) {
            return None;
        }
        Some(message)
    }
}

impl RestoreUpdateTap {
    fn names(&self, run: TransportRun, stamp: &OpeningInstance) -> bool {
        self.run == run && self.stamp.is_same_as(stamp)
    }
}

#[cfg(test)]
#[path = "owner_restore_updates_test.rs"]
mod tests;
