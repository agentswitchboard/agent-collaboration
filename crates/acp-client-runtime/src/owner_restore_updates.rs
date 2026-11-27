//! Route untagged ACP updates to the current restore's own stream.

use crate::acp_transport::{RestoreSelection, TransportRun};
use agent_client_protocol::Dispatch;
use session_event_model::session_profile_codec::STATE_METHOD;
use std::sync::{Mutex, PoisonError};
use switchboard_harness_shared_mechanics::opening::OpeningInstance;
use tokio::sync::mpsc;

#[derive(Default)]
pub(crate) struct RestoreUpdates(Mutex<Vec<RestoreUpdateTap>>);

struct RestoreUpdateTap {
    run: TransportRun,
    stamp: OpeningInstance,
    session: String,
    updates: Option<mpsc::UnboundedSender<Dispatch>>,
    awaiting_reply: bool,
}

impl RestoreUpdates {
    #[cfg(test)]
    pub(crate) fn open(
        &self,
        run: TransportRun,
        session: String,
    ) -> (OpeningInstance, mpsc::UnboundedReceiver<Dispatch>) {
        let stamp = OpeningInstance::default();
        let receive = self.open_instance(run, session, &stamp);
        (stamp, receive)
    }

    pub(crate) fn open_instance(
        &self,
        run: TransportRun,
        session: String,
        stamp: &OpeningInstance,
    ) -> mpsc::UnboundedReceiver<Dispatch> {
        let (send, receive) = mpsc::unbounded_channel();
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
        receive
    }

    pub(crate) fn retire(&self, run: TransportRun) {
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(tap) = taps
            .iter_mut()
            .rev()
            .find(|tap| tap.run == run && tap.updates.is_some())
        {
            tap.updates = None;
        }
        taps.retain(|tap| tap.updates.is_some() || tap.awaiting_reply);
    }

    pub(crate) fn retire_instance(&self, run: TransportRun, stamp: &OpeningInstance) {
        let mut taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(tap) = taps.iter_mut().find(|tap| tap.names(run, stamp)) {
            tap.updates = None;
        }
        taps.retain(|tap| tap.updates.is_some() || tap.awaiting_reply);
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
        selected: RestoreSelection,
        message: Dispatch,
    ) -> Option<Dispatch> {
        let taps = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let RestoreSelection::Registered { run, instance } = selected {
            let tap = taps
                .iter()
                .find(|tap| tap.names(run, &instance) && tap.session == session)?;
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
        if matches!(selected, RestoreSelection::NoRoute)
            && taps.iter().any(|tap| tap.session == session)
        {
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
