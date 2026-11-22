//! The latest welcomed socket locally owns delivery for an organization machine.
use central_link_protocol::MachineLinkCloseCode;
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use workspace_ids::{MachineId, MessageId};

type MachineKey = (OrganizationId, MachineId);

struct PushSocket {
    enrollment: EnrollmentId,
    sender: mpsc::Sender<MessageId>,
    replaced: CancellationToken,
    authority_close: Arc<OnceLock<MachineLinkCloseCode>>,
}

#[derive(Clone, Default)]
pub(crate) struct MachinePushRegistry(Arc<Mutex<HashMap<MachineKey, PushSocket>>>);

impl MachinePushRegistry {
    pub(crate) fn register(
        &self,
        organization: OrganizationId,
        machine: MachineId,
        enrollment: EnrollmentId,
    ) -> MachinePushRegistration {
        let key = (organization, machine);
        let (sender, receiver) = mpsc::channel(fleet_contracts::mail_link::WELCOME_PAGE_SIZE);
        let replaced = CancellationToken::new();
        let authority_close = Arc::new(OnceLock::new());
        let mut sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(older) = sockets.insert(
            key.clone(),
            PushSocket {
                enrollment,
                sender: sender.clone(),
                replaced: replaced.clone(),
                authority_close: authority_close.clone(),
            },
        ) {
            older.replaced.cancel();
        }
        MachinePushRegistration {
            registry: self.clone(),
            key,
            sender,
            replaced,
            authority_close,
            receiver,
        }
    }

    pub(crate) fn revoke(&self, organization: &OrganizationId, machine: &MachineId) {
        self.close_matching(
            organization,
            machine,
            |_| true,
            MachineLinkCloseCode::Revoked,
        );
    }

    pub(crate) fn close_idle_enrollment(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        enrollment: &EnrollmentId,
    ) {
        self.close_matching(
            organization,
            machine,
            |socket| &socket.enrollment == enrollment,
            MachineLinkCloseCode::Inactive,
        );
    }

    fn close_matching(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        matches: impl FnOnce(&PushSocket) -> bool,
        cause: MachineLinkCloseCode,
    ) {
        let mut sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (organization.clone(), machine.clone());
        if sockets.get(&key).is_some_and(matches)
            && let Some(socket) = sockets.remove(&key)
        {
            let _ = socket.authority_close.set(cause);
            socket.replaced.cancel();
        }
    }

    pub(crate) fn offer(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        message: MessageId,
    ) {
        let sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(socket) = sockets.get(&(organization.clone(), machine.clone()))
            && let Err(mpsc::error::TrySendError::Full(_)) = socket.sender.try_send(message)
        {
            // Reconnect can recover the durable queued rows after a backed-up socket closes.
            socket.replaced.cancel();
        }
    }
}

pub(crate) struct MachinePushRegistration {
    registry: MachinePushRegistry,
    key: MachineKey,
    sender: mpsc::Sender<MessageId>,
    pub(crate) replaced: CancellationToken,
    pub(crate) authority_close: Arc<OnceLock<MachineLinkCloseCode>>,
    pub(crate) receiver: mpsc::Receiver<MessageId>,
}

impl Drop for MachinePushRegistration {
    fn drop(&mut self) {
        let mut sockets = self
            .registry
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if sockets
            .get(&self.key)
            .is_some_and(|socket| socket.sender.same_channel(&self.sender))
        {
            sockets.remove(&self.key);
        }
    }
}
