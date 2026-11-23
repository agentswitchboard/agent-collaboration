//! The latest welcomed socket owns local routing for an organization machine.
use crate::machine_call_queue::{self, MacCallReceiver, MacCallSender, PendingMacCall};
use central_link_protocol::{FreshCorrelationId, MacCall, MacCallAddress, MachineLinkCloseCode};
use collaboration_central_core::ValidatedMachineConnection;
use fleet_contracts::{EnrollmentInvalidation, MachineConnectionAttachment};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_rpc::CallFailure;
use router_clock::ClockInstant;
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
    authority: MachineConnectionAttachment,
    sender: mpsc::Sender<MessageId>,
    calls: MacCallSender,
    replaced: CancellationToken,
    authority_close: Arc<OnceLock<MachineLinkCloseCode>>,
}

#[derive(Clone, Default)]
pub(crate) struct MachinePushRegistry(Arc<Mutex<HashMap<MachineKey, Vec<PushSocket>>>>);

impl MachinePushRegistry {
    pub(crate) fn register(
        &self,
        connection: &ValidatedMachineConnection,
    ) -> MachinePushRegistration {
        let key = (
            connection.organization_id().clone(),
            connection.machine_id().clone(),
        );
        let (sender, receiver) = mpsc::channel(fleet_contracts::mail_link::WELCOME_PAGE_SIZE);
        let replaced = CancellationToken::new();
        let (calls, outbound_calls) = machine_call_queue::channel(replaced.clone());
        let authority_close = Arc::new(OnceLock::new());
        let mut sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let active = sockets.entry(key.clone()).or_default();
        if let Some(older) = active.last() {
            older.calls.end();
        }
        active.push(PushSocket {
            enrollment: connection.enrollment_id().clone(),
            authority: connection.attachment(),
            sender: sender.clone(),
            calls,
            replaced: replaced.clone(),
            authority_close: authority_close.clone(),
        });
        MachinePushRegistration {
            registry: self.clone(),
            key,
            sender,
            replaced,
            authority_close,
            receiver,
            outbound_calls,
        }
    }

    pub(crate) fn select_approval_socket(
        &self,
        target: &central_link_protocol::approval_rpc::ApprovalHolderTarget,
    ) -> Result<
        (
            MachineConnectionAttachment,
            MacCallSender,
            CancellationToken,
        ),
        CallFailure,
    > {
        let sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let address = MacCallAddress::DeliverEnding(target.clone());
        sockets
            .get(&(target.organization_id.clone(), target.machine_id.clone()))
            .and_then(|sockets| sockets.last())
            .filter(|socket| {
                socket.calls.permits(
                    &address,
                    &target.organization_id,
                    &target.machine_id,
                    &socket.enrollment,
                )
            })
            .map(|socket| {
                (
                    socket.authority.clone(),
                    socket.calls.clone(),
                    socket.replaced.clone(),
                )
            })
            .ok_or(CallFailure::NeverReached)
    }

    pub(crate) fn queue_call(
        &self,
        organization: &OrganizationId,
        correlation: FreshCorrelationId,
        request: MacCall,
        address: MacCallAddress,
        deadline: ClockInstant,
    ) -> Result<PendingMacCall, CallFailure> {
        let sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let organization = address.organization_id().unwrap_or(organization);
        let machine = address.machine_id();
        let socket = sockets
            .get(&(organization.clone(), machine.clone()))
            .and_then(|sockets| sockets.last())
            .filter(|socket| {
                socket
                    .calls
                    .permits(&address, organization, machine, &socket.enrollment)
            })
            .ok_or(CallFailure::NeverReached)?;
        socket
            .calls
            .enqueue(correlation, deadline, request, address)
    }

    pub(crate) fn invalidate_enrollment(
        &self,
        organization: &OrganizationId,
        invalidation: &EnrollmentInvalidation,
    ) {
        let sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (organization.clone(), invalidation.machine_id.clone());
        if let Some(registered) = sockets.get(&key) {
            for socket in registered
                .iter()
                .filter(|socket| socket.enrollment == invalidation.enrollment_id)
            {
                let _ = socket.authority_close.set(invalidation.cause);
                socket.calls.end();
            }
        }
    }

    pub(crate) fn offer(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        message: MessageId,
    ) {
        let sockets = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(socket) = sockets
            .get(&(organization.clone(), machine.clone()))
            .and_then(|sockets| sockets.last())
            && !socket.replaced.is_cancelled()
            && let Err(mpsc::error::TrySendError::Full(_)) = socket.sender.try_send(message)
        {
            // Reconnect can recover the durable queued rows after a backed-up socket closes.
            socket.calls.end();
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
    pub(crate) outbound_calls: MacCallReceiver,
}

impl MachinePushRegistration {
    pub(crate) fn update_authority(&self, connection: &ValidatedMachineConnection) {
        let mut registry = self
            .registry
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(socket) = registry.get_mut(&self.key).and_then(|sockets| {
            sockets
                .iter_mut()
                .find(|socket| socket.sender.same_channel(&self.sender))
        }) {
            socket.authority = connection.attachment();
        }
    }
}

impl Drop for MachinePushRegistration {
    fn drop(&mut self) {
        let mut sockets = self
            .registry
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(registered) = sockets.get_mut(&self.key) {
            registered.retain(|socket| !socket.sender.same_channel(&self.sender));
            if registered.is_empty() {
                sockets.remove(&self.key);
            }
        }
    }
}
