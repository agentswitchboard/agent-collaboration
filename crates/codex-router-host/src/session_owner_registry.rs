//! Adopt provider IDs and replace failed session-owner incarnations.

use crate::binding_dispatch_permit::BindingUncertainty;
use crate::session_store_lane::{
    MarkedInput, SessionStoreLane, SessionStoreLaneTask, SessionStoreOfferError, StoreLaneHandover,
};
use acp_client_runtime::{
    acp_transport::{AcpTransport, SlotEvidence},
    session_owner_task::{SessionOwnerHandle, SessionOwnerTask},
};
use collaboration_protocol::{OperationId, SessionRef};
use provider_session_machines::session_owner::{
    ConnectionEpoch, DispatchAuthorization, OwnerAddress, OwnerEvidence, OwnerIncarnation,
    OwnerInputEvidence,
};
use router_clock::RuntimeClock;
use session_event_model::InputId;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct RegisteredOwner {
    pub address: OwnerAddress,
    pub handle: SessionOwnerHandle,
}

pub struct OwnerTaskLaunch {
    pub owner: RegisteredOwner,
    pub task: SessionOwnerTask,
    pub shutdown: CancellationToken,
}

#[derive(Clone)]
pub struct RegisteredSession {
    pub owner: RegisteredOwner,
    pub lane: SessionStoreLane,
}

pub struct SessionRegistration {
    pub session: RegisteredSession,
    pub launch: Option<(OwnerTaskLaunch, SessionStoreLaneTask)>,
}

pub struct FailedOwnerSettlement<Evidence = Vec<(DispatchAuthorization, SlotEvidence)>> {
    pub replacement: OwnerTaskLaunch,
    pub handover: StoreLaneHandover,
    pub slot_evidence: Evidence,
    pub inputs: Vec<FailedOwnerInput>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum FailedOwnerInput {
    KnownUnsent(MarkedInput),
    PotentiallyApplied(MarkedInput),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionOwnerRegistryError {
    #[error("this session id is already in use")]
    AlreadyOwned,
    #[error("this owner was already replaced")]
    Replaced,
    #[error(transparent)]
    StoreLane(#[from] SessionStoreOfferError),
}

struct OwnerEntry {
    owner: RegisteredOwner,
    shutdown: CancellationToken,
}

struct SessionEntry {
    owner: OwnerEntry,
    lane: SessionStoreLane,
}

#[derive(Default)]
struct RegistryEntries {
    sessions: BTreeMap<SessionRef, SessionEntry>,
    creates: BTreeMap<OperationId, OwnerEntry>,
}

pub struct SessionOwnerRegistry {
    entries: Mutex<RegistryEntries>,
    binding: Arc<std::sync::OnceLock<Arc<BindingUncertainty>>>,
    store: Arc<AsyncMutex<ProviderOperationStore>>,
    clock: Arc<dyn RuntimeClock>,
    mailbox_capacity: NonZeroUsize,
    completion_capacity: NonZeroUsize,
}

impl SessionOwnerRegistry {
    pub fn new(
        store: Arc<AsyncMutex<ProviderOperationStore>>,
        clock: Arc<dyn RuntimeClock>,
        mailbox_capacity: NonZeroUsize,
        completion_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            entries: Mutex::default(),
            binding: Arc::default(),
            store,
            clock,
            mailbox_capacity,
            completion_capacity,
        }
    }

    pub(crate) fn install_binding_permit(&self, binding: Arc<BindingUncertainty>) {
        let _already_installed = self.binding.set(binding);
    }

    fn entries(&self) -> MutexGuard<'_, RegistryEntries> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn session(&self, target: &SessionRef) -> Option<RegisteredSession> {
        self.entries()
            .sessions
            .get(target)
            .map(SessionEntry::registered)
    }

    pub fn get_or_insert(
        &self,
        target: SessionRef,
        connection_epoch: ConnectionEpoch,
    ) -> SessionRegistration {
        use std::collections::btree_map::Entry;
        let mut entries = self.entries();
        match entries.sessions.entry(target.clone()) {
            Entry::Occupied(entry) => SessionRegistration {
                session: entry.get().registered(),
                launch: None,
            },
            Entry::Vacant(entry) => {
                let launch = self.launch(connection_epoch);
                let (lane, lane_task) = self.new_lane(launch.owner.address.incarnation, target);
                let saved = SessionEntry {
                    owner: OwnerEntry::from(&launch),
                    lane,
                };
                let session = saved.registered();
                entry.insert(saved);
                SessionRegistration {
                    session,
                    launch: Some((launch, lane_task)),
                }
            }
        }
    }

    pub fn begin_create(
        &self,
        operation: OperationId,
        connection_epoch: ConnectionEpoch,
    ) -> Result<OwnerTaskLaunch, SessionOwnerRegistryError> {
        use std::collections::btree_map::Entry;
        let mut entries = self.entries();
        match entries.creates.entry(operation) {
            Entry::Occupied(_) => Err(SessionOwnerRegistryError::AlreadyOwned),
            Entry::Vacant(entry) => {
                let launch = self.launch(connection_epoch);
                entry.insert(OwnerEntry::from(&launch));
                Ok(launch)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn provisional_owner(&self, operation: &OperationId) -> Option<RegisteredOwner> {
        self.entries()
            .creates
            .get(operation)
            .map(|entry| entry.owner.clone())
    }

    pub fn adopt(
        &self,
        operation: &OperationId,
        incarnation: OwnerIncarnation,
        target: SessionRef,
    ) -> Result<(RegisteredSession, SessionStoreLaneTask), SessionOwnerRegistryError> {
        let mut entries = self.entries();
        if entries
            .creates
            .get(operation)
            .is_none_or(|entry| entry.owner.address.incarnation != incarnation)
        {
            return Err(SessionOwnerRegistryError::Replaced);
        }
        if entries.sessions.contains_key(&target) {
            return Err(SessionOwnerRegistryError::AlreadyOwned);
        }
        let owner = entries
            .creates
            .remove(operation)
            .ok_or(SessionOwnerRegistryError::Replaced)?;
        let (lane, lane_task) = self.new_lane(incarnation, target.clone());
        let saved = SessionEntry { owner, lane };
        let session = saved.registered();
        entries.sessions.insert(target, saved);
        Ok((session, lane_task))
    }

    /// An abandoned provisional create remains potentially applied.
    pub fn retire_create(
        &self,
        operation: &OperationId,
        incarnation: OwnerIncarnation,
    ) -> Result<(), SessionOwnerRegistryError> {
        let mut entries = self.entries();
        if entries
            .creates
            .get(operation)
            .is_none_or(|entry| entry.owner.address.incarnation != incarnation)
        {
            return Err(SessionOwnerRegistryError::Replaced);
        }
        if let Some(entry) = entries.creates.remove(operation) {
            entry.shutdown.cancel();
        }
        Ok(())
    }

    /// Called by the task monitor after the old owner stops offering effects.
    /// It fences the registry compare-and-swap, transport revocation and lane
    /// handover against a concurrent attempt to replace the same entry.
    pub fn replace_failed_session<'connection, Frame: 'connection>(
        &self,
        target: &SessionRef,
        failed: OwnerIncarnation,
        connection: ConnectionEpoch,
        transports: impl IntoIterator<Item = &'connection AcpTransport<Frame>>,
    ) -> Result<FailedOwnerSettlement, SessionOwnerRegistryError> {
        let mut slots = Vec::new();
        let settled = self.replace_failed_with(
            target,
            failed,
            connection,
            BTreeSet::new(),
            |incarnation, inputs| {
                slots = transports
                    .into_iter()
                    .flat_map(|transport| transport.revoke_incarnation(incarnation))
                    .collect();
                answer_complete_acp_snapshot(inputs, &slots)
            },
        )?;
        Ok(FailedOwnerSettlement {
            replacement: settled.replacement,
            handover: settled.handover,
            slot_evidence: slots,
            inputs: settled.inputs,
        })
    }

    pub(crate) fn replace_failed_with(
        &self,
        target: &SessionRef,
        failed: OwnerIncarnation,
        connection: ConnectionEpoch,
        mut asked_inputs: BTreeSet<InputId>,
        give_up_owner: impl FnOnce(OwnerIncarnation, &BTreeSet<InputId>) -> OwnerEvidence,
    ) -> Result<FailedOwnerSettlement<OwnerEvidence>, SessionOwnerRegistryError> {
        let mut entries = self.entries();
        let current = entries
            .sessions
            .get_mut(target)
            .filter(|entry| entry.owner.owner.address.incarnation == failed)
            .ok_or(SessionOwnerRegistryError::Replaced)?;
        let accepted_marks = current.lane.accepted_marks(failed);
        asked_inputs.extend(accepted_marks.iter().map(|marked| marked.input.clone()));
        let slot_evidence = give_up_owner(failed, &asked_inputs);
        let mut cleanups = Vec::new();
        let mut inputs = Vec::new();
        for marked in accepted_marks {
            if owner_proved_unsent(slot_evidence.get(&marked.input)) {
                cleanups.push(marked.clone());
                inputs.push(FailedOwnerInput::KnownUnsent(marked));
            } else {
                inputs.push(FailedOwnerInput::PotentiallyApplied(marked));
            }
        }
        let replacement = self.launch(connection);
        let handover =
            current
                .lane
                .handover(failed, replacement.owner.address.incarnation, cleanups)?;
        current.owner.shutdown.cancel();
        current.owner = OwnerEntry::from(&replacement);
        Ok(FailedOwnerSettlement {
            replacement,
            handover,
            slot_evidence,
            inputs,
        })
    }

    fn launch(&self, connection_epoch: ConnectionEpoch) -> OwnerTaskLaunch {
        let address = OwnerAddress {
            incarnation: OwnerIncarnation(Uuid::now_v7()),
            connection_epoch,
        };
        let (task, handle) = SessionOwnerTask::new(
            address,
            Arc::clone(&self.clock),
            self.mailbox_capacity,
            self.completion_capacity,
        );
        OwnerTaskLaunch {
            owner: RegisteredOwner { address, handle },
            task,
            shutdown: CancellationToken::new(),
        }
    }

    fn new_lane(
        &self,
        incarnation: OwnerIncarnation,
        target: SessionRef,
    ) -> (SessionStoreLane, SessionStoreLaneTask) {
        SessionStoreLane::with_binding(
            incarnation,
            target,
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            Arc::clone(&self.binding),
        )
    }
}

pub(super) fn owner_proved_unsent(evidence: Option<&OwnerInputEvidence>) -> bool {
    matches!(
        evidence,
        Some(OwnerInputEvidence::Withdrawn { .. } | OwnerInputEvidence::NeverOffered)
    )
}

/// Project the complete ACP snapshot used by the raw-slot registry fixture.
fn answer_complete_acp_snapshot(
    inputs: &BTreeSet<InputId>,
    slots: &[(DispatchAuthorization, SlotEvidence)],
) -> OwnerEvidence {
    let mut by_input = BTreeMap::<InputId, Vec<_>>::new();
    for (authorization, evidence) in slots {
        by_input
            .entry(authorization.input_id.clone())
            .or_default()
            .push((authorization.clone(), *evidence));
    }
    inputs
        .iter()
        .map(|input| {
            let answer = match by_input.remove(input) {
                None => OwnerInputEvidence::NeverOffered,
                Some(slots) if slots.iter().any(|(_, slot)| *slot == SlotEvidence::Taken) => {
                    OwnerInputEvidence::Taken { slots }
                }
                Some(slots) if slots.iter().all(|(_, slot)| *slot == SlotEvidence::Revoked) => {
                    OwnerInputEvidence::Withdrawn { slots }
                }
                Some(slots) => OwnerInputEvidence::NoEvidence { slots },
            };
            (input.clone(), answer)
        })
        .collect()
}

impl From<&OwnerTaskLaunch> for OwnerEntry {
    fn from(launch: &OwnerTaskLaunch) -> Self {
        Self {
            owner: launch.owner.clone(),
            shutdown: launch.shutdown.clone(),
        }
    }
}

impl SessionEntry {
    fn registered(&self) -> RegisteredSession {
        RegisteredSession {
            owner: self.owner.owner.clone(),
            lane: self.lane.clone(),
        }
    }
}

#[cfg(test)]
#[path = "session_owner_registry_test.rs"]
mod tests;
