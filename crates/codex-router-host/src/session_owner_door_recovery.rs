//! Settle failed-owner callers and start the registry's successor.

use super::{SessionOwnerAddress, SessionOwnerDoor};
use crate::{
    session_owner_calls::SessionOwnerCalls,
    session_owner_execution::SessionOwnerExecution,
    session_owner_registry::{FailedOwnerSettlement, owner_proved_unsent},
    session_store_lane::SessionStoreSettlement,
};
use acp_client_runtime::session_owner_task::SessionOwnerTask;
use collaboration_protocol::SessionRef;
use provider_session_machines::session_owner::{
    OwnerEvidence, RemoteEffect, RevokeReason, SessionOwnerReply,
};
use session_event_model::InputId;
use std::{collections::BTreeMap, sync::Arc};

impl SessionOwnerDoor {
    pub(super) fn run_registered(
        self: &Arc<Self>,
        target: SessionRef,
        mut task: SessionOwnerTask,
        mut ports: SessionOwnerExecution,
        mut owner: Arc<SessionOwnerAddress>,
        work_roots: crate::provider_work_roots::ProviderWorkRoots,
    ) {
        let weak = Arc::downgrade(self);
        let stop = self.shutdown.clone();
        self.tasks.spawn(async move {
            loop {
                match task.run(ports, stop.clone()).await {
                    Ok(()) => {
                        owner.calls.stop();
                        return;
                    }
                    Err(error) => tracing::debug!(%error, "provider session owner failed"),
                }
                let Some((next_task, next_ports, next_owner)) = weak
                    .upgrade()
                    .and_then(|door| door.recover_session(&target, &owner, work_roots.clone()))
                else {
                    owner.calls.stop();
                    return;
                };
                task = next_task;
                ports = next_ports;
                owner = next_owner;
            }
        });
    }

    pub(super) fn recover_session(
        &self,
        target: &SessionRef,
        previous: &Arc<SessionOwnerAddress>,
        work_roots: crate::provider_work_roots::ProviderWorkRoots,
    ) -> Option<(
        SessionOwnerTask,
        SessionOwnerExecution,
        Arc<SessionOwnerAddress>,
    )> {
        if self.shutdown.is_cancelled() {
            return None;
        }
        #[cfg(test)]
        if self.preparation_fixture.take_replacement_refusal() {
            return None;
        }
        let failed = previous.owner.address.incarnation;
        let asked_inputs = previous
            .calls
            .outstanding_inputs()
            .into_iter()
            .map(|input| input.input)
            .collect();
        let FailedOwnerSettlement {
            replacement,
            handover,
            slot_evidence,
            ..
        } = self
            .registry
            .replace_failed_with(
                target,
                failed,
                self.connection,
                asked_inputs,
                |incarnation, inputs| self.provider.give_up_owner(incarnation, inputs),
            )
            .ok()?;
        let registered = self
            .registry
            .session(target)
            .filter(|session| session.owner.address == replacement.owner.address)?;
        let (successor, ports) = self
            .prepare_adopted(target.clone(), registered, None, Some(failed), work_roots)
            .ok()?;
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(target.clone(), successor.clone());
        #[cfg(test)]
        self.preparation_fixture.observe_installed_replacement();

        let callers = previous.calls.clone();
        #[cfg(test)]
        let pause = self.preparation_fixture.clone();
        self.tasks.spawn(async move {
            let mut results = BTreeMap::new();
            for (marked, ticket) in handover.cleanups {
                let saved = matches!(
                    ticket.wait().await,
                    Ok(SessionStoreSettlement::Operation(_)
                        | SessionStoreSettlement::MarkNeverWritten)
                );
                results.insert(marked.input, saved);
            }
            #[cfg(test)]
            if let Some(release) = pause.hold_recovery() {
                let _test_withdrew_pause = release.await;
            }
            settle_recovered_callers(&callers, &slot_evidence, results);
        });
        Some((replacement.task, ports, successor))
    }

    #[cfg(test)]
    pub(crate) fn settle_recovered_callers_for_test(
        callers: &Arc<SessionOwnerCalls>,
        evidence: &OwnerEvidence,
        saved: BTreeMap<InputId, bool>,
    ) {
        settle_recovered_callers(callers, evidence, saved);
    }
}

fn settle_recovered_callers(
    callers: &Arc<SessionOwnerCalls>,
    evidence: &OwnerEvidence,
    mut saved_by_input: BTreeMap<InputId, bool>,
) {
    for input in callers.outstanding_inputs() {
        let saved = saved_by_input.remove(&input.input).unwrap_or(false);
        let reply = if owner_proved_unsent(evidence.get(&input.input)) {
            SessionOwnerReply::InputUnsent {
                input,
                reason: RevokeReason::ConnectionLost,
            }
        } else {
            SessionOwnerReply::InputFinished {
                input,
                remote: RemoteEffect::Unknown,
            }
        };
        callers.settle(reply, saved, None);
    }
    callers.stop();
}

impl Drop for SessionOwnerDoor {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
