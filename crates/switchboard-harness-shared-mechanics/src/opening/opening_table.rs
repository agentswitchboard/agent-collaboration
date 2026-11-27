//! Admission, exact row ownership and the opening table's under-lock effects.

use std::collections::BTreeMap;
use switchboard_harness_contracts::{
    ActivationRefusal, ActivationSetup, CreateAttempt, LifecycleRefusal, OpenAttempt, OwnerAddress,
    SessionGeneration,
};
use tokio_util::sync::CancellationToken;

use super::opening_row::{
    Command, Effect, OpenCreateRequest, OpenRestoreRequest, OpeningRow, Rejected, RestoreOpening,
    RunResources, Slot,
};
use super::{LifecycleRoute, OpeningAdapter, OpeningInstance, OpeningPhase, activation_refusal};

pub struct OpeningTable<A: OpeningAdapter> {
    creates: BTreeMap<CreateAttempt, OpeningRow<A>>,
    runs: BTreeMap<SessionGeneration, OpeningRow<A>>,
    port_refusal: Option<LifecycleRefusal>,
}

impl<A: OpeningAdapter> Default for OpeningTable<A> {
    fn default() -> Self {
        Self {
            creates: BTreeMap::new(),
            runs: BTreeMap::new(),
            port_refusal: None,
        }
    }
}

impl<A: OpeningAdapter> OpeningTable<A> {
    pub fn get(&self, slot: Slot) -> Option<&OpeningRow<A>> {
        match slot {
            Slot::Create(create) => self.creates.get(&create),
            Slot::Run(run) => self.runs.get(&run),
        }
    }

    fn insert(&mut self, slot: Slot, row: OpeningRow<A>) {
        match slot {
            Slot::Create(create) => {
                self.creates.insert(create, row);
            }
            Slot::Run(run) => {
                self.runs.insert(run, row);
            }
        }
    }

    fn remove(&mut self, slot: Slot) -> Option<OpeningRow<A>> {
        match slot {
            Slot::Create(create) => self.creates.remove(&create),
            Slot::Run(run) => self.runs.remove(&run),
        }
    }

    pub fn exact(&self, slot: Slot, instance: &OpeningInstance) -> Option<&OpeningRow<A>> {
        self.get(slot)
            .filter(|row| row.instance().is_same_as(instance))
    }

    pub fn apply(
        &mut self,
        slot: Slot,
        instance: &OpeningInstance,
        command: Command<A>,
    ) -> Result<Effect<A>, Rejected<A>> {
        if self.exact(slot, instance).is_none() {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        }
        let Some(row) = self.remove(slot) else {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        };
        self.commit(slot, row, instance, command)
    }

    fn commit(
        &mut self,
        slot: Slot,
        row: OpeningRow<A>,
        instance: &OpeningInstance,
        command: Command<A>,
    ) -> Result<Effect<A>, Rejected<A>> {
        match row.apply(instance, command) {
            Ok((next, effect)) => {
                effect.fence();
                if let Some(row) = next {
                    self.insert(slot, row);
                }
                Ok(effect)
            }
            Err(refused) => {
                if !refused.row.is_vacant() {
                    self.insert(slot, *refused.row);
                }
                Err(refused.rejection)
            }
        }
    }

    pub fn begin_create(
        &mut self,
        bound: bool,
        attempt: CreateAttempt,
        request: OpenCreateRequest,
    ) -> Result<(OpeningInstance, Effect<A>), Rejected<A>> {
        let identity = OpenAttempt::Create(attempt);
        let command = Command::OpenCreate(Box::new(request));
        if !self.creates.is_empty() || !self.runs.is_empty() || self.port_refusal.is_some() {
            return Err(Rejected {
                reason: self.refusal_for(identity, bound),
                _command: Box::new(command),
            });
        }
        self.begin_vacant(bound, identity, command)
    }

    pub fn begin_restore(
        &mut self,
        bound_session: Option<&str>,
        owner: OwnerAddress,
        request: RestoreOpening,
        stopped: CancellationToken,
    ) -> Result<(OpeningInstance, Effect<A>), Rejected<A>> {
        let identity = OpenAttempt::Restore(request.run);
        let bound = bound_session.is_some();
        let wrong_session = bound_session.is_some_and(|bound| bound != request.session.0);
        let command = Command::OpenRestore(Box::new(OpenRestoreRequest {
            request,
            owner,
            stopped,
        }));
        if let Some(reason) = self.port_refusal {
            return Err(Rejected {
                reason,
                _command: Box::new(command),
            });
        }
        if wrong_session {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        }
        if !self.creates.is_empty() || self.get(Slot::for_attempt(identity)).is_some() {
            return Err(Rejected {
                reason: self.refusal_for(identity, bound),
                _command: Box::new(command),
            });
        }
        self.begin_vacant(bound, identity, command)
    }

    fn begin_vacant(
        &mut self,
        bound: bool,
        identity: OpenAttempt,
        command: Command<A>,
    ) -> Result<(OpeningInstance, Effect<A>), Rejected<A>> {
        let instance = OpeningInstance::default();
        let row = OpeningRow::vacant(identity, instance.clone(), bound);
        let effect = self.commit(Slot::for_attempt(identity), row, &instance, command)?;
        Ok((instance, effect))
    }

    pub fn begin_reader(
        &mut self,
        bound: bool,
        owner: OwnerAddress,
        setup: ActivationSetup,
        parent_stop: &CancellationToken,
    ) -> Result<(OpeningInstance, SessionGeneration, Effect<A>), ActivationRefusal> {
        if let Some(reason) = self.port_refusal {
            return Err(activation_refusal(reason));
        }
        let (identity, generation) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => (OpenAttempt::Create(attempt), generation),
            ActivationSetup::Restored(run) => (OpenAttempt::Restore(run), run.generation),
        };
        let run_slot = Slot::Run(generation);
        if generation == SessionGeneration::UNTRACKED {
            return Err(ActivationRefusal::AttemptMismatch);
        }
        if let Some(row) = self.get(run_slot) {
            if row.attempt() != identity {
                return Err(ActivationRefusal::AttemptMismatch);
            }
            if row.phase() == OpeningPhase::Active {
                return Err(ActivationRefusal::AlreadyActive);
            }
            if matches!(identity, OpenAttempt::Create(_)) {
                return Err(activation_refusal(row.phase().refusal()));
            }
        }
        let slot = Slot::for_attempt(identity);
        let instance = self
            .targeted(identity)
            .map(|row| row.instance().clone())
            .ok_or_else(|| activation_refusal(self.refusal_for(identity, bound)))?;
        let created = matches!(identity, OpenAttempt::Create(_));
        let effect = self
            .apply(
                slot,
                &instance,
                Command::Activate {
                    run: RunResources::new(owner, generation, parent_stop),
                    created,
                },
            )
            .map_err(|rejected| activation_refusal(rejected.reason))?;
        if created
            && matches!(effect, Effect::StartReader(_))
            && let Some(row) = self.remove(slot)
        {
            self.insert(run_slot, row);
        }
        Ok((instance, generation, effect))
    }

    pub fn targeted(&self, attempt: OpenAttempt) -> Option<&OpeningRow<A>> {
        self.get(Slot::for_attempt(attempt))
            .filter(|row| row.attempt() == attempt)
            .or_else(|| match attempt {
                OpenAttempt::Create(_) => self.runs.values().find(|row| row.attempt() == attempt),
                OpenAttempt::Restore(_) => None,
            })
    }

    pub fn refusal_for(&self, attempt: OpenAttempt, bound: bool) -> LifecycleRefusal {
        if let Some(refusal) = self.port_refusal {
            return refusal;
        }
        if let Some(row) = self.targeted(attempt) {
            return row.phase().refusal();
        }
        if self.creates.is_empty() && self.runs.is_empty() {
            idle_phase(bound).refusal()
        } else {
            LifecycleRefusal::AttemptMismatch
        }
    }

    pub fn close_target(
        &self,
        generation: SessionGeneration,
        bound: bool,
    ) -> Result<OpeningInstance, LifecycleRefusal> {
        if let Some(refusal) = self.port_refusal {
            return Err(refusal);
        }
        let Some(row) = self.get(Slot::Run(generation)) else {
            return Err(if let Some(create) = self.creates.values().next() {
                create.phase().refusal()
            } else if self.runs.is_empty() {
                idle_phase(bound).refusal()
            } else {
                LifecycleRefusal::AttemptMismatch
            });
        };
        if !bound {
            return Err(row.phase().refusal());
        }
        Ok(row.instance().clone())
    }

    pub fn removed_routes(
        &self,
    ) -> (
        Vec<(Slot, OpeningInstance, LifecycleRoute)>,
        Option<LifecycleRefusal>,
    ) {
        let removed = self
            .runs
            .iter()
            .filter_map(|(&generation, row)| {
                row.removed_route()
                    .map(|route| (Slot::Run(generation), row.instance().clone(), route))
            })
            .collect();
        (removed, self.port_refusal)
    }

    pub fn stop_all(
        &mut self,
        refusal: LifecycleRefusal,
    ) -> Option<Vec<Result<Effect<A>, Rejected<A>>>> {
        if self.port_refusal == Some(LifecycleRefusal::ConnectionRetired)
            || self.port_refusal == Some(refusal)
        {
            return None;
        }
        self.port_refusal = Some(refusal);
        let rows = self
            .creates
            .iter()
            .map(|(&create, row)| (Slot::Create(create), row.instance().clone()))
            .chain(
                self.runs
                    .iter()
                    .map(|(&generation, row)| (Slot::Run(generation), row.instance().clone())),
            )
            .collect::<Vec<_>>();
        let mut effects = Vec::with_capacity(rows.len());
        for (slot, instance) in rows {
            let command = if refusal == LifecycleRefusal::ConnectionRetired {
                Command::ConnectionRetired
            } else {
                Command::WriterStopped
            };
            effects.push(self.apply(slot, &instance, command));
        }
        Some(effects)
    }

    pub fn port_refusal(&self) -> Option<LifecycleRefusal> {
        self.port_refusal
    }

    pub fn runs_is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn phase_of(&self, generation: SessionGeneration) -> Option<OpeningPhase> {
        self.runs.get(&generation).map(OpeningRow::phase)
    }
}

fn idle_phase(bound: bool) -> OpeningPhase {
    if bound {
        OpeningPhase::BoundIdle
    } else {
        OpeningPhase::UnboundIdle
    }
}

#[cfg(test)]
#[path = "opening_table_test.rs"]
mod tests;
