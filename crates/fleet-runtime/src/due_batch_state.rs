use central_engine_model::{
    DueDrainEffect, DueDrainEvent, DueDrainMachine, DueDrainPass, DueDrainState, DueDrainWakeId,
};
use std::sync::{Arc, Mutex, PoisonError};

type ScheduleDueHandoff = Box<dyn FnOnce(DueBatchClaim) + Send>;

#[cfg(feature = "test-support")]
#[derive(Default)]
struct ReplyDrainOrder {
    discoveries_started: usize,
    discoveries_at_each_reply: Vec<usize>,
}

#[derive(Default)]
struct DueDrainHolder {
    machine: DueDrainState,
    pending_handoff: Option<ScheduleDueHandoff>,
}

/// One due-drain state machine and its waiting reply's transport handoff.
#[derive(Default)]
pub struct DueBatchState {
    drain: Mutex<DueDrainHolder>,
    #[cfg(feature = "test-support")]
    reply_order: Mutex<ReplyDrainOrder>,
    #[cfg(feature = "test-support")]
    unit_starts: Mutex<Vec<DueUnitStart>>,
    #[cfg(feature = "test-support")]
    batch_starts: Mutex<Vec<i64>>,
}

pub struct DueBatchClaim {
    owner: Arc<DueBatchState>,
    wake_id: DueDrainWakeId,
}

pub enum AlarmDrainAdmission {
    RunPass {
        claim: DueBatchClaim,
        pass: DueDrainPass,
    },
    PublishWhileOwned,
}

/// Test-support receipt of one due unit, in visit order.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DueUnitStart {
    pub batch_start_unix_millis: i64,
    pub machine_kind: String,
    pub subject_key: String,
}

impl DueBatchState {
    pub fn accept_reply(
        self: &Arc<Self>,
        wake_id: DueDrainWakeId,
        schedule: impl FnOnce(DueBatchClaim) + Send + 'static,
    ) {
        let discarded_handoff = {
            let mut drain = self.drain.lock().unwrap_or_else(PoisonError::into_inner);
            let Ok((next, effects)) =
                DueDrainMachine::reduce(&drain.machine, DueDrainEvent::ReplyAccepted(wake_id))
            else {
                return;
            };
            drain.machine = next;
            if matches!(effects.as_slice(), [DueDrainEffect::ScheduleHandoff(_)]) {
                drop(drain);
                schedule(self.claim_for(wake_id));
                return;
            }
            drain.pending_handoff.replace(Box::new(schedule))
        };
        drop(discarded_handoff);
    }

    pub fn alarm_fired(self: &Arc<Self>, wake_id: DueDrainWakeId) -> Option<AlarmDrainAdmission> {
        match self.reduce(DueDrainEvent::AlarmFired(wake_id))? {
            DueDrainEffect::RunPass { wake_id, pass } => Some(AlarmDrainAdmission::RunPass {
                claim: self.claim_for(wake_id),
                pass,
            }),
            DueDrainEffect::PublishDurableAlarm(_) => Some(AlarmDrainAdmission::PublishWhileOwned),
            _ => None,
        }
    }

    fn claim_for(self: &Arc<Self>, wake_id: DueDrainWakeId) -> DueBatchClaim {
        DueBatchClaim {
            owner: self.clone(),
            wake_id,
        }
    }

    fn reduce(self: &Arc<Self>, event: DueDrainEvent) -> Option<DueDrainEffect> {
        let mut drain = self.drain.lock().unwrap_or_else(PoisonError::into_inner);
        let (next, effects) = DueDrainMachine::reduce(&drain.machine, event).ok()?;
        let effect = effects.into_iter().next();
        let previous_pending = drain.machine.pending_discovery();
        drain.machine = next;
        if previous_pending != next.pending_discovery() {
            let pending_handoff = drain.pending_handoff.take();
            drop(drain);
            if let (Some(schedule), Some(DueDrainEffect::TransferHandoff { next, .. })) =
                (pending_handoff, effect)
            {
                schedule(self.claim_for(next));
            }
        }
        effect
    }

    #[cfg(feature = "test-support")]
    pub fn record_discovery_start(&self) {
        self.reply_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .discoveries_started += 1;
    }

    #[cfg(feature = "test-support")]
    pub fn record_reply_accepted(&self) {
        let mut order = self
            .reply_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let discoveries_started = order.discoveries_started;
        order.discoveries_at_each_reply.push(discoveries_started);
    }

    #[cfg(feature = "test-support")]
    pub fn discoveries_at_each_reply(&self) -> Vec<usize> {
        self.reply_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .discoveries_at_each_reply
            .clone()
    }

    #[cfg(feature = "test-support")]
    pub fn record_batch_start(&self, milliseconds: i64) {
        self.batch_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(milliseconds);
    }

    #[cfg(feature = "test-support")]
    pub fn record_unit_start(&self, start: DueUnitStart) {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(start);
    }

    #[cfg(feature = "test-support")]
    pub fn batch_starts(&self) -> Vec<i64> {
        self.batch_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[cfg(feature = "test-support")]
    pub fn unit_starts(&self) -> Vec<DueUnitStart> {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl DueBatchClaim {
    pub fn wake_id(&self) -> DueDrainWakeId {
        self.wake_id
    }

    pub fn transition(
        &self,
        event: impl FnOnce(DueDrainWakeId) -> DueDrainEvent,
    ) -> Option<DueDrainEffect> {
        self.owner.reduce(event(self.wake_id))
    }
}

impl Drop for DueBatchClaim {
    fn drop(&mut self) {
        self.owner
            .reduce(DueDrainEvent::HandoffDropped(self.wake_id));
    }
}

#[cfg(test)]
#[path = "due_batch_state_test.rs"]
mod tests;
