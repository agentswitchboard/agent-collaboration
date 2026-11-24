use crate::{
    Context, DueBatchPolicy, EngineTime, EventTag, Machine, MachineKind, RefusalTag, StateTag,
    Transition, TransitionError, TransitionResult,
};
use uuid::Uuid;

/// One accepted reply or physical alarm, identified by its caller's UUIDv7.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DueDrainWakeId(pub Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DueDrainPass(u8);

impl DueDrainPass {
    const FIRST: Self = Self(1);

    pub fn number(self) -> usize {
        usize::from(self.0)
    }

    fn next(self) -> Option<Self> {
        (self.number() < DueBatchPolicy::DEFAULT.max_batches_per_wake).then(|| Self(self.0 + 1))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct DueDrainState {
    owner: Option<DueDrainOwner>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DueDrainOwner {
    wake_id: DueDrainWakeId,
    pending_discovery: Option<DueDrainWakeId>,
    phase: DueDrainPhase,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DueDrainPhase {
    HandedOff,
    Draining {
        pass: DueDrainPass,
        discovery_covers: Option<DueDrainWakeId>,
    },
    WaitingForYield {
        next_pass: DueDrainPass,
    },
    PublishingAlarm,
    StoppedOnFault,
}

impl DueDrainState {
    pub fn owner(self) -> Option<DueDrainWakeId> {
        self.owner.map(|owner| owner.wake_id)
    }

    pub fn pending_discovery(self) -> Option<DueDrainWakeId> {
        self.owner.and_then(|owner| owner.pending_discovery)
    }
}

impl StateTag for DueDrainState {
    fn state_tag(&self) -> &'static str {
        match self.owner.map(|owner| owner.phase) {
            None => "idle",
            Some(DueDrainPhase::HandedOff) => "handed_off",
            Some(DueDrainPhase::Draining { .. }) => "draining",
            Some(DueDrainPhase::WaitingForYield { .. }) => "waiting_for_yield",
            Some(DueDrainPhase::PublishingAlarm) => "publishing_alarm",
            Some(DueDrainPhase::StoppedOnFault) => "stopped_on_fault",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DueDrainEvent {
    ReplyAccepted(DueDrainWakeId),
    AlarmFired(DueDrainWakeId),
    HandoffStarted(DueDrainWakeId),
    FullPassMadeProgress(DueDrainWakeId),
    FullPassMadeNoProgress(DueDrainWakeId),
    ShortPassFinished(DueDrainWakeId),
    DrainFaulted(DueDrainWakeId),
    HostYielded(DueDrainWakeId),
    HostYieldFailed(DueDrainWakeId),
    AlarmPublicationSettled(DueDrainWakeId),
    HandoffDropped(DueDrainWakeId),
}

impl DueDrainEvent {
    fn wake_id(self) -> DueDrainWakeId {
        match self {
            Self::ReplyAccepted(wake_id)
            | Self::AlarmFired(wake_id)
            | Self::HandoffStarted(wake_id)
            | Self::FullPassMadeProgress(wake_id)
            | Self::FullPassMadeNoProgress(wake_id)
            | Self::ShortPassFinished(wake_id)
            | Self::DrainFaulted(wake_id)
            | Self::HostYielded(wake_id)
            | Self::HostYieldFailed(wake_id)
            | Self::AlarmPublicationSettled(wake_id)
            | Self::HandoffDropped(wake_id) => wake_id,
        }
    }
}

impl EventTag for DueDrainEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::ReplyAccepted(_) => "reply_accepted",
            Self::AlarmFired(_) => "alarm_fired",
            Self::HandoffStarted(_) => "handoff_started",
            Self::FullPassMadeProgress(_) => "full_pass_made_progress",
            Self::FullPassMadeNoProgress(_) => "full_pass_made_no_progress",
            Self::ShortPassFinished(_) => "short_pass_finished",
            Self::DrainFaulted(_) => "drain_faulted",
            Self::HostYielded(_) => "host_yielded",
            Self::HostYieldFailed(_) => "host_yield_failed",
            Self::AlarmPublicationSettled(_) => "alarm_publication_settled",
            Self::HandoffDropped(_) => "handoff_dropped",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DueDrainEffect {
    ScheduleHandoff(DueDrainWakeId),
    RunPass {
        wake_id: DueDrainWakeId,
        pass: DueDrainPass,
    },
    YieldToHost(DueDrainWakeId),
    PublishDurableAlarm(DueDrainWakeId),
    TransferHandoff {
        released: DueDrainWakeId,
        next: DueDrainWakeId,
    },
    ReleaseClaim(DueDrainWakeId),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum DueDrainRefusal {}

impl RefusalTag for DueDrainRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

pub struct DueDrainMachine;

impl Machine for DueDrainMachine {
    const KIND: MachineKind = MachineKind("due_drain");
    type State = DueDrainState;
    type Event = DueDrainEvent;
    type Effect = DueDrainEffect;
    type Error = DueDrainRefusal;

    fn transition(
        state: &DueDrainState,
        event: DueDrainEvent,
        _: &mut Context<'_>,
    ) -> TransitionResult<Self> {
        Self::reduce(state, event)
    }

    fn transitions() -> &'static [Transition] {
        super::due_drain_transitions::DUE_DRAIN_TRANSITIONS
    }

    fn next_due(_: &DueDrainState) -> Option<EngineTime> {
        // The physical alarm is derived from the durable due rows.
        None
    }
}

impl DueDrainMachine {
    pub fn reduce(state: &DueDrainState, event: DueDrainEvent) -> TransitionResult<Self> {
        let wake_id = event.wake_id();
        let refuse_event = || TransitionError::NotAllowed {
            state: state.state_tag(),
            event: event.event_tag(),
        };
        let Some(mut owner) = state.owner else {
            return match event {
                DueDrainEvent::ReplyAccepted(_) => Ok(schedule_handoff(wake_id)),
                DueDrainEvent::AlarmFired(_) => {
                    Ok(DueDrainOwner::new(wake_id).begin_pass(DueDrainPass::FIRST))
                }
                DueDrainEvent::HandoffStarted(_)
                | DueDrainEvent::FullPassMadeProgress(_)
                | DueDrainEvent::FullPassMadeNoProgress(_)
                | DueDrainEvent::ShortPassFinished(_)
                | DueDrainEvent::DrainFaulted(_)
                | DueDrainEvent::HostYielded(_)
                | DueDrainEvent::HostYieldFailed(_)
                | DueDrainEvent::AlarmPublicationSettled(_)
                | DueDrainEvent::HandoffDropped(_) => Err(refuse_event()),
            };
        };

        match event {
            DueDrainEvent::ReplyAccepted(_) if owner.wake_id != wake_id => {
                owner.pending_discovery = Some(wake_id);
                return Ok((owner.state(), Vec::new()));
            }
            DueDrainEvent::ReplyAccepted(_) => return Err(TransitionError::Stale),
            DueDrainEvent::AlarmFired(_) => {
                // An alarm can have fired while the owner was suspended. Preserve
                // the durable fallback without admitting a second drain.
                return Ok((*state, vec![DueDrainEffect::PublishDurableAlarm(wake_id)]));
            }
            DueDrainEvent::HandoffStarted(_)
            | DueDrainEvent::FullPassMadeProgress(_)
            | DueDrainEvent::FullPassMadeNoProgress(_)
            | DueDrainEvent::ShortPassFinished(_)
            | DueDrainEvent::DrainFaulted(_)
            | DueDrainEvent::HostYielded(_)
            | DueDrainEvent::HostYieldFailed(_)
            | DueDrainEvent::AlarmPublicationSettled(_)
            | DueDrainEvent::HandoffDropped(_)
                if owner.wake_id != wake_id =>
            {
                return Err(TransitionError::Stale);
            }
            DueDrainEvent::HandoffStarted(_)
            | DueDrainEvent::FullPassMadeProgress(_)
            | DueDrainEvent::FullPassMadeNoProgress(_)
            | DueDrainEvent::ShortPassFinished(_)
            | DueDrainEvent::DrainFaulted(_)
            | DueDrainEvent::HostYielded(_)
            | DueDrainEvent::HostYieldFailed(_)
            | DueDrainEvent::AlarmPublicationSettled(_)
            | DueDrainEvent::HandoffDropped(_) => {}
        }

        match (owner.phase, event) {
            (DueDrainPhase::HandedOff, DueDrainEvent::HandoffStarted(_)) => {
                Ok(owner.begin_pass(DueDrainPass::FIRST))
            }
            (
                DueDrainPhase::Draining {
                    pass,
                    discovery_covers,
                },
                DueDrainEvent::FullPassMadeProgress(_) | DueDrainEvent::ShortPassFinished(_),
            ) => {
                if owner.pending_discovery == discovery_covers {
                    owner.pending_discovery = None;
                }
                let needs_pass = matches!(event, DueDrainEvent::FullPassMadeProgress(_))
                    || owner.pending_discovery.is_some();
                match pass.next().filter(|_| needs_pass) {
                    Some(next_pass) => Ok(owner.wait_for_yield(next_pass)),
                    None => Ok(owner.finish(DueDrainPhase::PublishingAlarm)),
                }
            }
            (DueDrainPhase::Draining { .. }, DueDrainEvent::FullPassMadeNoProgress(_)) => {
                Ok(owner.finish(DueDrainPhase::PublishingAlarm))
            }
            (DueDrainPhase::WaitingForYield { next_pass }, DueDrainEvent::HostYielded(_)) => {
                Ok(owner.begin_pass(next_pass))
            }
            (DueDrainPhase::Draining { .. }, DueDrainEvent::DrainFaulted(_))
            | (DueDrainPhase::WaitingForYield { .. }, DueDrainEvent::HostYieldFailed(_)) => {
                Ok(owner.finish(DueDrainPhase::StoppedOnFault))
            }
            (
                DueDrainPhase::PublishingAlarm | DueDrainPhase::StoppedOnFault,
                DueDrainEvent::AlarmPublicationSettled(_),
            )
            | (_, DueDrainEvent::HandoffDropped(_)) => Ok(owner.release()),
            (
                _,
                DueDrainEvent::ReplyAccepted(_)
                | DueDrainEvent::AlarmFired(_)
                | DueDrainEvent::HandoffStarted(_)
                | DueDrainEvent::FullPassMadeProgress(_)
                | DueDrainEvent::FullPassMadeNoProgress(_)
                | DueDrainEvent::ShortPassFinished(_)
                | DueDrainEvent::DrainFaulted(_)
                | DueDrainEvent::HostYielded(_)
                | DueDrainEvent::HostYieldFailed(_)
                | DueDrainEvent::AlarmPublicationSettled(_),
            ) => Err(refuse_event()),
        }
    }
}

fn schedule_handoff(wake_id: DueDrainWakeId) -> (DueDrainState, Vec<DueDrainEffect>) {
    (
        DueDrainOwner::new(wake_id).state(),
        vec![DueDrainEffect::ScheduleHandoff(wake_id)],
    )
}

impl DueDrainOwner {
    fn new(wake_id: DueDrainWakeId) -> Self {
        Self {
            wake_id,
            pending_discovery: None,
            phase: DueDrainPhase::HandedOff,
        }
    }

    fn state(self) -> DueDrainState {
        DueDrainState { owner: Some(self) }
    }

    fn begin_pass(mut self, pass: DueDrainPass) -> (DueDrainState, Vec<DueDrainEffect>) {
        self.phase = DueDrainPhase::Draining {
            pass,
            discovery_covers: self.pending_discovery,
        };
        (
            self.state(),
            vec![DueDrainEffect::RunPass {
                wake_id: self.wake_id,
                pass,
            }],
        )
    }

    fn wait_for_yield(mut self, next_pass: DueDrainPass) -> (DueDrainState, Vec<DueDrainEffect>) {
        self.phase = DueDrainPhase::WaitingForYield { next_pass };
        (
            self.state(),
            vec![DueDrainEffect::YieldToHost(self.wake_id)],
        )
    }

    fn finish(mut self, phase: DueDrainPhase) -> (DueDrainState, Vec<DueDrainEffect>) {
        self.phase = phase;
        (
            self.state(),
            vec![DueDrainEffect::PublishDurableAlarm(self.wake_id)],
        )
    }

    fn release(self) -> (DueDrainState, Vec<DueDrainEffect>) {
        match self.pending_discovery {
            Some(next) => (
                Self::new(next).state(),
                vec![DueDrainEffect::TransferHandoff {
                    released: self.wake_id,
                    next,
                }],
            ),
            None => (
                DueDrainState::default(),
                vec![DueDrainEffect::ReleaseClaim(self.wake_id)],
            ),
        }
    }
}

#[cfg(test)]
#[path = "due_drain_machine_test.rs"]
mod tests;
