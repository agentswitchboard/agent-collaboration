use super::*;
use crate::{IdSource, check_transitions};
use std::collections::{HashSet, VecDeque};

const FIRST_REPLY: DueDrainWakeId = wake_id(1);
const COMPETING_REPLY: DueDrainWakeId = wake_id(2);
const FALLBACK_ALARM: DueDrainWakeId = wake_id(3);

const fn wake_id(suffix: u8) -> DueDrainWakeId {
    DueDrainWakeId(Uuid::from_bytes([
        0, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, suffix,
    ]))
}

struct NoGeneratedIds;

impl IdSource for NoGeneratedIds {
    fn next_id(&mut self) -> Uuid {
        panic!("the drain receives identifiers from the caller")
    }
}

fn reduce(state: DueDrainState, event: DueDrainEvent) -> TransitionResult<DueDrainMachine> {
    DueDrainMachine::transition(
        &state,
        event,
        &mut Context {
            now: EngineTime::UNIX_EPOCH,
            ids: &mut NoGeneratedIds,
        },
    )
}

fn events(wake_id: DueDrainWakeId) -> [DueDrainEvent; 11] {
    [
        DueDrainEvent::ReplyAccepted(wake_id),
        DueDrainEvent::AlarmFired(wake_id),
        DueDrainEvent::HandoffStarted(wake_id),
        DueDrainEvent::FullPassMadeProgress(wake_id),
        DueDrainEvent::FullPassMadeNoProgress(wake_id),
        DueDrainEvent::ShortPassFinished(wake_id),
        DueDrainEvent::DrainFaulted(wake_id),
        DueDrainEvent::HostYielded(wake_id),
        DueDrainEvent::HostYieldFailed(wake_id),
        DueDrainEvent::AlarmPublicationSettled(wake_id),
        DueDrainEvent::HandoffDropped(wake_id),
    ]
}

fn reachable_cases() -> Vec<(DueDrainState, DueDrainEvent)> {
    let idle = DueDrainState::default();
    let mut seen_states = HashSet::from([idle]);
    let mut states_to_visit = VecDeque::from([idle]);
    let mut cases = Vec::new();
    while let Some(state) = states_to_visit.pop_front() {
        for event in [FIRST_REPLY, COMPETING_REPLY, FALLBACK_ALARM]
            .into_iter()
            .flat_map(events)
        {
            cases.push((state, event));
            if let Ok((next, _)) = reduce(state, event)
                && seen_states.insert(next)
            {
                states_to_visit.push_back(next);
            }
        }
    }
    cases
}

#[test]
fn every_reachable_state_and_event_pair_matches_the_transition_table_or_is_refused() {
    let cases = reachable_cases();
    check_transitions::<DueDrainMachine>(
        cases.iter().copied(),
        &mut Context {
            now: EngineTime::UNIX_EPOCH,
            ids: &mut NoGeneratedIds,
        },
    )
    .unwrap();

    for (state, event) in cases {
        match reduce(state, event) {
            Ok((next, effects)) => check_ownership_and_pass(state, next, &effects),
            Err(TransitionError::Stale) => {
                let owner = state.owner().expect("a stale event needs a current owner");
                assert_eq!(
                    owner == event.wake_id(),
                    matches!(event, DueDrainEvent::ReplyAccepted(_))
                );
            }
            Err(TransitionError::NotAllowed {
                state: state_tag,
                event: event_tag,
            }) => {
                assert_eq!(state_tag, state.state_tag());
                assert_eq!(event_tag, event.event_tag());
                assert!(!DueDrainMachine::transitions().iter().any(|transition| {
                    transition.from == state_tag && transition.event == event_tag
                }));
            }
            Err(TransitionError::Domain(refusal)) => match refusal {},
        }
    }
}

fn check_ownership_and_pass(
    before: DueDrainState,
    after: DueDrainState,
    effects: &[DueDrainEffect],
) {
    match effects {
        [DueDrainEffect::ScheduleHandoff(wake_id)] => {
            assert_eq!(before.owner(), None);
            assert_eq!(after.owner(), Some(*wake_id));
        }
        [DueDrainEffect::RunPass { wake_id, pass }] => {
            assert_eq!(after.owner(), Some(*wake_id));
            assert!(pass.number() <= 4);
            match before.owner.map(|owner| owner.phase) {
                None | Some(DueDrainPhase::HandedOff) => assert_eq!(pass.number(), 1),
                Some(DueDrainPhase::WaitingForYield { next_pass }) => {
                    assert_eq!(*pass, next_pass);
                    assert!(pass.number() > 1);
                }
                Some(
                    DueDrainPhase::Draining { .. }
                    | DueDrainPhase::PublishingAlarm
                    | DueDrainPhase::StoppedOnFault,
                ) => panic!("a pass began before the current owner finished its phase"),
            }
        }
        [DueDrainEffect::TransferHandoff { released, next }] => {
            assert_eq!(before.owner(), Some(*released));
            assert_eq!(before.pending_discovery(), Some(*next));
            assert_eq!(after.owner(), Some(*next));
            assert_eq!(after.pending_discovery(), None);
            assert_ne!(released, next);
        }
        [DueDrainEffect::ReleaseClaim(wake_id)] => {
            assert_eq!(before.owner(), Some(*wake_id));
            assert_eq!(before.pending_discovery(), None);
            assert_eq!(after.owner(), None);
        }
        [] | [DueDrainEffect::YieldToHost(_) | DueDrainEffect::PublishDurableAlarm(_)] => {
            assert_eq!(before.owner(), after.owner());
        }
        _ => panic!("a transition produced competing due-drain effects: {effects:?}"),
    }
}

#[test]
fn every_reachable_claim_can_settle_or_drop_and_the_alarm_can_recover() {
    let states: HashSet<_> = reachable_cases()
        .into_iter()
        .map(|(state, _)| state)
        .collect();
    for starting_state in states {
        for abandon_handoff in [false, true] {
            let mut state = starting_state;
            let mut transferred = None;
            let mut settled = false;
            for _ in 0..20 {
                let Some(owner) = state.owner else {
                    settled = true;
                    break;
                };
                let event = if abandon_handoff {
                    DueDrainEvent::HandoffDropped(owner.wake_id)
                } else {
                    match owner.phase {
                        DueDrainPhase::HandedOff => DueDrainEvent::HandoffStarted(owner.wake_id),
                        DueDrainPhase::Draining { .. } => {
                            DueDrainEvent::FullPassMadeProgress(owner.wake_id)
                        }
                        DueDrainPhase::WaitingForYield { .. } => {
                            DueDrainEvent::HostYielded(owner.wake_id)
                        }
                        DueDrainPhase::PublishingAlarm | DueDrainPhase::StoppedOnFault => {
                            DueDrainEvent::AlarmPublicationSettled(owner.wake_id)
                        }
                    }
                };
                let (next, effects) = reduce(state, event).ok().expect("fair event");
                if let Some(pending) = state.pending_discovery() {
                    assert!(
                        next.pending_discovery() == Some(pending)
                            || next.owner() == Some(pending)
                            || matches!(owner.phase, DueDrainPhase::Draining {
                                discovery_covers: Some(covered), ..
                            } if covered == pending && matches!(event, DueDrainEvent::FullPassMadeProgress(_)))
                    );
                }
                if let [DueDrainEffect::TransferHandoff { next, .. }] = effects.as_slice() {
                    assert_eq!(transferred.replace(*next), None);
                }
                state = next;
            }
            assert!(settled, "claim failed to settle from {starting_state:?}");
            let (recovered, effects) = reduce(state, DueDrainEvent::AlarmFired(FALLBACK_ALARM))
                .ok()
                .expect("the physical fallback can always reclaim an idle drain");
            assert_eq!(recovered.owner(), Some(FALLBACK_ALARM));
            assert_eq!(
                effects,
                [DueDrainEffect::RunPass {
                    wake_id: FALLBACK_ALARM,
                    pass: DueDrainPass::FIRST,
                }]
            );
        }
    }
}

#[test]
fn a_reply_accepted_after_fourth_discovery_transfers_to_its_own_first_pass() {
    let (mut state, _) = reduce(
        DueDrainState::default(),
        DueDrainEvent::ReplyAccepted(FIRST_REPLY),
    )
    .ok()
    .unwrap();
    state = reduce(state, DueDrainEvent::HandoffStarted(FIRST_REPLY))
        .ok()
        .unwrap()
        .0;
    for next_pass in 2..=4 {
        let (yielding, effects) = reduce(state, DueDrainEvent::FullPassMadeProgress(FIRST_REPLY))
            .ok()
            .unwrap();
        assert_eq!(effects, [DueDrainEffect::YieldToHost(FIRST_REPLY)]);
        let (draining, effects) = reduce(yielding, DueDrainEvent::HostYielded(FIRST_REPLY))
            .ok()
            .unwrap();
        assert_eq!(
            effects,
            [DueDrainEffect::RunPass {
                wake_id: FIRST_REPLY,
                pass: DueDrainPass(next_pass),
            }]
        );
        state = draining;
    }
    let (pending, effects) = reduce(state, DueDrainEvent::ReplyAccepted(COMPETING_REPLY))
        .ok()
        .unwrap();
    assert!(effects.is_empty());
    let (publishing, effects) = reduce(pending, DueDrainEvent::FullPassMadeProgress(FIRST_REPLY))
        .ok()
        .unwrap();
    assert_eq!(effects, [DueDrainEffect::PublishDurableAlarm(FIRST_REPLY)]);
    let (transferred, effects) = reduce(
        publishing,
        DueDrainEvent::AlarmPublicationSettled(FIRST_REPLY),
    )
    .ok()
    .unwrap();
    assert_eq!(
        effects,
        [DueDrainEffect::TransferHandoff {
            released: FIRST_REPLY,
            next: COMPETING_REPLY,
        }]
    );
    assert!(matches!(
        reduce(transferred, DueDrainEvent::HandoffDropped(FIRST_REPLY)),
        Err(TransitionError::Stale)
    ));
    let (_, effects) = reduce(transferred, DueDrainEvent::HandoffStarted(COMPETING_REPLY))
        .ok()
        .unwrap();
    assert_eq!(
        effects,
        [DueDrainEffect::RunPass {
            wake_id: COMPETING_REPLY,
            pass: DueDrainPass::FIRST,
        }]
    );
}

#[test]
fn a_fault_or_failed_yield_preserves_already_accepted_discovery() {
    let (running, _) = reduce(
        DueDrainState::default(),
        DueDrainEvent::AlarmFired(FALLBACK_ALARM),
    )
    .ok()
    .unwrap();
    let (waiting, _) = reduce(running, DueDrainEvent::FullPassMadeProgress(FALLBACK_ALARM))
        .ok()
        .unwrap();
    for (state, fault) in [
        (running, DueDrainEvent::DrainFaulted(FALLBACK_ALARM)),
        (waiting, DueDrainEvent::HostYieldFailed(FALLBACK_ALARM)),
    ] {
        let (pending, _) = reduce(state, DueDrainEvent::ReplyAccepted(COMPETING_REPLY))
            .ok()
            .unwrap();
        let (faulted, effects) = reduce(pending, fault).ok().unwrap();
        assert_eq!(faulted.state_tag(), "stopped_on_fault");
        assert_eq!(
            effects,
            [DueDrainEffect::PublishDurableAlarm(FALLBACK_ALARM)]
        );
        let (_, effects) = reduce(
            faulted,
            DueDrainEvent::AlarmPublicationSettled(FALLBACK_ALARM),
        )
        .ok()
        .unwrap();
        assert_eq!(
            effects,
            [DueDrainEffect::TransferHandoff {
                released: FALLBACK_ALARM,
                next: COMPETING_REPLY,
            }]
        );
    }
}
