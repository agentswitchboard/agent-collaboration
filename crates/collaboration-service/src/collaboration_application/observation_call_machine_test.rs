use super::{
    Admission, HistoryArrival, LiveArrival, ObservationArrival, ObservationCallMachine,
    ObservationEffect, ObservationEnding, ObservationRecord, ObservationState,
};
use collaboration_protocol::{ObservationCursor, ObservationEndReason};
use serde_json::json;
use state_machine_core::{
    EventTag, StateMachine, StateTag, TransitionError, check_state_machine_transitions,
};

fn declared_rows() -> Vec<(
    ObservationState,
    ObservationArrival,
    ObservationState,
    Vec<ObservationEffect>,
)> {
    use ObservationArrival as Arrival;
    use ObservationEffect as Effect;
    use ObservationEndReason as Reason;
    use ObservationState as State;

    let cursor = ObservationCursor {
        epoch: 3,
        after_sequence: 41,
    };
    let record = ObservationRecord {
        sequence: 41,
        value: json!({"sequence":41,"event":{"kind":"itemCompleted","itemId":"done"}}),
    };
    let bare_gap = json!({"kind":"resyncRequired"});
    let numbered_gap = json!({"sequence":42,"event":{"kind":"resyncRequired","replayEpoch":4}});
    let limit = State::Ended(ObservationEnding::Result(Reason::ResultLimitReached));
    let resync = State::Ended(ObservationEnding::Result(Reason::ResyncRequired));

    vec![
        (
            State::Admitting,
            Arrival::Admission(Admission::AttachFailed),
            State::Ended(ObservationEnding::AttachFailed),
            vec![],
        ),
        (
            State::Admitting,
            Arrival::Admission(Admission::Refused),
            resync.clone(),
            vec![Effect::ResyncRequired(bare_gap.clone())],
        ),
        (
            State::Admitting,
            Arrival::Admission(Admission::Accepted),
            State::History,
            vec![],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::Record(record.clone())),
            State::History,
            vec![Effect::Record(record.clone())],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::LimitWithMore(record.clone())),
            limit.clone(),
            vec![Effect::Record(record.clone())],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::LimitAtEnd(record.clone(), cursor)),
            limit.clone(),
            vec![Effect::Record(record.clone()), Effect::SnapshotEnd(cursor)],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::DoesNotFit),
            limit.clone(),
            vec![],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::Gap(numbered_gap.clone())),
            resync.clone(),
            vec![Effect::ResyncRequired(numbered_gap.clone())],
        ),
        (
            State::History,
            Arrival::History(HistoryArrival::Exhausted(cursor)),
            State::Live,
            vec![Effect::SnapshotEnd(cursor)],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Record(record.clone())),
            State::Live,
            vec![Effect::Record(record.clone())],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Limit(record.clone())),
            limit.clone(),
            vec![Effect::Record(record)],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::DoesNotFit),
            limit,
            vec![],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Gap(numbered_gap.clone())),
            resync.clone(),
            vec![Effect::ResyncRequired(numbered_gap)],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Gap(bare_gap.clone())),
            resync,
            vec![Effect::ResyncRequired(bare_gap)],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Disconnected),
            State::Ended(ObservationEnding::Result(Reason::BackendDisconnected)),
            vec![],
        ),
        (
            State::Live,
            Arrival::Live(LiveArrival::Deadline),
            State::Ended(ObservationEnding::Result(Reason::DeadlineReached)),
            vec![],
        ),
    ]
}

// Rejects a transition that the machine runs without declaring, or declares without running.
#[test]
fn every_state_and_arrival_matches_the_declared_transitions() {
    let rows = declared_rows();
    let cases = [
        ObservationState::Admitting,
        ObservationState::History,
        ObservationState::Live,
        ObservationState::Ended(ObservationEnding::AttachFailed),
    ]
    .into_iter()
    .flat_map(|state| {
        rows.iter()
            .map(move |(_, arrival, _, _)| (state.clone(), arrival.clone()))
    });

    assert_eq!(
        check_state_machine_transitions::<ObservationCallMachine>(cases),
        Ok(())
    );
}

// Rejects a correct edge with missing or reordered emissions, a wrong end reason, or output after Ended.
#[test]
fn each_transition_sends_the_expected_order_and_reason() {
    let rows = declared_rows();
    let ended = ObservationState::Ended(ObservationEnding::Result(
        ObservationEndReason::ResyncRequired,
    ));
    for (_, arrival, _, _) in &rows {
        assert_eq!(
            ObservationCallMachine::transition(&ended, arrival.clone()),
            Err(TransitionError::Stale)
        );
    }
    for active_state in [
        ObservationState::Admitting,
        ObservationState::History,
        ObservationState::Live,
    ] {
        for (allowed_state, arrival, _, _) in &rows {
            if &active_state != allowed_state {
                assert_eq!(
                    ObservationCallMachine::transition(&active_state, arrival.clone()),
                    Err(TransitionError::NotAllowed {
                        state: active_state.state_tag(),
                        event: arrival.event_tag(),
                    }),
                );
            }
        }
    }
    for (state, arrival, next, effects) in rows {
        let observed = ObservationCallMachine::transition(&state, arrival.clone());
        assert_eq!(observed, Ok((next, effects)), "{state:?} + {arrival:?}");
    }
}
