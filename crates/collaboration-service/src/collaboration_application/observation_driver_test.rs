use super::{LabelledObservationArrival as Input, drive_labelled_observation};
use collaboration_protocol::{
    ObservationCursor, ObservationEndReason, ObservationEventNotification,
};
use serde_json::{Value, json};

const CURSOR: ObservationCursor = ObservationCursor {
    epoch: 4,
    after_sequence: 1,
};

fn record(sequence: u64) -> Value {
    json!({"sequence":sequence,"event":{"kind":"itemCompleted","itemId":"fixture-item"}})
}

fn snapshot() -> Value {
    json!({"kind":"snapshotEnd","cursor":{"epoch":4,"afterSequence":1}})
}

fn observed(
    arrivals: impl IntoIterator<Item = Input>,
) -> (
    Vec<Value>,
    Vec<ObservationEventNotification>,
    Option<ObservationEndReason>,
) {
    let (sink, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let (events, reason) = drive_labelled_observation(CURSOR.epoch, arrivals, sink).unwrap();
    let mut notifications = Vec::new();
    while let Ok(notification) = receiver.try_recv() {
        notifications.push(notification);
    }
    assert_eq!(
        notifications.iter().map(|n| &n.event).collect::<Vec<_>>(),
        events.iter().collect::<Vec<_>>()
    );
    (events, notifications, reason)
}

// Rejects early live output, a duplicate marker, late history, or output after Ended.
#[test]
fn the_driver_only_emits_the_machine_authorized_order() {
    let (events, streamed, reason) = observed([
        Input::Accepted,
        Input::Live {
            sequence: 2,
            value: record(2),
        },
        Input::Kept {
            sequence: 1,
            value: record(1),
        },
        Input::HistoryExhausted(CURSOR),
        Input::HistoryExhausted(CURSOR),
        Input::Kept {
            sequence: 3,
            value: record(3),
        },
        Input::Deadline,
        Input::Live {
            sequence: 4,
            value: record(4),
        },
    ]);

    assert_eq!(events, vec![record(1), snapshot(), record(2)]);
    assert_eq!(streamed[1].cursor, Some(CURSOR));
    assert_eq!(reason, Some(ObservationEndReason::DeadlineReached));
}

// Rejects losing the cut on exact fit or suppressing the gap after earlier records.
#[test]
fn exact_fit_and_post_marker_gap_reach_both_outputs() {
    let (exact, exact_notifications, exact_reason) = observed([
        Input::Accepted,
        Input::LastKeptAtLimit {
            cursor: CURSOR,
            value: record(1),
        },
    ]);
    let (gap, gap_notifications, gap_reason) = observed([
        Input::Accepted,
        Input::Kept {
            sequence: 1,
            value: record(1),
        },
        Input::HistoryExhausted(CURSOR),
        Input::LiveGap,
        Input::Live {
            sequence: 2,
            value: record(2),
        },
    ]);

    assert_eq!(exact, vec![record(1), snapshot()]);
    assert_eq!(exact_notifications[1].cursor, Some(CURSOR));
    assert_eq!(exact_reason, Some(ObservationEndReason::ResultLimitReached));
    assert_eq!(
        gap,
        vec![record(1), snapshot(), json!({"kind":"resyncRequired"})]
    );
    assert_eq!(gap_notifications[2].cursor, None);
    assert_eq!(gap_reason, Some(ObservationEndReason::ResyncRequired));
}
