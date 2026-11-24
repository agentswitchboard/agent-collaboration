//! Labelled arrivals for testing the real reducer-to-notification path.
use super::{
    super::observation_call_machine::{
        Admission, HistoryArrival, LiveArrival, ObservationArrival, ObservationEnding,
        ObservationRecord, ObservationState, bare_gap,
    },
    ObservationDriver,
};
use collaboration_protocol::{ObservationCursor, ObservationEndReason};
use serde_json::Value;
use std::collections::VecDeque;

pub enum LabelledObservationArrival {
    Accepted,
    Kept {
        sequence: u64,
        value: Value,
    },
    LastKeptAtLimit {
        cursor: ObservationCursor,
        value: Value,
    },
    HistoryExhausted(ObservationCursor),
    Live {
        sequence: u64,
        value: Value,
    },
    LiveGap,
    Deadline,
}

pub fn drive_labelled_observation(
    epoch: u64,
    arrivals: impl IntoIterator<Item = LabelledObservationArrival>,
    observed: super::ObservedEventSink,
) -> Result<(Vec<Value>, Option<ObservationEndReason>), super::ObservationFailure> {
    use LabelledObservationArrival as Input;
    let mut driver = ObservationDriver::new(Some(observed));
    let mut queued_live = VecDeque::new();
    for arrival in arrivals {
        let arrival = match arrival {
            Input::Accepted => ObservationArrival::Admission(Admission::Accepted),
            Input::Kept { sequence, value } => {
                ObservationArrival::History(HistoryArrival::Record(ObservationRecord {
                    sequence,
                    value,
                }))
            }
            Input::LastKeptAtLimit { cursor, value } => {
                ObservationArrival::History(HistoryArrival::LimitAtEnd(
                    ObservationRecord {
                        sequence: cursor.after_sequence,
                        value,
                    },
                    cursor,
                ))
            }
            Input::HistoryExhausted(cursor) => {
                ObservationArrival::History(HistoryArrival::Exhausted(cursor))
            }
            Input::Live { sequence, value } => {
                let live = ObservationArrival::Live(LiveArrival::Record(ObservationRecord {
                    sequence,
                    value,
                }));
                if driver.state == ObservationState::History {
                    // Like the broadcast receiver, hold a live frame until the driver polls live.
                    queued_live.push_back(live);
                    continue;
                }
                live
            }
            Input::LiveGap => ObservationArrival::Live(LiveArrival::Gap(bare_gap())),
            Input::Deadline => ObservationArrival::Live(LiveArrival::Deadline),
        };
        driver.dispatch(arrival, epoch)?;
        if driver.state == ObservationState::Live {
            for arrival in queued_live.drain(..) {
                driver.dispatch(arrival, epoch)?;
            }
        }
    }
    let reason = match driver.state {
        ObservationState::Ended(ObservationEnding::Result(reason)) => Some(reason),
        _ => None,
    };
    Ok((driver.events, reason))
}

#[cfg(test)]
#[path = "observation_driver_test.rs"]
mod tests;
