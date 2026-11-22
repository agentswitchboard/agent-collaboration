//! Applies the call machine's ordered effects to the result and notification sink.
use super::{
    ObservationFailure, ObservedEventSink,
    observation_call_machine::{
        ObservationArrival, ObservationCallMachine, ObservationEffect, ObservationState,
    },
};
use collaboration_protocol::{
    ObservationCursor, ObservationEventNotification, ProviderSnapshotMarker,
};
use serde_json::Value;
use state_machine_core::StateMachine;

pub(super) struct ObservationDriver {
    pub(super) state: ObservationState,
    pub(super) events: Vec<Value>,
    observed: Option<ObservedEventSink>,
}

impl ObservationDriver {
    pub(super) fn new(observed: Option<ObservedEventSink>) -> Self {
        Self {
            state: ObservationState::Admitting,
            events: Vec::new(),
            observed,
        }
    }

    pub(super) fn dispatch(
        &mut self,
        arrival: ObservationArrival,
        epoch: u64,
    ) -> Result<(), ObservationFailure> {
        let Ok((next, effects)) = ObservationCallMachine::transition(&self.state, arrival) else {
            return Ok(());
        };
        self.state = next;
        for effect in effects {
            let notification = match effect {
                ObservationEffect::Record(record) => ObservationEventNotification {
                    event: record.value,
                    cursor: Some(ObservationCursor {
                        epoch,
                        after_sequence: record.sequence,
                    }),
                },
                ObservationEffect::SnapshotEnd(cursor) => {
                    let event =
                        serde_json::to_value(ProviderSnapshotMarker::SnapshotEnd { cursor })
                            .map_err(|_| ObservationFailure::Unavailable)?;
                    ObservationEventNotification {
                        event,
                        cursor: Some(cursor),
                    }
                }
                ObservationEffect::ResyncRequired(gap) => {
                    ObservationEventNotification::resync_marker(gap)
                }
            };
            self.events.push(notification.event.clone());
            if let Some(observed) = &self.observed {
                let _streamed = observed.send(notification);
            }
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
#[path = "observation_driver_test_support.rs"]
pub mod test_support;
