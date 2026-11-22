//! Provider Session observation: bounded, resumable reads of the same hub attached faces use.
//! Following a Session is a chain of these reads, each resuming after the last event.
use super::{
    observation_call_driver::{ObservationDriver, ObservedEventSink},
    observation_call_machine::{
        Admission, HistoryArrival, LiveArrival, ObservationArrival, ObservationEnding,
        ObservationRecord, ObservationState, bare_gap,
    },
    observation_failure::ObservationFailure,
};
use collaboration_contracts::HubEvent;
use collaboration_contracts::SessionEventAttachment;
use collaboration_contracts::SessionEventHubError;
use collaboration_protocol::{
    BoundedObservationRequest, BoundedObservationResult, ChannelDescription, CodexGeneration,
    EndpointAvailability, ObservationCursor, ProviderObservationEventTooLarge,
    ProviderObservationEventTooLargeKind, SessionRef, UuidIdentity,
};
use router_clock::RuntimeClock;
use serde_json::{Value, json};
use session_event_model::SessionEvent;
use tokio::sync::broadcast;

/// Observation operations over the provider Session event hub.
pub struct ObservationOperations {
    pub(super) identity: collaboration_contracts::RouterIdentity,
    pub(super) provider_session_hub:
        Option<std::sync::Arc<dyn collaboration_contracts::SessionEventHub>>,
    pub(super) directory: collaboration_runtime::EndpointDirectory,
    clock: std::sync::Arc<dyn RuntimeClock>,
}

impl ObservationOperations {
    pub fn new(
        identity: collaboration_contracts::RouterIdentity,
        provider_session_hub: Option<std::sync::Arc<dyn collaboration_contracts::SessionEventHub>>,
        directory: collaboration_runtime::EndpointDirectory,
        clock: std::sync::Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            identity,
            provider_session_hub,
            directory,
            clock,
        }
    }

    /// The Host clock shared by the collector and its notification carrier.
    #[must_use]
    pub fn clock(&self) -> &dyn RuntimeClock {
        self.clock.as_ref()
    }

    /// Collects events after `afterSequence` (in `epoch`) until a bound or the deadline,
    /// handing each one to `observed` as it is collected.
    pub async fn session_observe(
        &self,
        request: BoundedObservationRequest,
        observed: Option<ObservedEventSink>,
    ) -> Result<BoundedObservationResult, ObservationFailure> {
        collect(request, self, observed).await
    }
}

const MAX_EVENTS: usize = 4096;
const MAX_BYTES: usize = 1_048_576;
// A bounded result is one response of at most MAX_BYTES. Leave room for its envelope,
// target, generation and JSON framing when callers request the full byte bound.
const MAX_EVENT_BYTES: usize = MAX_BYTES - 65_536;

async fn collect(
    request: BoundedObservationRequest,
    operations: &ObservationOperations,
    observed: Option<ObservedEventSink>,
) -> Result<BoundedObservationResult, ObservationFailure> {
    if request.timeout_seconds == 0
        || request.max_events == 0
        || request.max_events > MAX_EVENTS
        || request.max_bytes == 0
        || request.max_bytes > MAX_BYTES
    {
        return Err(ObservationFailure::InvalidField);
    }
    let continuation = ObservationContinuation::from_request(&request)?;
    let clock = operations.clock();
    let deadline = clock
        .monotonic_now()
        .checked_add(std::time::Duration::from_secs(request.timeout_seconds))
        .ok_or(ObservationFailure::InvalidField)?;
    let mut driver = ObservationDriver::new(observed);
    let attached = router_clock::timeout_at(clock, deadline, attach(&request.target, operations))
        .await
        .map_err(|_| ObservationFailure::Unavailable)
        .and_then(std::convert::identity);
    let (attachment, generation) = match attached {
        Ok(attached) => attached,
        Err(failure) => {
            driver.dispatch(ObservationArrival::Admission(Admission::AttachFailed), 0)?;
            return Err(failure);
        }
    };

    let ObservationContinuation {
        epoch,
        after_sequence,
        service_epoch,
    } = continuation;
    let current_epoch = attachment.epoch;
    let refused = epoch.is_some_and(|epoch| epoch != current_epoch)
        || after_sequence.is_some_and(|sequence| sequence > attachment.latest_issued_sequence)
        || service_epoch.is_some_and(|epoch| epoch != generation.service_epoch);
    if refused {
        driver.dispatch(
            ObservationArrival::Admission(Admission::Refused),
            current_epoch,
        )?;
    } else {
        driver.dispatch(
            ObservationArrival::Admission(Admission::Accepted),
            current_epoch,
        )?;
        let mut cursor = ObservationCursor {
            epoch: current_epoch,
            after_sequence: after_sequence.unwrap_or(0),
        };
        let mut budget = ObservationBudget {
            remaining_records: request.max_events,
            bytes: 0,
            max_bytes: request.max_bytes,
        };
        let mut history = attachment
            .snapshot
            .into_iter()
            .filter(|event| after_sequence.is_none_or(|after| event.sequence > after))
            .peekable();
        while driver.state == ObservationState::History {
            let arrival = match history.next() {
                None => HistoryArrival::Exhausted(cursor),
                Some(event) if matches!(&event.event, SessionEvent::ResyncRequired { .. }) => {
                    HistoryArrival::Gap(event_value(event)?)
                }
                Some(event) => {
                    let sequence = event.sequence;
                    match budget.admit(event)? {
                        RecordAdmission::Room(record) => {
                            cursor.after_sequence = sequence;
                            HistoryArrival::Record(record)
                        }
                        RecordAdmission::Exact(record) if history.peek().is_none() => {
                            cursor.after_sequence = sequence;
                            HistoryArrival::LimitAtEnd(record, cursor)
                        }
                        RecordAdmission::Exact(record) => HistoryArrival::LimitWithMore(record),
                        RecordAdmission::NoRoom => HistoryArrival::DoesNotFit,
                    }
                }
            };
            driver.dispatch(ObservationArrival::History(arrival), current_epoch)?;
        }
        let mut receiver = attachment.receiver;
        while driver.state == ObservationState::Live {
            let arrival = tokio::select! {
                () = clock.sleep_until(deadline) => LiveArrival::Deadline,
                received = receiver.recv() => match received {
                    Ok(event) if matches!(&event.event, SessionEvent::ResyncRequired { .. }) => {
                        LiveArrival::Gap(event_value(event)?)
                    }
                    Ok(event) => match budget.admit(event)? {
                        RecordAdmission::Room(record) => LiveArrival::Record(record),
                        RecordAdmission::Exact(record) => LiveArrival::Limit(record),
                        RecordAdmission::NoRoom => LiveArrival::DoesNotFit,
                    },
                    Err(broadcast::error::RecvError::Lagged(_)) => LiveArrival::Gap(bare_gap()),
                    Err(broadcast::error::RecvError::Closed) => LiveArrival::Disconnected,
                },
            };
            driver.dispatch(ObservationArrival::Live(arrival), current_epoch)?;
        }
    }
    let ObservationState::Ended(ObservationEnding::Result(end_reason)) = driver.state else {
        return Err(ObservationFailure::Unavailable);
    };
    Ok(BoundedObservationResult {
        target: request.target,
        generation,
        attached: true,
        events: driver.events,
        end_reason,
        continuation_gap: true,
        epoch: Some(current_epoch),
    })
}

struct ObservationContinuation {
    epoch: Option<u64>,
    after_sequence: Option<u64>,
    service_epoch: Option<UuidIdentity>,
}

impl ObservationContinuation {
    fn from_request(request: &BoundedObservationRequest) -> Result<Self, ObservationFailure> {
        if request.epoch.is_none()
            && (request.after_sequence.is_some() || request.service_epoch.is_some())
            || request.service_epoch.is_some() && request.after_sequence.is_none()
        {
            return Err(ObservationFailure::InvalidField);
        }
        Ok(Self {
            epoch: request.epoch,
            after_sequence: request.after_sequence,
            service_epoch: request.service_epoch.clone(),
        })
    }
}

struct ObservationBudget {
    remaining_records: usize,
    bytes: usize,
    max_bytes: usize,
}

enum RecordAdmission {
    Room(ObservationRecord),
    Exact(ObservationRecord),
    NoRoom,
}

impl ObservationBudget {
    fn admit(&mut self, event: HubEvent) -> Result<RecordAdmission, ObservationFailure> {
        let sequence = event.sequence;
        let (value, encoded_bytes) = event_value_bounded(event)?;
        let Some(total_bytes) = self.bytes.checked_add(encoded_bytes) else {
            return Ok(RecordAdmission::NoRoom);
        };
        if self.remaining_records == 0 || total_bytes > self.max_bytes.min(MAX_EVENT_BYTES) {
            return Ok(RecordAdmission::NoRoom);
        }
        self.remaining_records -= 1;
        self.bytes = total_bytes;
        let record = ObservationRecord { sequence, value };
        Ok(
            if self.remaining_records == 0 || self.bytes == self.max_bytes {
                RecordAdmission::Exact(record)
            } else {
                RecordAdmission::Room(record)
            },
        )
    }
}

async fn attach(
    target: &SessionRef,
    operations: &ObservationOperations,
) -> Result<(SessionEventAttachment, CodexGeneration), ObservationFailure> {
    if target.endpoint.service_id != *operations.identity.service_id() {
        return Err(ObservationFailure::NotFound);
    }
    let endpoint = operations
        .directory
        .read_endpoint(&target.endpoint)
        .map_err(|_| ObservationFailure::Unavailable)?
        .ok_or(ObservationFailure::NotFound)?;
    if !matches!(
        endpoint.availability,
        EndpointAvailability::Available { .. }
    ) {
        return Err(ObservationFailure::Unavailable);
    }
    let generation = endpoint
        .channels
        .iter()
        .find_map(|channel| match channel {
            ChannelDescription::ExternalProvider {
                binding_generation, ..
            } => Some(CodexGeneration {
                service_epoch: operations.identity.service_epoch().clone(),
                generation: *binding_generation,
            }),
            _ => None,
        })
        .ok_or(ObservationFailure::NotFound)?;
    let session = serde_json::from_value(
        serde_json::to_value(target).map_err(|_| ObservationFailure::InvalidField)?,
    )
    .map_err(|_| ObservationFailure::InvalidField)?;
    let hub = operations
        .provider_session_hub
        .as_ref()
        .ok_or(ObservationFailure::Unavailable)?;
    let attachment = hub.attach(session).await.map_err(|error| match error {
        SessionEventHubError::SessionNotFound => ObservationFailure::NotFound,
        SessionEventHubError::Unavailable => ObservationFailure::Unavailable,
    })?;
    Ok((attachment, generation))
}

#[derive(serde::Serialize)]
struct SequencedEvent {
    sequence: u64,
    event: SessionEvent,
}

fn event_value(event: HubEvent) -> Result<Value, ObservationFailure> {
    serde_json::to_value(SequencedEvent {
        sequence: event.sequence,
        event: event.event,
    })
    .map_err(|_| ObservationFailure::Unavailable)
}

fn event_value_bounded(event: HubEvent) -> Result<(Value, usize), ObservationFailure> {
    let sequence = event.sequence;
    let item_id = match &event.event {
        SessionEvent::ItemStarted { item } | SessionEvent::ItemUpdated { item } => {
            Some(item.item_id.clone())
        }
        SessionEvent::ItemCompleted { item_id } => Some(item_id.clone()),
        _ => None,
    };
    let value = event_value(event)?;
    let encoded = serde_json::to_vec(&value).map_err(|_| ObservationFailure::Unavailable)?;
    if encoded.len() <= MAX_EVENT_BYTES {
        return Ok((value, encoded.len()));
    }
    let replacement = serde_json::to_value(ProviderObservationEventTooLarge {
        kind: ProviderObservationEventTooLargeKind::EventTooLarge,
        sequence,
        item_id,
    })
    .map_err(|_| ObservationFailure::Unavailable)?;
    let bytes = serde_json::to_vec(&replacement)
        .map_err(|_| ObservationFailure::Unavailable)?
        .len();
    Ok((replacement, bytes))
}

#[cfg(test)]
#[path = "provider_observation_operations_test.rs"]
mod tests;
