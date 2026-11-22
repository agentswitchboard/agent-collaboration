use super::{ObservationFailure, ObservationOperations};
use crate::ApplicationFixture;
use collaboration_contracts::{
    HubEvent, HubFuture, HubSessionSummary, SessionEventAttachment, SessionEventHub,
    SessionEventHubError, SkillViewAttachment,
};
use collaboration_protocol::{BoundedObservationRequest, ObservationEndReason};
use message_board::{SessionEndpointRef, SessionRef};
use router_clock::{ClockInstant, ManualClock};
use serde_json::json;
use session_event_model::SessionState;
use std::{future::pending, path::PathBuf, pin::pin, sync::Arc, task::Poll, time::Duration};
use tokio::sync::broadcast;

const SERVICE_ID: &str = "00000000-0000-7000-8000-000000000001";
const SERVICE_EPOCH: &str = "00000000-0000-7000-8000-000000000002";
const BOUND: Duration = Duration::from_secs(30);

enum AttachFixture {
    Blocked,
    Quiet(broadcast::Sender<HubEvent>),
}

impl SessionEventHub for AttachFixture {
    fn attach(&self, _: SessionRef) -> HubFuture<'_, SessionEventAttachment> {
        match self {
            Self::Blocked => Box::pin(pending()),
            Self::Quiet(sender) => Box::pin(async {
                Ok(SessionEventAttachment {
                    snapshot: vec![],
                    receiver: sender.subscribe(),
                    epoch: 0,
                    latest_issued_sequence: 0,
                })
            }),
        }
    }

    fn skill_view(&self, _: SessionRef, _: PathBuf) -> HubFuture<'_, SkillViewAttachment> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }

    fn state(&self, _: SessionRef) -> HubFuture<'_, SessionState> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }

    fn sessions(&self, _: SessionEndpointRef) -> HubFuture<'_, Vec<HubSessionSummary>> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }
}

fn observation_fixture(
    clock: Arc<ManualClock>,
    hub: AttachFixture,
) -> (crate::CollaborationApplication, BoundedObservationRequest) {
    let endpoint = json!({"serviceId": SERVICE_ID, "endpointId": "claude-local"});
    let request: BoundedObservationRequest = serde_json::from_value(json!({
        "target": {"endpoint": endpoint, "sessionId": "fixture-session"},
        "timeoutSeconds": BOUND.as_secs(),
        "maxEvents": 2,
        "maxBytes": 4096
    }))
    .unwrap();
    let application = ApplicationFixture::new(SERVICE_ID, SERVICE_EPOCH)
        .unwrap()
        .with_clock(clock)
        .with_provider_session_hub(Arc::new(hub))
        .with_endpoints(vec![
            serde_json::from_value(json!({
                "endpoint": endpoint,
                "label": "Claude fixture",
                "availability": {"state": "available", "observedAt": "2026-09-26T00:00:00Z"},
                "channels": [{"kind": "externalProvider", "transport": "stdioAcp",
                    "bindingId": "fixture-binding", "bindingGeneration": 7,
                    "runtime": {"provider": "claudeCode", "runtimeName": "fixture"},
                "capabilities": [{"name": "create", "status": "supported", "evidence": "advertised"}]}]
            }))
            .unwrap(),
        ])
        .unwrap()
        .build();
    (application, request)
}

async fn advance_past_bound_while_held(
    clock: &ManualClock,
    operation: &ObservationOperations,
    request: BoundedObservationRequest,
) -> Result<collaboration_protocol::BoundedObservationResult, ObservationFailure> {
    let deadline = ClockInstant::ZERO + BOUND;
    let mut call = pin!(operation.session_observe(request, None));
    assert!(futures_util::poll!(&mut call).is_pending());
    assert!(clock.has_not_due_check_now(deadline));
    clock.advance(BOUND - Duration::from_secs(1)).unwrap();
    assert!(futures_util::poll!(&mut call).is_pending());
    clock.advance(Duration::from_secs(2)).unwrap();

    match futures_util::poll!(&mut call) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("observation outlived its injected deadline"),
    }
}

// Rejects an attach wait that uses Tokio's clock or fails before the injected deadline.
#[tokio::test]
async fn attach_wait_uses_the_injected_deadline() {
    let clock = Arc::new(ManualClock::new(Default::default()));
    let (application, request) = observation_fixture(clock.clone(), AttachFixture::Blocked);

    let result = advance_past_bound_while_held(&clock, application.observation(), request).await;

    assert_eq!(result.unwrap_err(), ObservationFailure::Unavailable);
}

// Rejects a live wait that uses Tokio's clock or finishes before the injected deadline.
#[tokio::test]
async fn live_wait_uses_the_injected_deadline() {
    let clock = Arc::new(ManualClock::new(Default::default()));
    let (sender, _) = broadcast::channel(1);
    let (application, request) = observation_fixture(clock.clone(), AttachFixture::Quiet(sender));

    let result = advance_past_bound_while_held(&clock, application.observation(), request)
        .await
        .unwrap();

    assert_eq!(result.end_reason, ObservationEndReason::DeadlineReached);
}
