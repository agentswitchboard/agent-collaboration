//! Observation cuts and continuation admission through the public MCP tool.
use crate::served_api::ServedApi;
use collaboration_client::CollaborationClient;
use collaboration_contracts::{
    HubEvent, HubFuture, HubSessionSummary, SessionEventAttachment, SessionEventHub,
    SessionEventHubError, SkillViewAttachment,
};
use collaboration_protocol::{
    BoundedObservationRequest, BoundedObservationResult, ObservationCursor, ObservationEndReason,
    ObservationEventNotification,
};
use collaboration_service::ApplicationFixture;
use message_board::{SessionEndpointRef, SessionRef};
use router_clock::ManualClock;
use serde_json::{Value, json};
use session_event_model::{SessionEvent, SessionState};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc};

const SERVICE: &str = "00000000-0000-7000-8000-000000000101";
const FIRST_RUN: &str = "00000000-0000-7000-8000-000000000102";
const NEXT_RUN: &str = "00000000-0000-7000-8000-000000000103";
const EPOCH: u64 = 13;

struct CutHub {
    history: Vec<HubEvent>,
    latest_issued_sequence: u64,
    already_live: Vec<HubEvent>,
    live_capacity: usize,
    keep_open: bool,
    open_senders: std::sync::Mutex<Vec<broadcast::Sender<HubEvent>>>,
}

impl CutHub {
    fn new(history: Vec<HubEvent>, already_live: Vec<HubEvent>) -> Self {
        Self {
            latest_issued_sequence: history.last().map_or(0, |event| event.sequence),
            history,
            already_live,
            live_capacity: 8,
            keep_open: false,
            open_senders: Default::default(),
        }
    }
}

impl SessionEventHub for CutHub {
    fn attach(&self, _: SessionRef) -> HubFuture<'_, SessionEventAttachment> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(self.live_capacity);
            for event in &self.already_live {
                sender
                    .send(event.clone())
                    .map_err(|_| SessionEventHubError::Unavailable)?;
            }
            if self.keep_open {
                self.open_senders
                    .lock()
                    .map_err(|_| SessionEventHubError::Unavailable)?
                    .push(sender);
            }
            Ok(SessionEventAttachment {
                epoch: EPOCH,
                latest_issued_sequence: self.latest_issued_sequence,
                snapshot: self.history.clone(),
                receiver,
            })
        })
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

fn identity(
    run: &str,
    clock: Arc<ManualClock>,
    hub: CutHub,
) -> Result<ApplicationFixture, Box<dyn std::error::Error>> {
    Ok(ApplicationFixture::new(SERVICE, run)?
        .with_clock(clock)
        .with_provider_session_hub(Arc::new(hub))
        .with_endpoints(vec![serde_json::from_value(json!({
            "endpoint":{"serviceId":SERVICE,"endpointId":"claude-local"},
            "label":"Claude fixture",
            "availability":{"state":"available","observedAt":"2026-11-21T00:00:00Z"},
            "channels":[{"kind":"externalProvider","transport":"stdioAcp",
                "bindingId":"fixture-binding","bindingGeneration":7,
                "runtime":{"provider":"claudeCode","runtimeName":"fixture"},
                "capabilities":[{"name":"create","status":"supported","evidence":"advertised"}]}]
        }))?])?)
}

fn request(
    max_events: usize,
    max_bytes: usize,
) -> Result<BoundedObservationRequest, serde_json::Error> {
    serde_json::from_value(json!({
        "target":{"endpoint":{"serviceId":SERVICE,"endpointId":"claude-local"},"sessionId":"cut-fixture"},
        "timeoutSeconds":30,"maxEvents":max_events,"maxBytes":max_bytes
    }))
}

fn item(sequence: u64) -> HubEvent {
    HubEvent {
        sequence,
        event: SessionEvent::ItemCompleted {
            item_id: format!("item-{sequence}"),
        },
    }
}

fn numbered(event: &HubEvent) -> Value {
    json!({"sequence":event.sequence,"event":event.event})
}

fn marker(sequence: u64) -> Value {
    json!({"kind":"snapshotEnd","cursor":{"epoch":EPOCH,"afterSequence":sequence}})
}

fn marker_notification(sequence: u64) -> ObservationEventNotification {
    ObservationEventNotification {
        event: marker(sequence),
        cursor: Some(ObservationCursor {
            epoch: EPOCH,
            after_sequence: sequence,
        }),
    }
}

async fn read(
    client: &CollaborationClient,
    request: BoundedObservationRequest,
) -> Result<
    (BoundedObservationResult, Vec<ObservationEventNotification>),
    collaboration_client::ClientError,
> {
    let (sink, mut notifications) = mpsc::unbounded_channel();
    let result = client
        .observe_provider_session_streaming(request, Some(sink))
        .await?;
    let mut streamed = Vec::new();
    while let Ok(event) = notifications.try_recv() {
        streamed.push(event);
    }
    assert_eq!(
        streamed
            .iter()
            .map(|event| &event.event)
            .collect::<Vec<_>>(),
        result.events.iter().collect::<Vec<_>>()
    );
    Ok((result, streamed))
}

// Rejects a cut missing or misplaced between retained and already-queued live records.
#[tokio::test]
async fn the_public_tool_streams_the_cut_before_queued_live_while_the_call_is_open() {
    let clock = Arc::new(ManualClock::new(Default::default()));
    let mut hub = CutHub::new(vec![item(1), item(2)], vec![item(3)]);
    hub.keep_open = true;
    let api = ServedApi::start(identity(FIRST_RUN, clock.clone(), hub).unwrap()).unwrap();
    let client = api.client("snapshot-cut").await.unwrap();
    let (sink, mut notifications) = mpsc::unbounded_channel();
    let call = tokio::spawn(async move {
        client
            .observe_provider_session_streaming(request(10, 4096).unwrap(), Some(sink))
            .await
    });
    let first = notifications.recv().await.expect("first kept event");
    let last = notifications.recv().await.expect("last kept event");
    let cut = notifications
        .recv()
        .await
        .expect("snapshotEnd precedes live");
    assert_eq!(cut, marker_notification(2));
    let live = notifications.recv().await.expect("queued live event");
    assert!(!call.is_finished());

    clock.advance(Duration::from_secs(31)).unwrap();
    let result = call.await.unwrap().unwrap();
    assert_eq!(first.event, numbered(&item(1)));
    assert_eq!(last.event, numbered(&item(2)));
    assert_eq!(live.event, numbered(&item(3)));
    assert_eq!(
        result.events,
        vec![first.event, last.event, cut.event, live.event]
    );
    assert_eq!(result.end_reason, ObservationEndReason::DeadlineReached);
    api.stop().await.unwrap();
}

// Rejects counting the marker as a record or bytes, emitting it early, or losing it on an exact fit.
#[tokio::test]
async fn the_cut_matches_the_five_result_limit_cases() {
    let with_one = numbered(&item(1));
    let exact_bytes = serde_json::to_vec(&with_one).unwrap().len();
    let cases = [
        (
            vec![item(1)],
            vec![item(2)],
            1,
            4096,
            vec![with_one.clone(), marker(1)],
        ),
        (
            vec![item(1)],
            vec![item(2)],
            5,
            exact_bytes,
            vec![with_one.clone(), marker(1)],
        ),
        (
            vec![item(1), item(2)],
            vec![],
            1,
            4096,
            vec![with_one.clone()],
        ),
        (vec![item(1)], vec![], 1, exact_bytes - 1, vec![]),
        (
            vec![],
            vec![item(1)],
            1,
            exact_bytes,
            vec![marker(0), with_one],
        ),
    ];
    for (history, live, max_events, max_bytes, events) in cases {
        let clock = Arc::new(ManualClock::new(Default::default()));
        let api = ServedApi::start(identity(FIRST_RUN, clock, CutHub::new(history, live)).unwrap())
            .unwrap();
        let client = api.client("snapshot-limit").await.unwrap();
        let (result, _) = read(&client, request(max_events, max_bytes).unwrap())
            .await
            .unwrap();

        assert_eq!(
            result.events, events,
            "record limit {max_events}, byte limit {max_bytes}"
        );
        assert_eq!(result.end_reason, ObservationEndReason::ResultLimitReached);
        api.stop().await.unwrap();
    }
}

// Rejects dropping an empty cut or resetting a valid supplied position to zero.
#[tokio::test]
async fn an_empty_cut_uses_the_start_or_supplied_position() {
    for (history, supplied, expected) in [(vec![], None, 0), (vec![item(19)], Some(19), 19)] {
        let clock = Arc::new(ManualClock::new(Default::default()));
        let api =
            ServedApi::start(identity(FIRST_RUN, clock, CutHub::new(history, vec![])).unwrap())
                .unwrap();
        let client = api.client("empty-snapshot").await.unwrap();
        let mut input = request(1, 1).unwrap();
        input.epoch = supplied.map(|_| EPOCH);
        input.after_sequence = supplied;

        let (result, notifications) = read(&client, input).await.unwrap();

        assert_eq!(result.events, vec![marker(expected)]);
        assert_eq!(notifications, vec![marker_notification(expected)]);
        assert_eq!(result.end_reason, ObservationEndReason::BackendDisconnected);
        api.stop().await.unwrap();
    }
}

// Rejects a Host-run guard that accepts a previous run or refuses every guarded request.
#[tokio::test]
async fn continuation_refuses_the_old_host_without_refusing_the_current_or_legacy_call() {
    let clock = Arc::new(ManualClock::new(Default::default()));
    let api = ServedApi::start(
        identity(
            FIRST_RUN,
            clock,
            CutHub::new(vec![item(1), item(2)], vec![]),
        )
        .unwrap(),
    )
    .unwrap();
    let client = api.client("first-host").await.unwrap();
    let (first, _) = read(&client, request(2, 4096).unwrap()).await.unwrap();
    let old_identity = first.generation.service_epoch;
    assert_eq!(first.events.last(), Some(&marker(2)));
    api.stop().await.unwrap();

    let clock = Arc::new(ManualClock::new(Default::default()));
    let api = ServedApi::start(
        identity(
            NEXT_RUN,
            clock,
            CutHub::new(vec![item(1), item(2), item(3)], vec![]),
        )
        .unwrap(),
    )
    .unwrap();
    let client = api.client("second-host").await.unwrap();
    let (current, _) = read(&client, request(3, 4096).unwrap()).await.unwrap();
    let current_identity = current.generation.service_epoch;
    let mut input = request(1, 4096).unwrap();
    input.after_sequence = Some(2);
    input.epoch = first.epoch;

    input.service_epoch = Some(old_identity);
    let (refused, notifications) = read(&client, input.clone()).await.unwrap();
    assert_eq!(refused.events, vec![json!({"kind":"resyncRequired"})]);
    assert_eq!(refused.end_reason, ObservationEndReason::ResyncRequired);
    assert_eq!(
        notifications,
        vec![ObservationEventNotification::resync_marker(
            json!({"kind":"resyncRequired"})
        )]
    );

    for guard in [Some(current_identity), None] {
        input.service_epoch = guard;
        let (continued, _) = read(&client, input.clone()).await.unwrap();
        assert_eq!(continued.events, vec![numbered(&item(3)), marker(3)]);
        assert_eq!(
            continued.end_reason,
            ObservationEndReason::ResultLimitReached
        );
    }
    api.stop().await.unwrap();
}

// Rejects validating the supplied sequence against an event issued after the attachment.
#[tokio::test]
async fn a_position_ahead_of_the_attachment_is_refused_even_when_it_is_waiting_live() {
    let clock = Arc::new(ManualClock::new(Default::default()));
    let api = ServedApi::start(
        identity(FIRST_RUN, clock, CutHub::new(vec![item(1)], vec![item(2)])).unwrap(),
    )
    .unwrap();
    let client = api.client("future-position").await.unwrap();
    let mut input = request(10, 1).unwrap();
    input.after_sequence = Some(2);
    input.epoch = Some(EPOCH);

    let (refused, notifications) = read(&client, input).await.unwrap();

    assert_eq!(refused.events, vec![json!({"kind":"resyncRequired"})]);
    assert_eq!(refused.end_reason, ObservationEndReason::ResyncRequired);
    assert_eq!(notifications[0].cursor, None);
    api.stop().await.unwrap();
}

// Rejects suppressing a gap inside history, suppressing a lag gap at one byte, or allowing a cut after a gap.
#[tokio::test]
async fn captured_restart_and_live_lag_end_the_call_with_an_unbudgeted_gap() {
    let restart = HubEvent {
        sequence: 2,
        event: SessionEvent::ResyncRequired {
            replay_epoch: EPOCH + 1,
        },
    };
    let mut lag = CutHub::new(vec![], vec![item(1), item(2)]);
    lag.live_capacity = 1;
    for (fixture, limit, events) in [
        (
            CutHub::new(vec![item(1), restart.clone(), item(3)], vec![item(4)]),
            4096,
            vec![numbered(&item(1)), numbered(&restart)],
        ),
        (lag, 1, vec![marker(0), json!({"kind":"resyncRequired"})]),
    ] {
        let clock = Arc::new(ManualClock::new(Default::default()));
        let api = ServedApi::start(identity(FIRST_RUN, clock, fixture).unwrap()).unwrap();
        let client = api.client("snapshot-gap").await.unwrap();

        let (result, notifications) = read(&client, request(10, limit).unwrap()).await.unwrap();

        assert_eq!(result.events, events);
        assert_eq!(result.end_reason, ObservationEndReason::ResyncRequired);
        assert_eq!(notifications.last().expect("gap").cursor, None);
        api.stop().await.unwrap();
    }
}
