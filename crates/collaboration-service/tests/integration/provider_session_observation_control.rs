//! Provider observers read one ordered Session hub through bounded observation on the API.

use std::sync::Arc;

use crate::{provider_history::ResumePublicationFixture, served_api};
use collaboration_client::CollaborationClient;
use collaboration_protocol::{
    BoundedObservationRequest, EndpointDescription, ObservationEndReason, ProviderRequestedPolicy,
    ProviderWorkingDirectory, RouterAccess, SessionRef,
};
use collaboration_runtime::ProviderOperationStore;
use collaboration_runtime::ProviderSessionEventHub;
use collaboration_runtime::ProviderSessionRecord;
use collaboration_service::ApplicationFixture;
use serde_json::{Value, json};
use session_event_model::{SessionEvent, SessionItem, SessionItemKind};
use tokio::sync::Mutex;

/// A Router whose provider endpoint's Session hub has one retained event.
struct ProviderFixture {
    _root: tempfile::TempDir,
    target: SessionRef,
    board_target: message_board::SessionRef,
    hub: Arc<ProviderSessionEventHub>,
    identity: ApplicationFixture,
}

async fn provider_fixture() -> Result<ProviderFixture, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let service_id = "00000000-0000-4000-8000-000000000001";
    let epoch = "00000000-0000-4000-8000-000000000002";
    let target: SessionRef = serde_json::from_value(json!({
        "endpoint":{"serviceId":service_id,"endpointId":"claude-local"},
        "sessionId":"provider-session"
    }))?;
    let owner: SessionRef = serde_json::from_value(json!({
        "endpoint":{"serviceId":service_id,"endpointId":"codex-local"},
        "sessionId":"owner"
    }))?;
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&root.path().join("operations.sqlite")).await?,
    ));
    store
        .lock()
        .await
        .record_session(&ProviderSessionRecord {
            target: target.clone(),
            working_directory: ProviderWorkingDirectory::try_from(
                root.path().display().to_string(),
            )?,
            requested_policy: ProviderRequestedPolicy {
                access: RouterAccess::WriteRestricted,
            },
            created_by: owner.clone().into(),
            approver: owner.into(),
            updated_at_ms: 1,
            resume: Default::default(),
        })
        .await?;
    let hub = Arc::new(ProviderSessionEventHub::with_capacity(store, 2));
    let board_target: message_board::SessionRef =
        serde_json::from_value(serde_json::to_value(&target)?)?;
    hub.publish(board_target.clone(), item("item-1", "first"))
        .await?;
    let description: EndpointDescription = serde_json::from_value(json!({
        "endpoint":target.endpoint,"label":"Claude fixture",
        "availability":{"state":"available","observedAt":"2026-09-26T00:00:00Z"},
        "channels":[{"kind":"externalProvider","transport":"stdioAcp",
            "bindingId":"fixture-binding","bindingGeneration":7,
            "runtime":{"provider":"claudeCode","runtimeName":"fixture"},
            "capabilities":[{"name":"create","status":"supported","evidence":"advertised"}]}]
    }))?;
    let identity =
        ApplicationFixture::new(service_id, epoch)?.with_provider_session_hub(hub.clone());
    identity.endpoint_directory().publish(description)?;
    Ok(ProviderFixture {
        _root: root,
        target,
        board_target,
        hub,
        identity,
    })
}

#[tokio::test]
async fn the_client_hands_on_each_streamed_event_before_the_call_returns() {
    // Arrange: one retained event; the call is bounded at two events and thirty seconds.
    let fixture = provider_fixture().await.expect("provider fixture");
    let served = served_api::ServedApi::start(fixture.identity).expect("serve the API");
    let client = served
        .client("streaming-observer")
        .await
        .expect("observing client");
    let (observed, mut streamed) = tokio::sync::mpsc::unbounded_channel();
    let request = BoundedObservationRequest {
        target: fixture.target.clone(),
        timeout_seconds: 30,
        max_events: 2,
        max_bytes: 262_144,
        after_sequence: None,
        epoch: None,
        service_epoch: None,
    };

    // Act
    let call = tokio::spawn(async move {
        client
            .observe_provider_session_streaming(request, Some(observed))
            .await
    });
    let retained = streamed.recv().await.expect("streamed event");
    let snapshot_end = streamed.recv().await.expect("snapshot end streamed");
    let still_open = !call.is_finished();
    fixture
        .hub
        .publish(fixture.board_target.clone(), item("item-2", "second"))
        .await
        .expect("live item");
    let result = call
        .await
        .expect("call joins")
        .expect("bounded observation");
    let live = streamed.recv().await.expect("live event streamed");

    // Assert
    assert!(
        still_open,
        "the call ended before its first event was handed on"
    );
    assert_eq!(retained.event["sequence"], 1);
    let epoch = result.epoch.expect("provider epoch");
    assert_eq!(
        retained.cursor,
        Some(collaboration_protocol::ObservationCursor {
            epoch,
            after_sequence: 1
        })
    );
    assert_eq!(live.event["sequence"], 2);
    assert_eq!(snapshot_end.event, snapshot_marker(epoch, 1));
    assert_eq!(snapshot_end.cursor, retained.cursor);
    assert_eq!(
        result.events,
        vec![retained.event, snapshot_end.event, live.event]
    );
    assert_eq!(result.end_reason, ObservationEndReason::ResultLimitReached);
    served.stop().await.expect("API stops");
}

#[tokio::test]
async fn a_stale_epoch_streams_its_resync_marker_without_a_cursor() {
    // Arrange: the hub's current epoch, then a call that names an older one.
    let fixture = provider_fixture().await.expect("provider fixture");
    let served = served_api::ServedApi::start(fixture.identity).expect("serve the API");
    let client = served
        .client("stale-observer")
        .await
        .expect("observing client");
    let (_, epoch) = single_event(&client, &fixture.target, None, None)
        .await
        .expect("current epoch");
    let (observed, mut streamed) = tokio::sync::mpsc::unbounded_channel();

    // Act
    let result = client
        .observe_provider_session_streaming(
            BoundedObservationRequest {
                target: fixture.target.clone(),
                timeout_seconds: 1,
                max_events: 1,
                max_bytes: 262_144,
                after_sequence: Some(1),
                epoch: Some(epoch.expect("provider epoch") + 1),
                service_epoch: None,
            },
            Some(observed),
        )
        .await
        .expect("stale epoch response");

    // Assert: the marker was streamed like every other event, with no cursor to continue
    // from; the result is unchanged.
    let marker = streamed.try_recv().expect("the resync marker was streamed");
    assert_eq!(
        marker,
        collaboration_protocol::ObservationEventNotification {
            event: json!({"kind":"resyncRequired"}),
            cursor: None,
        }
    );
    assert_eq!(result.events, vec![json!({"kind":"resyncRequired"})]);
    assert_eq!(result.end_reason, ObservationEndReason::ResyncRequired);
    served.stop().await.expect("API stops");
}

#[tokio::test]
async fn a_stale_epoch_answers_its_resync_marker_even_when_the_budget_cannot_hold_it() {
    // Arrange: a call naming an older epoch, with the smallest valid event and byte budget.
    let fixture = provider_fixture().await.expect("provider fixture");
    let served = served_api::ServedApi::start(fixture.identity).expect("serve the API");
    let client = served
        .client("small-budget-observer")
        .await
        .expect("observing client");
    let (_, epoch) = single_event(&client, &fixture.target, None, None)
        .await
        .expect("current epoch");
    let (observed, mut streamed) = tokio::sync::mpsc::unbounded_channel();

    // Act
    let result = client
        .observe_provider_session_streaming(
            BoundedObservationRequest {
                target: fixture.target.clone(),
                timeout_seconds: 1,
                max_events: 1,
                max_bytes: 1,
                after_sequence: Some(1),
                epoch: Some(epoch.expect("provider epoch") + 1),
                service_epoch: None,
            },
            Some(observed),
        )
        .await
        .expect("stale epoch response");

    // Assert: the marker is the answer itself, so the budget does not apply to it.
    assert_eq!(result.events, vec![json!({"kind":"resyncRequired"})]);
    assert_eq!(result.end_reason, ObservationEndReason::ResyncRequired);
    let marker = streamed.try_recv().expect("the resync marker was streamed");
    assert_eq!(
        marker,
        collaboration_protocol::ObservationEventNotification {
            event: json!({"kind":"resyncRequired"}),
            cursor: None,
        }
    );
    assert!(
        streamed.try_recv().is_err(),
        "one notification carries the marker"
    );
    served.stop().await.expect("API stops");
}

#[tokio::test]
async fn a_reset_during_an_open_call_streams_its_resync_marker_without_a_cursor() {
    // Arrange: an open call that has streamed the retained event.
    let fixture = provider_fixture().await.expect("provider fixture");
    let resume = ResumePublicationFixture::start(&fixture.hub, &fixture.board_target)
        .await
        .expect("fixture owner publisher");
    let served = served_api::ServedApi::start(fixture.identity).expect("serve the API");
    let client = served
        .client("reset-observer")
        .await
        .expect("observing client");
    let (observed, mut streamed) = tokio::sync::mpsc::unbounded_channel();
    let request = BoundedObservationRequest {
        target: fixture.target.clone(),
        timeout_seconds: 30,
        max_events: 10,
        max_bytes: 262_144,
        after_sequence: None,
        epoch: None,
        service_epoch: None,
    };
    let call = tokio::spawn(async move {
        client
            .observe_provider_session_streaming(request, Some(observed))
            .await
    });
    let retained = streamed.recv().await.expect("the retained event streamed");
    let baseline = streamed.recv().await.expect("the owner baseline streamed");
    assert_eq!(baseline.event["sequence"], 2);
    let snapshot_end = streamed.recv().await.expect("snapshot end streamed");
    let baseline_cursor = baseline.cursor.expect("retained position");
    assert_eq!(
        snapshot_end.event,
        snapshot_marker(baseline_cursor.epoch, 2)
    );
    assert_eq!(snapshot_end.cursor, Some(baseline_cursor));

    // Act: the Session's history is invalidated while the call is open.
    resume.resume(None).await.expect("history reset");
    let result = call
        .await
        .expect("call joins")
        .expect("bounded observation");
    let marker = streamed.recv().await.expect("resync marker streamed");

    // Assert: the hub numbered the marker, but nothing continues after it, so it has no
    // cursor; the result still ends with it and reports the resync.
    assert_eq!(retained.event["sequence"], 1);
    assert_eq!(
        marker.event["event"]["kind"], "resyncRequired",
        "{marker:?}"
    );
    assert_eq!(marker.cursor, None, "{marker:?}");
    assert_eq!(result.events.last(), Some(&marker.event));
    assert_eq!(result.end_reason, ObservationEndReason::ResyncRequired);
    resume.stop().await.expect("publisher stops");
    served.stop().await.expect("API stops");
}

#[tokio::test]
async fn provider_observers_read_snapshot_and_live_order() {
    let ProviderFixture {
        _root,
        target,
        board_target,
        hub,
        identity,
    } = provider_fixture().await.expect("provider fixture");

    let served = served_api::ServedApi::start(identity).expect("serve the API");
    let bounded = served
        .client("provider-observer")
        .await
        .expect("bounded client");
    let (first_snapshot, epoch) = single_event(&bounded, &target, None, None)
        .await
        .expect("first snapshot");
    assert_eq!(first_snapshot["sequence"], 1);

    hub.publish(board_target, item("item-2", "second"))
        .await
        .expect("live item");
    let (first_live, _) = single_event(&bounded, &target, Some(1), epoch)
        .await
        .expect("first live event");
    assert_eq!(first_live["sequence"], 2);

    let observed = bounded
        .observe_provider_session(BoundedObservationRequest {
            target,
            timeout_seconds: 1,
            max_events: 2,
            max_bytes: 262_144,
            after_sequence: None,
            epoch: None,
            service_epoch: None,
        })
        .await
        .expect("bounded observation");
    let marker = snapshot_marker(observed.epoch.expect("provider epoch"), 2);
    assert_eq!(
        observed.events,
        vec![first_snapshot, first_live.clone(), marker.clone()]
    );
    assert_eq!(
        observed.end_reason,
        ObservationEndReason::ResultLimitReached
    );
    assert!(observed.continuation_gap);
    let resumed = bounded
        .observe_provider_session(BoundedObservationRequest {
            target: observed.target.clone(),
            timeout_seconds: 1,
            max_events: 1,
            max_bytes: 262_144,
            after_sequence: Some(1),
            epoch: observed.epoch,
            service_epoch: None,
        })
        .await
        .expect("paged observation");
    assert_eq!(resumed.events, vec![first_live.clone(), marker]);
    assert_eq!(resumed.epoch, observed.epoch);
    let stale = bounded
        .observe_provider_session(BoundedObservationRequest {
            target: observed.target.clone(),
            timeout_seconds: 1,
            max_events: 1,
            max_bytes: 262_144,
            after_sequence: Some(1),
            epoch: Some(observed.epoch.expect("provider epoch") + 1),
            service_epoch: None,
        })
        .await
        .expect("stale epoch response");
    assert_eq!(stale.end_reason, ObservationEndReason::ResyncRequired);
    assert_eq!(stale.events, vec![json!({"kind":"resyncRequired"})]);
    for index in 3..=5 {
        hub.publish(
            serde_json::from_value(serde_json::to_value(&observed.target).expect("target JSON"))
                .expect("board target"),
            item(&format!("item-{index}"), "overflow subscriber buffer"),
        )
        .await
        .expect("overflow event");
    }
    let large_text = format!("{}{}", "a".repeat(900_000), "\"".repeat(50_000));
    hub.publish(
        serde_json::from_value(serde_json::to_value(&observed.target).expect("target JSON"))
            .expect("board target"),
        item("oversized", &large_text),
    )
    .await
    .expect("large item");
    let oversized_page = bounded
        .observe_provider_session(BoundedObservationRequest {
            target: observed.target.clone(),
            timeout_seconds: 1,
            max_events: 10,
            max_bytes: 1_048_576,
            after_sequence: None,
            epoch: None,
            service_epoch: None,
        })
        .await
        .expect("oversized observation");
    assert!(oversized_page.events.iter().any(|event| {
        event == &json!({"kind":"eventTooLarge","sequence":6,"itemId":"oversized"})
    }));
    let replay = bounded
        .observe_provider_session(BoundedObservationRequest {
            target: observed.target.clone(),
            timeout_seconds: 1,
            max_events: 10,
            max_bytes: 1_048_576,
            after_sequence: Some(5),
            epoch: observed.epoch,
            service_epoch: None,
        })
        .await
        .expect("replay after the large item");
    assert!(replay.events.iter().any(|event| {
        event == &json!({"kind":"eventTooLarge","sequence":6,"itemId":"oversized"})
    }));
    hub.publish(
        serde_json::from_value(serde_json::to_value(&observed.target).expect("target JSON"))
            .expect("board target"),
        item("after-large", "still delivered"),
    )
    .await
    .expect("later item");
    assert_eq!(
        single_event(&bounded, &observed.target, Some(6), observed.epoch)
            .await
            .expect("later delivery")
            .0["sequence"],
        7
    );
    drop(bounded);
    served.stop().await.expect("API stops");
}

fn item(item_id: &str, text: &str) -> SessionEvent {
    SessionEvent::ItemStarted {
        item: SessionItem {
            item_id: item_id.into(),
            kind: SessionItemKind::AgentMessage,
            text: Some(text.into()),
        },
    }
}

/// Metadata stays available through the existing public observation surface,
/// independently of whether a transcript facade chooses to display an Item.
#[tokio::test]
async fn metadata_payloads_remain_exact_in_retained_and_live_public_observation() {
    // Arrange: usage already retained, and a catalogue that will arrive live.
    let fixture = provider_fixture().await.expect("provider fixture");
    let usage = SessionEvent::ItemStarted {
        item: SessionItem {
            item_id: "usage-metadata".into(),
            kind: SessionItemKind::Usage,
            text: Some(r#"{"used":12,"size":100,"cost":{"amount":0.25,"currency":"USD"}}"#.into()),
        },
    };
    let commands = SessionEvent::ItemStarted {
        item: SessionItem {
            item_id: "command-metadata".into(),
            kind: SessionItemKind::Notice,
            text: Some(
                r#"{"availableCommands":[{"name":"compact","description":"Compact context"}]}"#
                    .into(),
            ),
        },
    };
    fixture
        .hub
        .publish(fixture.board_target.clone(), usage.clone())
        .await
        .expect("retain usage");
    let served = served_api::ServedApi::start(fixture.identity).expect("served API");
    let client = served.client("metadata-observer").await.expect("client");
    let request = BoundedObservationRequest {
        target: fixture.target.clone(),
        timeout_seconds: 5,
        max_events: 2,
        max_bytes: 262_144,
        after_sequence: Some(1),
        epoch: Some(0),
        service_epoch: None,
    };
    let (observed, mut streamed) = tokio::sync::mpsc::unbounded_channel();

    // Act: await the retained event as the actual attachment signal, then publish live.
    let call = tokio::spawn(async move {
        client
            .observe_provider_session_streaming(request, Some(observed))
            .await
    });
    let retained = streamed.recv().await.expect("retained metadata");
    let snapshot_end = streamed.recv().await.expect("snapshot end streamed");
    assert!(
        !call.is_finished(),
        "the observer must still be attached for live metadata"
    );
    fixture
        .hub
        .publish(fixture.board_target, commands.clone())
        .await
        .expect("publish commands");
    let result = call
        .await
        .expect("observation joins")
        .expect("observation succeeds");

    // Assert: exact original kinds and JSON bodies, with resumable positions.
    assert_eq!(retained.event, json!({"sequence":2,"event":usage}));
    assert_eq!(
        result.events,
        vec![
            json!({"sequence":2,"event":usage}),
            snapshot_marker(0, 2),
            json!({"sequence":3,"event":commands})
        ]
    );
    assert_eq!(result.epoch, Some(0));
    assert_eq!(snapshot_end.event, snapshot_marker(0, 2));
    assert_eq!(result.end_reason, ObservationEndReason::ResultLimitReached);
    assert_eq!(
        streamed.recv().await.expect("live metadata").cursor,
        Some(collaboration_protocol::ObservationCursor {
            epoch: 0,
            after_sequence: 3
        })
    );
    served.stop().await.expect("API stops");
}

/// The next event after `after_sequence` in `epoch`, waiting up to two seconds for it, with the
/// hub's epoch.
async fn single_event(
    client: &CollaborationClient,
    target: &SessionRef,
    after_sequence: Option<u64>,
    epoch: Option<u64>,
) -> Result<(Value, Option<u64>), Box<dyn std::error::Error>> {
    let observed = client
        .observe_provider_session(BoundedObservationRequest {
            target: target.clone(),
            timeout_seconds: 2,
            max_events: 1,
            max_bytes: 262_144,
            after_sequence,
            epoch,
            service_epoch: None,
        })
        .await?;
    let event = observed
        .events
        .into_iter()
        .find(|event| event["kind"] != "snapshotEnd")
        .ok_or("no event arrived")?;
    Ok((event, observed.epoch))
}

fn snapshot_marker(epoch: u64, sequence: u64) -> Value {
    json!({"kind":"snapshotEnd","cursor":{"epoch":epoch,"afterSequence":sequence}})
}

// Rejects repeats, skipped retained records, and treating compacted intermediate updates as a gap.
#[tokio::test]
async fn pages_follow_the_retained_projection_through_compacted_updates() {
    let fixture = provider_fixture().await.expect("provider fixture");
    for (id, text) in [("updated", "start"), ("other", "between pages")] {
        fixture
            .hub
            .publish(fixture.board_target.clone(), item(id, text))
            .await
            .unwrap();
    }
    let served = served_api::ServedApi::start(fixture.identity).unwrap();
    let client = served.client("projection-pages").await.unwrap();
    let mut request = BoundedObservationRequest {
        target: fixture.target,
        timeout_seconds: 30,
        max_events: 1,
        max_bytes: 262_144,
        after_sequence: None,
        epoch: None,
        service_epoch: None,
    };
    let (first, first_trace) = trace_page(&client, request.clone()).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0]["sequence"], 1);
    request.epoch = first.epoch;
    request.service_epoch = Some(first.generation.service_epoch);
    request.after_sequence = Some(1);
    for text in ["intermediate", "latest"] {
        fixture
            .hub
            .publish(
                fixture.board_target.clone(),
                SessionEvent::ItemUpdated {
                    item: SessionItem {
                        item_id: "updated".into(),
                        kind: SessionItemKind::AgentMessage,
                        text: Some(text.into()),
                    },
                },
            )
            .await
            .unwrap();
    }

    let (second, second_trace) = trace_page(&client, request.clone()).await.unwrap();
    request.after_sequence = Some(3);
    let (third, third_trace) = trace_page(&client, request.clone()).await.unwrap();
    fixture
        .hub
        .publish(fixture.board_target, item("later", "after the cut"))
        .await
        .unwrap();
    request.after_sequence = Some(5);
    let (fourth, fourth_trace) = trace_page(&client, request).await.unwrap();

    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0]["sequence"], 3);
    assert_eq!(third.events.len(), 2);
    assert_eq!(third.events[0]["sequence"], 5);
    assert_eq!(third.events[0]["event"]["item"]["text"], "latest");
    assert_eq!(third.events[1], snapshot_marker(0, 5));
    assert_eq!(fourth.events.len(), 2);
    assert_eq!(fourth.events[0]["sequence"], 6);
    assert_eq!(fourth.events[1], snapshot_marker(0, 6));
    for page in [second, third, fourth] {
        assert_eq!(page.end_reason, ObservationEndReason::ResultLimitReached);
    }
    save_trace(
        "continued-observation.json",
        &[first_trace, second_trace, third_trace, fourth_trace],
        "One end-of-snapshot at the first cut. Forward sequence 6 as live and consume the later cut.",
    ).unwrap();
    served.stop().await.unwrap();
}

// Rejects a truncated event losing its continuation sequence or preventing the cut.
#[tokio::test]
async fn a_kept_oversized_replacement_precedes_the_cut_and_advances_it() {
    let fixture = provider_fixture().await.expect("provider fixture");
    fixture
        .hub
        .publish(
            fixture.board_target.clone(),
            item("big", &"x".repeat(1_000_000)),
        )
        .await
        .unwrap();
    let served = served_api::ServedApi::start(fixture.identity).unwrap();
    let client = served.client("oversize-cut").await.unwrap();
    let mut request = BoundedObservationRequest {
        target: fixture.target,
        timeout_seconds: 30,
        max_events: 1,
        max_bytes: 4096,
        after_sequence: Some(1),
        epoch: Some(0),
        service_epoch: None,
    };
    let (page, page_trace) = trace_page(&client, request.clone()).await.unwrap();
    assert_eq!(
        page.events,
        vec![
            json!({"kind":"eventTooLarge","itemId":"big","sequence":2}),
            snapshot_marker(0, 2)
        ]
    );
    assert_eq!(
        page_trace["notifications"][0]["cursor"],
        snapshot_marker(0, 2)["cursor"]
    );
    assert_eq!(
        page_trace["notifications"][1]["cursor"],
        snapshot_marker(0, 2)["cursor"]
    );
    fixture
        .hub
        .publish(fixture.board_target, item("later", "continues"))
        .await
        .unwrap();
    request.after_sequence = Some(2);
    request.service_epoch = Some(page.generation.service_epoch);
    let (next, next_trace) = trace_page(&client, request).await.unwrap();
    assert_eq!(next.events[0]["sequence"], 3);
    assert_eq!(next.events[1], snapshot_marker(0, 3));
    save_trace(
        "oversized-observation.json",
        &[page_trace, next_trace],
        "One gap when the oversized record arrives. The attachment emits no end-of-snapshot.",
    )
    .unwrap();
    served.stop().await.unwrap();
}

async fn trace_page(
    client: &CollaborationClient,
    request: BoundedObservationRequest,
) -> Result<
    (collaboration_protocol::BoundedObservationResult, Value),
    collaboration_client::ClientError,
> {
    let (sink, mut streamed) = tokio::sync::mpsc::unbounded_channel();
    let result = client
        .observe_provider_session_streaming(request.clone(), Some(sink))
        .await?;
    let mut notifications = Vec::new();
    while let Ok(notification) = streamed.try_recv() {
        notifications.push(notification);
    }
    assert_eq!(
        notifications.iter().map(|n| &n.event).collect::<Vec<_>>(),
        result.events.iter().collect::<Vec<_>>(),
    );
    let trace = json!({"request":request,"notifications":notifications,"result":result});
    Ok((result, trace))
}

fn save_trace(
    name: &str,
    calls: &[Value],
    expected_attachment: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(directory) = std::env::var_os("PROVIDER_OBSERVATION_TRACE_DIRECTORY") {
        let path = std::path::PathBuf::from(directory).join(name);
        let trace = json!({"calls":calls,"expectedAttachment":expected_attachment});
        std::fs::write(path, serde_json::to_vec_pretty(&trace)?)?;
    }
    Ok(())
}
