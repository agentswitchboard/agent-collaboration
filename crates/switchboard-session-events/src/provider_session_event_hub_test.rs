use std::sync::Arc;

use message_board::SessionRef;
use session_event_model::{SessionEvent, SessionItem, SessionItemKind};
use tokio::sync::Mutex;

use super::ProviderSessionEventHub;
use collaboration_contracts::SessionEventHub;
use switchboard_session_store::ProviderOperationStore;

#[tokio::test]
async fn publish_uses_injected_clock_for_inventory_activity() {
    let root = tempfile::tempdir().expect("root");
    let mut store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let session: SessionRef = serde_json::from_value(serde_json::json!({
        "endpoint": {
            "serviceId": "019abcde-f001-7a23-8b45-6789abcdef24",
            "endpointId": "claude-local"
        },
        "sessionId": "clock-observation"
    }))
    .expect("session");
    let owner: collaboration_protocol::ProviderIdentity =
        serde_json::from_value(serde_json::json!({"humanId": "owner"})).expect("owner");
    store
        .record_session(&switchboard_session_store::ProviderSessionRecord {
            target: serde_json::from_value(serde_json::to_value(&session).expect("target"))
                .expect("target"),
            working_directory: root.path().display().to_string().try_into().expect("cwd"),
            requested_policy: collaboration_protocol::ProviderRequestedPolicy {
                access: collaboration_protocol::RouterAccess::WriteRestricted,
            },
            created_by: owner.clone(),
            approver: owner,
            updated_at_ms: 10,
            resume: Default::default(),
        })
        .await
        .expect("record session");
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let hub = ProviderSessionEventHub::new(Arc::new(Mutex::new(store)), clock.clone());

    let observed_at_seconds = 1_700_012_345;
    clock.set_utc(chrono::DateTime::from_timestamp(observed_at_seconds, 0).expect("fixture time"));
    hub.publish(session.clone(), started_item("clock observation"))
        .await
        .expect("publish activity");
    let sessions = hub
        .sessions(session.endpoint.clone())
        .await
        .expect("sessions");

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session, session);
    assert_eq!(sessions[0].updated_at_seconds, observed_at_seconds);
}

#[tokio::test]
async fn inventory_uses_first_user_message_and_existing_session_title() {
    // Arrange: durable inventory plus the same retained events used by attachments.
    let root = tempfile::tempdir().expect("root");
    let mut store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let session: SessionRef = serde_json::from_value(serde_json::json!({
        "endpoint":{"serviceId":uuid::Uuid::now_v7(),"endpointId":"claude-local"},
        "sessionId":uuid::Uuid::now_v7()
    }))
    .expect("session");
    let owner: collaboration_protocol::ProviderIdentity =
        serde_json::from_value(serde_json::json!({"humanId":"owner"})).expect("owner");
    store
        .record_session(&switchboard_session_store::ProviderSessionRecord {
            target: serde_json::from_value(serde_json::to_value(&session).expect("target"))
                .expect("target"),
            working_directory: root.path().display().to_string().try_into().expect("cwd"),
            requested_policy: collaboration_protocol::ProviderRequestedPolicy {
                access: collaboration_protocol::RouterAccess::WriteRestricted,
            },
            created_by: owner.clone(),
            approver: owner,
            updated_at_ms: 1,
            resume: Default::default(),
        })
        .await
        .expect("inventory");
    let hub = ProviderSessionEventHub::new(
        Arc::new(Mutex::new(store)),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
    );
    let first_id = uuid::Uuid::now_v7().to_string();
    for (item_id, kind, text) in [
        (
            uuid::Uuid::now_v7().to_string(),
            SessionItemKind::AgentMessage,
            "agent greeting",
        ),
        (first_id.clone(), SessionItemKind::UserMessage, "first "),
        (
            uuid::Uuid::now_v7().to_string(),
            SessionItemKind::UserMessage,
            "later message",
        ),
    ] {
        hub.publish(
            session.clone(),
            SessionEvent::ItemStarted {
                item: SessionItem {
                    item_id,
                    kind,
                    text: Some(text.into()),
                },
            },
        )
        .await
        .expect("item");
    }
    // Act: the first message's final text arrives after another item.
    hub.publish(
        session.clone(),
        SessionEvent::ItemUpdated {
            item: SessionItem {
                item_id: first_id,
                kind: SessionItemKind::UserMessage,
                text: Some("first message".into()),
            },
        },
    )
    .await
    .expect("final first text");
    let summary = hub
        .sessions(session.endpoint.clone())
        .await
        .expect("sessions")
        .remove(0);
    // Assert: order is original user-item order, not lexical ID or last-update order.
    assert_eq!(summary.preview, "first message");
    assert_eq!(summary.name, None, "a preview never invents a title");

    // Act / Assert: only a title the provider actually reported becomes the name.
    hub.publish(
        session.clone(),
        SessionEvent::ItemStarted {
            item: SessionItem {
                item_id: uuid::Uuid::now_v7().to_string(),
                kind: SessionItemKind::SessionInfo,
                text: Some(r#"{"title":"Existing provider title"}"#.into()),
            },
        },
    )
    .await
    .expect("session info");
    let summary = hub
        .sessions(session.endpoint.clone())
        .await
        .expect("sessions")
        .remove(0);
    assert_eq!(summary.name.as_deref(), Some("Existing provider title"));

    // Inactivity clears replay items, but must not erase inventory labels.
    hub.publish(
        session.clone(),
        SessionEvent::StateChanged {
            state: session_event_model::SessionState::Unloaded,
        },
    )
    .await
    .expect("unload");
    let summary = hub
        .sessions(session.endpoint)
        .await
        .expect("sessions")
        .remove(0);
    assert_eq!(summary.preview, "first message");
    assert_eq!(summary.name.as_deref(), Some("Existing provider title"));
}

// Rejects a reset that omits the old subscriber's gap or carries an old frontier into a new epoch.
#[tokio::test]
async fn closed_and_unloaded_sessions_admit_fresh_history_replay() {
    // Arrange: both states are inactive and retain the provider identity for loading.
    for state in [
        session_event_model::SessionState::Closed,
        session_event_model::SessionState::Unloaded,
    ] {
        let root = tempfile::tempdir().expect("root");
        let store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
            .await
            .expect("store");
        let hub = ProviderSessionEventHub::new(
            Arc::new(Mutex::new(store)),
            Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        );
        let session: SessionRef = serde_json::from_value(serde_json::json!({"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"cursor-local"},"sessionId":"restorable"})).expect("session");
        hub.publish(
            session.clone(),
            SessionEvent::StateChanged {
                state: state.clone(),
            },
        )
        .await
        .expect("inactive state");
        let mut before = hub.attach(session.clone()).await.expect("attach");
        // Act: the hub itself owns replay admission and epoch reset.
        let epoch = hub
            .begin_history_replay(session.clone())
            .await
            .expect("inactive Session must admit replay");
        // Assert: existing reset semantics remain; old subscribers must resynchronize.
        assert!(epoch > before.epoch);
        assert!(
            matches!(before.receiver.recv().await.expect("resync"), collaboration_contracts::HubEvent { event: SessionEvent::ResyncRequired { replay_epoch }, .. } if replay_epoch == epoch)
        );
        let after = hub.attach(session.clone()).await.expect("new attach");
        assert_eq!(after.epoch, epoch);
        assert!(after.snapshot.is_empty());
        assert_eq!(after.latest_issued_sequence, 0);
        assert_eq!(
            hub.state(session).await.expect("state"),
            session_event_model::SessionState::Unloaded
        );
    }
}

// Rejects an attach that captures the issued sequence on the wrong side of a concurrent publication.
#[tokio::test]
async fn concurrent_publication_lands_on_exactly_one_side_of_the_attachment() {
    let root = tempfile::tempdir().expect("temp root");
    let store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let hub = ProviderSessionEventHub::new(
        Arc::new(Mutex::new(store)),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
    );

    for publication_first in [true, false] {
        let session: SessionRef = serde_json::from_value(serde_json::json!({
            "endpoint":{"serviceId":"00000000-0000-7000-8000-000000000001","endpointId":"claude-local"},
            "sessionId": if publication_first { "publish-first" } else { "attach-first" }
        }))
        .expect("session");
        let first = hub
            .publish(session.clone(), started_item("first"))
            .await
            .expect("seed history");
        let held = hub.hold_history_for_test(session.clone()).await;
        let mut publish = std::pin::pin!(hub.publish(session.clone(), started_item("second")));
        let mut attach = std::pin::pin!(hub.attach(session));
        // Both operations contend on the held history lock. Tokio's FIFO mutex gives the
        // first polled operation the lock first after we release it.
        if publication_first {
            assert!(futures_util::poll!(&mut publish).is_pending());
            assert!(futures_util::poll!(&mut attach).is_pending());
        } else {
            assert!(futures_util::poll!(&mut attach).is_pending());
            assert!(futures_util::poll!(&mut publish).is_pending());
        }
        drop(held);

        let (published, attached) = tokio::join!(publish, attach);
        let published = published.expect("publish second item");
        let mut attached = attached.expect("attach history");

        if publication_first {
            assert_eq!(attached.latest_issued_sequence, published.sequence);
            assert_eq!(attached.snapshot, vec![first, published]);
        } else {
            assert_eq!(attached.latest_issued_sequence, first.sequence);
            assert_eq!(attached.snapshot, vec![first]);
            assert!(published.sequence > attached.latest_issued_sequence);
            assert_eq!(attached.receiver.try_recv().expect("live item"), published);
        }
        assert!(matches!(
            attached.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
}

fn started_item(item_id: &str) -> SessionEvent {
    SessionEvent::ItemStarted {
        item: SessionItem {
            item_id: item_id.into(),
            kind: SessionItemKind::UserMessage,
            text: Some(item_id.into()),
        },
    }
}

#[tokio::test]
async fn rejected_active_history_replay_preserves_current_skill_view_and_subscription() {
    use session_event_model::{
        ApprovalChoice, ApprovalEffect, ApprovalRequest, ApprovalScope, OfferedOption,
        OfferedOptionId, OfferedOptions, OptionsOrigin, PendingInteraction, PendingInteractions,
        SessionState,
    };
    let request = ApprovalRequest {
        request_id: "pending".into(),
        title: "pending approval".into(),
        description: None,
        subject: None,
        options_origin: OptionsOrigin::AgentOffered,
        options: OfferedOptions::new(vec![OfferedOption {
            option_id: OfferedOptionId::new("allow").expect("option"),
            label: "Allow".into(),
            choice: ApprovalChoice::new(ApprovalEffect::Allow, ApprovalScope::Once),
        }])
        .expect("options"),
    };
    let pending = PendingInteractions::new(vec![PendingInteraction::Approval {
        approver: message_board::Identity::Human {
            human_id: message_board::HumanId::try_from("approver".to_owned()).expect("human"),
        },
        request: Box::new(request),
    }])
    .expect("pending");
    for state in [
        SessionState::Idle,
        SessionState::Running,
        SessionState::AuthenticationRequired,
        SessionState::RequiresAction { pending },
    ] {
        assert_active_replay_preserves_state(state).await;
    }
}

async fn assert_active_replay_preserves_state(state: session_event_model::SessionState) {
    let root = tempfile::tempdir().expect("root");
    let store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let hub = ProviderSessionEventHub::new(
        Arc::new(Mutex::new(store)),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
    );
    let session: SessionRef = serde_json::from_value(serde_json::json!({"endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},"sessionId":"active"})).expect("session");
    hub.publish(
        session.clone(),
        SessionEvent::StateChanged {
            state: state.clone(),
        },
    )
    .await
    .expect("active state");
    let view = session_event_model::SkillView::Available {
        skills: vec![session_event_model::ProviderSkill {
            invocation_name: session_event_model::SkillInvocationName::new("review").expect("name"),
            description: "Review".into(),
            scope: session_event_model::ProviderSkillScope::System,
        }],
        errors: vec![],
    };
    hub.publish_skill_view(session.clone(), view.clone())
        .await
        .expect("source view");
    let history = hub
        .histories
        .lock()
        .await
        .get(&session)
        .cloned()
        .expect("history");
    let receiver = history.lock().await.skill_view.subscribe();
    let mut before = hub
        .attach(session.clone())
        .await
        .expect("attached current history");
    assert!(matches!(
        hub.begin_history_replay(session.clone()).await,
        Err(collaboration_contracts::SessionEventHubError::Unavailable)
    ));
    assert_eq!(*receiver.borrow(), view);
    assert!(!receiver.has_changed().expect("subscription remains open"));
    let after = hub
        .attach(session.clone())
        .await
        .expect("history unchanged");
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(after.snapshot, before.snapshot);
    assert_eq!(hub.state(session).await.expect("state unchanged"), state);
    assert!(matches!(
        before.receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn locked_attach_on_one_session_does_not_stall_another_publish() {
    let root = tempfile::tempdir().expect("temp root");
    let store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .expect("store");
    let hub = Arc::new(ProviderSessionEventHub::new(
        Arc::new(Mutex::new(store)),
        Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
    ));
    let first: SessionRef = serde_json::from_value(serde_json::json!({
        "endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},
        "sessionId":"first"
    }))
    .expect("first session");
    let second: SessionRef = serde_json::from_value(serde_json::json!({
        "endpoint":{"serviceId":"00000000-0000-4000-8000-000000000001","endpointId":"claude-local"},
        "sessionId":"second"
    }))
    .expect("second session");
    for target in [&first, &second] {
        hub.publish(
            target.clone(),
            SessionEvent::ItemStarted {
                item: SessionItem {
                    item_id: "one".into(),
                    kind: SessionItemKind::UserMessage,
                    text: Some("hello".into()),
                },
            },
        )
        .await
        .expect("seed item");
    }
    let held = hub.hold_history_for_test(first).await;
    let published = {
        let mut publication = std::pin::pin!(tokio::task::unconstrained(
            hub.publish(second, started_item("two"))
        ));
        // B's history is seeded and uncontended, so it can complete in one
        // poll even while A's history lock is held.
        futures_util::poll!(&mut publication)
    };
    drop(held);

    let std::task::Poll::Ready(result) = published else {
        panic!("publishing to the second session waited on the first session's history");
    };
    assert_eq!(result.expect("publish to the second session").sequence, 2);
}
