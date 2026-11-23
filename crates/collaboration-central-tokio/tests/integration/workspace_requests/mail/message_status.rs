use super::super::{m, support::*};
use super::direct_receipt_sequences::queue_direct;
use super::engine_probe_support::{Runtime, integer, runtime as central_for_fixture};
use central_storage::{
    AlarmUpdate, BatchOutcome, CorruptReason, GuardedStatement, RowExpectation, Statement,
    StatementBatch, StoreFailure,
};
use fleet_contracts::mail_link::{MailReceiptSettlement, WindowedDeliveryDecision};
use router_clock::RuntimeClock;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
    MessageDelivery, MessageStatus, ReceiptKind,
};

#[tokio::test]
async fn only_the_original_sender_can_read_direct_message_status() {
    let fixture = Fixture::new().await;
    let runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let send = runtime
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(1550),
                operation: CentralMailOperation::SendMessage {
                    to: session(&fixture.caller),
                    body: "Read this message's status".to_owned().try_into().unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                    sent_at: fixture.clock.utc_now(),
                },
            },
        )
        .await
        .unwrap();
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = send else {
        panic!("send did not queue: {send:?}");
    };
    let other_session = workspace_ids::SessionRef {
        session_id: "status-other-session".to_owned().try_into().unwrap(),
        ..session(&fixture.caller)
    };
    let other_caller = collaboration_central_core::WorkspaceCaller::new(
        fixture.caller.connection().clone(),
        m::Caller::Session(other_session),
    )
    .unwrap();
    let registered = runtime
        .request_central(
            &other_caller,
            CentralMailRequest {
                request_id: id(1551),
                operation: CentralMailOperation::SetNoticeCadence {
                    cadence: m::NoticeCadence::M15,
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        registered,
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })
    ));

    let status_request = |message_id| m::WorkspaceRequest {
        request_id: None,
        workspace_id: None,
        operation: m::WorkspaceOperation::List {
            query: m::ListQuery::MessageStatus { message_id },
            cursor: m::ListCursor::start(m::ListOrder::OldestFirst),
            limit: 1.try_into().unwrap(),
        },
    };
    let expected = |status| m::WorkspaceResult::Done(m::WorkspaceAnswer::MessageStatus { status });
    let query = status_request(message_id.clone());
    assert_eq!(
        runtime
            .request(&fixture.caller, query.clone())
            .await
            .unwrap()
            .result,
        expected(MessageStatus::Sent {
            deadline_passed: false
        })
    );
    let other = runtime.request(&other_caller, query.clone()).await.unwrap();
    let unknown = runtime
        .request(&fixture.caller, status_request(id(1552)))
        .await
        .unwrap();
    assert_eq!(other, unknown);
    assert_eq!(other.result, expected(MessageStatus::NotFound {}));

    assert_eq!(
        runtime
            .apply_delivery_receipt(
                &fixture.caller.connection().attachment(),
                &message_id,
                ReceiptKind::Delivered {
                    arrived_at: fixture.clock.utc_now(),
                }
            )
            .await
            .unwrap(),
        MailReceiptSettlement::Applied
    );
    assert_eq!(
        runtime
            .request(&fixture.caller, query)
            .await
            .unwrap()
            .result,
        expected(MessageStatus::Delivered {
            arrived_at: fixture.clock.utc_now(),
            deadline_passed: false
        })
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn unresolved_direct_sender_is_a_store_failure_without_a_state_change_or_outcome() {
    let fixture = Fixture::new().await;
    let runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let sender = session(&fixture.caller);
    let response = runtime
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(1553),
                operation: CentralMailOperation::SendMessage {
                    to: sender.clone(),
                    body: "Unresolvable sender fixture".to_owned().try_into().unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                    sent_at: fixture.clock.utc_now(),
                },
            },
        )
        .await
        .unwrap();
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = response else {
        panic!("send did not queue: {response:?}");
    };
    let actors = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT identity FROM access_actor_identities WHERE identity_kind='agentSession'",
            args: vec![],
        })
        .await
        .unwrap();
    let [actor] = actors.rows.as_slice() else {
        panic!("expected the fixture sender's actor")
    };
    let [central_storage::SqlValue::Text(original_identity)] = actor.as_slice() else {
        panic!("expected a stored identity");
    };
    let component_identity = m::Identity::RouterComponent {
        machine_id: sender.machine_id,
        component: name("Fixture component"),
    };
    let component_json = serde_json::to_string(&component_identity).unwrap();
    let row_identity = |prior_kind: &str, prior: &str, next_kind: &str, next: &str| {
        runtime.runtime().statement_host().run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "UPDATE access_actor_identities SET identity_kind=?,identity=? WHERE identity_kind=? AND identity=?",
                    args: vec![text(next_kind), text(next), text(prior_kind), text(prior)],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
    };
    assert!(matches!(
        row_identity(
            "agentSession",
            original_identity,
            "routerComponent",
            &component_json
        )
        .await
        .unwrap(),
        BatchOutcome::Applied { .. }
    ));
    let attachment = fixture.caller.connection().attachment();
    let alarm_before = runtime
        .runtime()
        .statement_host()
        .published_alarm()
        .unwrap();
    assert!(matches!(
        runtime
            .prepare_windowed_delivery(&attachment, &message_id)
            .await,
        Err(central_storage::StoreFailure::Corrupt { .. })
    ));
    assert_eq!(
        runtime
            .runtime()
            .statement_host()
            .published_alarm()
            .unwrap(),
        alarm_before
    );
    assert!(matches!(
        row_identity(
            "routerComponent",
            &component_json,
            "agentSession",
            original_identity
        )
        .await
        .unwrap(),
        BatchOutcome::Applied { .. }
    ));
    let reply = runtime
        .request(
            &fixture.caller,
            m::WorkspaceRequest {
                request_id: None,
                workspace_id: None,
                operation: m::WorkspaceOperation::List {
                    query: m::ListQuery::MessageStatus { message_id },
                    cursor: m::ListCursor::start(m::ListOrder::OldestFirst),
                    limit: 1.try_into().unwrap(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        reply.result,
        m::WorkspaceResult::Done(m::WorkspaceAnswer::MessageStatus {
            status: MessageStatus::Sent {
                deadline_passed: false
            },
        })
    );
    assert!(
        runtime
            .runtime()
            .statement_host()
            .read(Statement {
                sql: "SELECT message_id FROM mail_messages WHERE message_kind='deliveryOutcome'",
                args: vec![],
            })
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    fixture.host.close().await;
}

async fn queue_for_sender_read() -> (Fixture, Runtime, workspace_ids::MessageId) {
    let fixture = Fixture::new().await;
    let central = central_for_fixture(&fixture).await;
    let message = queue_direct(&fixture, fixture.clock.utc_now(), 1554).await;
    (fixture, central, message)
}

async fn replace_sender_identity(
    central: &Runtime,
    message: &workspace_ids::MessageId,
    identity_kind: &str,
    identity_json: &str,
) {
    let outcome = central
        .runtime()
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "UPDATE access_actor_identities SET identity_kind=?,identity=? WHERE actor_identity_id=(SELECT json_extract(sender,'$.value') FROM mail_messages WHERE message_id=?)",
                    args: vec![text(identity_kind), text(identity_json), text(message)],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(outcome, BatchOutcome::Applied { .. }));
}

#[tokio::test]
async fn an_owning_direct_offer_rejects_a_missing_sender_actor() {
    let (fixture, central, message) = queue_for_sender_read().await;
    let outcome = central
        .runtime()
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "UPDATE mail_messages SET sender=? WHERE message_id=?",
                    args: vec![
                        text(r#"{"kind":"actor","value":"01890f2e-7b4c-7cc0-98c4-0000000000c1"}"#),
                        text(&message),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(outcome, BatchOutcome::Applied { .. }));

    let offer = central
        .prepare_windowed_delivery(&fixture.caller.connection().attachment(), &message)
        .await;

    assert!(matches!(
        offer,
        Err(StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } })
            if column == "mail_messages.sender"
    ));
    fixture.host.close().await;
}

#[tokio::test]
async fn an_owning_direct_offer_rejects_a_sender_identity_kind_that_disagrees_with_json() {
    let (fixture, central, message) = queue_for_sender_read().await;
    let session_json = serde_json::to_string(&m::Identity::AgentSession {
        session: session(&fixture.caller),
    })
    .unwrap();
    replace_sender_identity(&central, &message, "person", &session_json).await;

    let offer = central
        .prepare_windowed_delivery(&fixture.caller.connection().attachment(), &message)
        .await;

    assert!(matches!(
        offer,
        Err(StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } })
            if column == "access_actor_identities.identity_kind"
    ));
    fixture.host.close().await;
}

#[tokio::test]
async fn an_owning_direct_offer_resolves_a_person_without_a_session_outcome() {
    let (fixture, central, message) = queue_for_sender_read().await;
    let person_id: workspace_ids::PersonId =
        "01890f2e-7b4c-7cc0-98c4-0000000000d1".parse().unwrap();
    let person_json = serde_json::to_string(&m::Identity::Person {
        person_id: person_id.clone(),
    })
    .unwrap();
    replace_sender_identity(&central, &message, "person", &person_json).await;

    let offer = central
        .prepare_windowed_delivery(&fixture.caller.connection().attachment(), &message)
        .await
        .unwrap();
    let WindowedDeliveryDecision::Ready(delivery) = offer else {
        panic!("a valid person sender must resolve to an offer");
    };
    assert_eq!(
        delivery.sender,
        m::ResolvedSender::Person {
            person_id,
            display_name: None,
        }
    );

    let accepted_at = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(accepted_at, accepted_at);
    let _alarm_driver = central
        .runtime()
        .pause_points()
        .hold_engine(fleet_runtime::PausePoint::BeforeDriveGuard, "driver");
    fixture.set_utc(mark).await;
    central.after_event().await.unwrap();

    let stored = central.runtime().statement_host().read(Statement {
        sql: "SELECT state,deadline_settled_at IS NOT NULL,(SELECT COUNT(*) FROM mail_messages WHERE message_kind='deliveryOutcome') FROM mail_messages WHERE message_id=?",
        args: vec![text(&message)],
    }).await.unwrap();
    assert_eq!(stored.rows, [vec![text("queued"), integer(1), integer(0)]]);
    fixture.host.close().await;
}
