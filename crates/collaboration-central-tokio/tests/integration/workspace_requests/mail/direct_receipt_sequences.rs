use super::super::support::*;
use super::engine_probe_support::{integer, raw, runtime};
use central_storage::{Statement, StoreFailure};
use fleet_contracts::mail_link::MailReceiptSettlement;
use fleet_runtime::{EngineCommitFault, EngineCommitFaultKind, PausePoint};
use router_clock::RuntimeClock;
use workspace_model::central_mail::*;

const REJECT_OUTCOME: &str = "CREATE TRIGGER reject_direct_outcome BEFORE INSERT ON mail_messages WHEN NEW.message_kind='deliveryOutcome' BEGIN SELECT RAISE(ABORT, 'rejected direct effect'); END";

#[hegel::test(test_cases = 50, suppress_health_check = [hegel::HealthCheck::TooSlow])]
fn the_stored_message_and_its_outcome_follow_the_receipt_sequence(case: hegel::TestCase) {
    let order = case.draw(hegel::generators::integers::<u8>().max_value(11));
    let reject_mark = case.draw(hegel::generators::booleans());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(journey(order, reject_mark));
}

async fn journey(order: u8, reject_mark: bool) {
    let mut fixture = Fixture::new().await;
    let now = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
    let mut runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let message_id = queue_direct(&fixture, now, 981001).await;

    let mut first_outcome = if order & 1 == 0 {
        Some("deliveryNotConfirmed")
    } else {
        Some("deliveredSeenNotConfirmed")
    };
    let delivered = ReceiptKind::Delivered {
        arrived_at: now,
        waiting: Some(WaitingReason::Busy),
    };
    if order & 1 != 0 {
        let attachment = fixture.caller.connection().attachment();
        assert_eq!(
            runtime
                .apply_delivery_receipt(&attachment, &message_id, delivered.clone())
                .await
                .unwrap(),
            MailReceiptSettlement::Applied
        );
    }
    let (final_receipt, terminal, immediate_outcome, expected_status, contradiction) =
        match order / 4 {
            0 => (
                ReceiptKind::NotSeen {
                    waiting: WaitingReason::Busy,
                },
                "notSeen",
                Some("notSeen"),
                MessageStatus::NotSeen {
                    waiting: WaitingReason::Busy,
                },
                ReceiptKind::Seen { completed_at: now },
            ),
            1 => (
                ReceiptKind::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
                "refused",
                Some("refused"),
                MessageStatus::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
                ReceiptKind::Refused {
                    reason: MessageRefusal::SessionClosed,
                },
            ),
            2 => (
                ReceiptKind::Seen { completed_at: now },
                "seen",
                None,
                MessageStatus::Seen { completed_at: now },
                ReceiptKind::Seen {
                    completed_at: now + chrono::Duration::milliseconds(1),
                },
            ),
            _ => panic!("invalid generated receipt class"),
        };
    if order & 2 != 0 {
        let attachment = fixture.caller.connection().attachment();
        assert_eq!(
            runtime
                .apply_delivery_receipt(&attachment, &message_id, final_receipt.clone())
                .await
                .unwrap(),
            MailReceiptSettlement::Applied
        );
        first_outcome = immediate_outcome;
    }
    if order & 2 == 0 {
        fixture.set_utc(mark - chrono::Duration::minutes(1)).await;
        runtime.after_event().await.unwrap();
        let before = runtime
            .runtime()
            .statement_host()
            .read(Statement {
                sql: "SELECT state FROM mail_messages WHERE message_id=?",
                args: vec![text(&message_id)],
            })
            .await
            .unwrap();
        assert_eq!(
            before.rows,
            [vec![text(if order & 1 != 0 {
                "delivered"
            } else {
                "queued"
            })]]
        );
    }
    let _alarm_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(fleet_runtime::PausePoint::BeforeDriveGuard, "driver");
    let reject_due_write = reject_mark && order & 2 == 0;
    if reject_due_write {
        raw(&runtime, REJECT_OUTCOME, vec![]).await;
    }
    fixture.set_utc(mark).await;
    runtime.after_event().await.unwrap();
    if reject_due_write {
        assert_store(
            &runtime,
            &message_id,
            if order & 1 == 0 {
                "queued"
            } else {
                "delivered"
            },
            None,
        )
        .await;
        let attempts = runtime.runtime().statement_host()
            .read(Statement {
                sql: "SELECT state,failure_count,last_failure FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
                args: vec![text(&message_id)],
            })
            .await
            .unwrap();
        assert_eq!(
            attempts.rows,
            [vec![
                text("failing"),
                integer(1),
                text("statement_rejected")
            ]]
        );
        raw(&runtime, "DROP TRIGGER reject_direct_outcome", vec![]).await;
        let at = mark + chrono::Duration::minutes(5);
        fixture.set_utc(at).await;
        runtime.after_event().await.unwrap();
        let attempts = runtime.runtime().statement_host()
            .read(Statement {
                sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
                args: vec![text(&message_id)],
            })
            .await
            .unwrap();
        assert!(
            attempts.rows.is_empty(),
            "successful retry clears the attempt"
        );
    }
    assert_store(
        &runtime,
        &message_id,
        if order & 2 != 0 {
            terminal
        } else {
            "pastTheMark"
        },
        first_outcome,
    )
    .await;

    if order & 2 == 0 {
        fixture.set_utc(now).await;
        let caller = fixture.fresh_caller(&fixture.caller).await;
        let status = runtime
            .request(&caller, status_query(message_id.clone()))
            .await
            .unwrap()
            .result;
        let expected = if order & 1 == 0 {
            MessageStatus::Sent {
                deadline_passed: true,
            }
        } else {
            MessageStatus::Delivered {
                arrived_at: now,
                waiting: Some(WaitingReason::Busy),
                deadline_passed: true,
            }
        };
        assert_eq!(status, status_answer(expected));
        fixture.set_utc(mark).await;
    }
    fixture.reopen().await;
    runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let recipient = fixture.fresh_caller(&fixture.caller).await;
    let attachment = recipient.connection().attachment();
    if order & 3 == 0 {
        let saved_at = mark + chrono::Duration::milliseconds(137);
        let first_outcome_row = || Statement {
            sql: "SELECT message_id,body,created_at,updated_at FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=?",
            args: vec![text(&message_id)],
        };
        let before = runtime
            .runtime()
            .statement_host()
            .read(first_outcome_row())
            .await
            .unwrap();
        assert_eq!(before.rows.len(), 1);
        let late_delivery = ReceiptKind::Delivered {
            arrived_at: saved_at,
            waiting: Some(WaitingReason::WaitingOnAPerson),
        };
        assert_eq!(
            runtime
                .apply_delivery_receipt(&attachment, &message_id, late_delivery)
                .await
                .unwrap(),
            MailReceiptSettlement::Applied
        );
        let row = runtime.runtime().statement_host().read(Statement {
            sql: "SELECT state,delivered_arrived_at,delivered_waiting_reason FROM mail_messages WHERE message_id=?",
            args: vec![text(&message_id)],
        }).await.unwrap();
        assert_eq!(
            row.rows,
            [vec![
                text("pastTheMark"),
                text(saved_at.to_rfc3339()),
                text("waitingOnAPerson")
            ]]
        );
        assert_eq!(
            runtime
                .request(&recipient, status_query(message_id.clone()))
                .await
                .unwrap()
                .result,
            status_answer(MessageStatus::Delivered {
                arrived_at: saved_at,
                waiting: Some(WaitingReason::WaitingOnAPerson),
                deadline_passed: true,
            })
        );
        assert_eq!(
            runtime
                .runtime()
                .statement_host()
                .read(first_outcome_row())
                .await
                .unwrap(),
            before
        );
    }
    let first = runtime
        .apply_delivery_receipt(&attachment, &message_id, final_receipt.clone())
        .await
        .unwrap();
    let expected = if order & 2 != 0 {
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    } else {
        MailReceiptSettlement::Applied
    };
    assert_eq!(first, expected);
    assert_eq!(
        runtime
            .apply_delivery_receipt(&attachment, &message_id, final_receipt)
            .await
            .unwrap(),
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        runtime
            .apply_delivery_receipt(&attachment, &message_id, delivered)
            .await
            .unwrap(),
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        runtime
            .apply_delivery_receipt(&attachment, &message_id, contradiction)
            .await
            .unwrap(),
        MailReceiptSettlement::Stop(ReceiptResult::AlreadySettled)
    );
    assert_store(&runtime, &message_id, terminal, first_outcome).await;
    let actual = runtime
        .request(&recipient, status_query(message_id))
        .await
        .unwrap()
        .result;
    assert_eq!(actual, status_answer(expected_status));
}

pub(super) async fn queue_direct(
    fixture: &Fixture,
    now: workspace_model::Timestamp,
    request_number: u32,
) -> workspace_ids::MessageId {
    let queue = fixture
        .host
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(request_number),
                operation: CentralMailOperation::SendMessage {
                    to: session(&fixture.caller),
                    body: "receipt sequence".to_owned().try_into().unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                    sent_at: now,
                },
            },
        )
        .await
        .unwrap();
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = queue else {
        panic!("direct send refused: {queue:?}");
    };

    message_id
}

#[tokio::test]
async fn a_rejected_direct_mark_rolls_back_and_a_superseding_receipt_prevents_the_charge() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let now = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
    let message_id = queue_direct(&fixture, now, 981002).await;
    raw(&runtime, REJECT_OUTCOME, vec![]).await;
    let _alarm_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");

    let mut held = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, message_id.as_str());
    fixture.set_utc(mark).await;
    let batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    held.until_held().await;

    assert_store(&runtime, &message_id, "queued", None).await;
    let attempts = || async {
        runtime.runtime().statement_host().read(Statement {
            sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap().rows
    };
    assert!(attempts().await.is_empty());
    let recipient = fixture.fresh_caller(&fixture.caller).await;
    assert_eq!(
        runtime
            .apply_delivery_receipt(
                &recipient.connection().attachment(),
                &message_id,
                ReceiptKind::Seen { completed_at: now },
            )
            .await
            .unwrap(),
        MailReceiptSettlement::Applied
    );
    held.release();
    batch.await.unwrap().unwrap();

    assert_store(&runtime, &message_id, "seen", None).await;
    assert!(
        attempts().await.is_empty(),
        "the rolled-back version was superseded"
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn an_unobserved_direct_mark_commit_never_charges_an_attempt() {
    for (number, kind, expected_state, expected_outcome) in [
        (
            981003,
            EngineCommitFaultKind::NotAppliedAnswerLost,
            "queued",
            None,
        ),
        (
            981004,
            EngineCommitFaultKind::AppliedAnswerLost,
            "pastTheMark",
            Some("deliveryNotConfirmed"),
        ),
    ] {
        let fixture = Fixture::new().await;
        let runtime = runtime(&fixture).await;
        let now = fixture.clock.utc_now();
        let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
        let message_id = queue_direct(&fixture, now, number).await;
        let _alarm_driver = runtime
            .runtime()
            .pause_points()
            .hold_engine(PausePoint::BeforeDriveGuard, "driver");

        let fault = EngineCommitFault {
            key: collaboration_central_core::engine_probe::InstanceKey(message_id.to_string()),
            kind,
        };
        runtime
            .runtime()
            .statement_host()
            .inject_engine_commit_fault(fault);
        fixture.set_utc(mark).await;
        runtime.after_event().await.unwrap();

        assert!(
            !runtime
                .runtime()
                .statement_host()
                .engine_commit_fault_pending()
        );
        assert_store(&runtime, &message_id, expected_state, expected_outcome).await;
        let attempts = runtime.runtime().statement_host().read(Statement {
            sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap();
        assert!(
            attempts.rows.is_empty(),
            "an uncertain commit must never charge a due attempt"
        );
        fixture.host.close().await;
    }
}

#[tokio::test]
async fn a_seen_receipt_cannot_settle_a_notice_and_session_closed_keeps_its_reason() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let now = fixture.clock.utc_now();
    let notice_id: workspace_ids::MessageId = id(981005);
    let recipient = session(&fixture.caller);
    raw(&runtime,
        "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        vec![
            text(&notice_id),
            text(serde_json::to_string(&MessageSender::Central).unwrap()),
            text(&recipient.machine_id),
            text(serde_json::to_string(&recipient).unwrap()),
            text("notice"),
            text(serde_json::to_string(&MessageBody::Notice { items: vec![] }).unwrap()),
            text("queued"),
            text(now.to_rfc3339()),
            text(now.to_rfc3339()),
        ],
    ).await;
    let stored_state = || async {
        runtime
            .runtime()
            .statement_host()
            .read(Statement {
                sql: "SELECT state,failure_reason FROM mail_messages WHERE message_id=?",
                args: vec![text(&notice_id)],
            })
            .await
            .unwrap()
            .rows
    };
    let submit = async |message_id: workspace_ids::MessageId, receipt| {
        runtime
            .apply_delivery_receipt(
                &fixture.caller.connection().attachment(),
                &message_id,
                receipt,
            )
            .await
            .unwrap()
    };
    assert_eq!(
        submit(notice_id.clone(), ReceiptKind::Seen { completed_at: now }).await,
        MailReceiptSettlement::Stop(ReceiptResult::NotApplicable)
    );
    assert_eq!(
        stored_state().await,
        [vec![text("queued"), central_storage::SqlValue::Null]]
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Refused {
                reason: MessageRefusal::SessionClosed
            }
        )
        .await,
        MailReceiptSettlement::Applied
    );
    assert_eq!(
        stored_state().await,
        [vec![text("failed"), text("sessionClosed")]]
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Refused {
                reason: MessageRefusal::SessionClosed
            }
        )
        .await,
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Delivered {
                arrived_at: now,
                waiting: None
            }
        )
        .await,
        MailReceiptSettlement::Stop(ReceiptResult::AlreadySettled)
    );
    assert_eq!(
        stored_state().await,
        [vec![text("failed"), text("sessionClosed")]]
    );
    for (tag, reason) in [("failed", ""), ("cancelled", "sessionClosed")] {
        raw(
            &runtime,
            "UPDATE mail_messages SET state=?,failure_reason=? WHERE message_id=?",
            vec![text(tag), text(reason), text(&notice_id)],
        )
        .await;
        assert_eq!(
            submit(notice_id.clone(), ReceiptKind::Seen { completed_at: now }).await,
            MailReceiptSettlement::Stop(ReceiptResult::NotApplicable)
        );
        let receipt = ReceiptKind::Delivered {
            arrived_at: now,
            waiting: None,
        };
        let rejected = runtime
            .apply_delivery_receipt(
                &fixture.caller.connection().attachment(),
                &notice_id,
                receipt,
            )
            .await;
        assert!(
            matches!(rejected, Err(StoreFailure::Corrupt { .. })),
            "{rejected:?}"
        );
        assert_eq!(stored_state().await, [vec![text(tag), text(reason)]]);
    }
    fixture.host.close().await;
}

#[tokio::test]
async fn an_eligible_direct_mark_records_a_corrupt_typed_row_without_settling_it() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let now = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
    let message_id = queue_direct(&fixture, now, 981006).await;
    raw(
        &runtime,
        "UPDATE mail_messages SET sender=x'01' WHERE message_id=?",
        vec![text(&message_id)],
    )
    .await;
    let _alarm_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");

    fixture.set_utc(mark).await;
    runtime.after_event().await.unwrap();

    assert_store(&runtime, &message_id, "queued", None).await;
    let attempts = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT state,failure_count,last_failure,last_failure_detail FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
        args: vec![text(&message_id)],
    }).await.unwrap();
    assert!(
        matches!(
            attempts.rows.as_slice(),
            [attempt] if matches!(attempt.as_slice(),
                [state, count, kind, central_storage::SqlValue::Text(detail)]
                    if state == &text("failing") && count == &integer(1)
                        && kind == &text("corrupt_row") && detail.contains("sender")
            )
        ),
        "stored attempts: {:?}",
        attempts.rows
    );
    fixture.host.close().await;
}

async fn assert_store(
    runtime: &collaboration_central_core::WorkspaceRuntime<
        collaboration_central_tokio::ObjectStorage,
        collaboration_central_tokio::TokioWorkspaceStores,
    >,
    message_id: &workspace_ids::MessageId,
    state: &str,
    first_outcome: Option<&str>,
) {
    let rows = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT state,(SELECT json_extract(body,'$.outcome.kind') FROM mail_messages WHERE message_kind='deliveryOutcome') AS outcome,(SELECT COUNT(*) FROM mail_messages WHERE message_kind='deliveryOutcome') AS outcome_count FROM mail_messages WHERE message_id=?",
        args: vec![text(message_id)],
    }).await.unwrap();
    assert_eq!(
        rows.rows,
        [vec![
            text(state),
            first_outcome.map_or(central_storage::SqlValue::Null, text),
            central_storage::SqlValue::Integer(
                i64::from(first_outcome.is_some()).try_into().unwrap()
            )
        ]]
    );
}

fn status_query(message_id: workspace_ids::MessageId) -> workspace_model::WorkspaceRequest {
    use workspace_model as model;
    model::WorkspaceRequest {
        request_id: None,
        workspace_id: None,
        operation: model::WorkspaceOperation::List {
            query: model::ListQuery::MessageStatus { message_id },
            cursor: model::ListCursor::start(model::ListOrder::OldestFirst),
            limit: 1.try_into().unwrap(),
        },
    }
}

fn status_answer(status: MessageStatus) -> workspace_model::WorkspaceResult {
    workspace_model::WorkspaceResult::Done(workspace_model::WorkspaceAnswer::MessageStatus {
        status,
    })
}
