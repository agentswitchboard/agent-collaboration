use super::super::{support::*, workerd_host::RemoteRuntime};
use super::engine_probe_support::{PauseControl, integer, raw, runtime};
use central_storage::{SqlValue, Statement, StoreFailure};
use fleet_contracts::{MachineConnectionAttachment, mail_link::MailReceiptSettlement};
use fleet_runtime::{EngineCommitFault, EngineCommitFaultKind, PausePoint};
use std::sync::atomic::Ordering;
use workspace_ids::MessageId;
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
    let fixture = Fixture::new().await;
    let sent_at = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let mark = fleet_mail::message_deadline::cancel_due_at(sent_at, sent_at);
    let runtime = fixture.host.central(&fixture.caller.organization_id).await;
    let message_id = queue_direct(&fixture, sent_at, 981001).await;

    let mut outcome = if order & 1 == 0 {
        Some("deliveryNotConfirmed")
    } else {
        Some("deliveredSeenNotConfirmed")
    };
    let delivered = ReceiptKind::Delivered {
        arrived_at: sent_at,
        waiting: Some(WaitingReason::Busy),
    };
    if order & 1 != 0 {
        assert_eq!(
            receipt(&fixture, &runtime, &message_id, delivered.clone()).await,
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
                ReceiptKind::Seen {
                    completed_at: sent_at,
                },
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
                ReceiptKind::Seen {
                    completed_at: sent_at,
                },
                "seen",
                None,
                MessageStatus::Seen {
                    completed_at: sent_at,
                },
                ReceiptKind::Seen {
                    completed_at: sent_at + chrono::Duration::milliseconds(1),
                },
            ),
            _ => panic!("invalid generated receipt class"),
        };
    if order & 2 != 0 {
        assert_eq!(
            receipt(&fixture, &runtime, &message_id, final_receipt.clone()).await,
            MailReceiptSettlement::Applied
        );
        outcome = immediate_outcome;
    }
    if order & 2 == 0 {
        fixture.clock.0.store(
            (mark - chrono::Duration::minutes(1)).timestamp_millis(),
            Ordering::SeqCst,
        );
        runtime.after_event().await.unwrap();
        let before = runtime
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
    let reject_due_write = reject_mark && order & 2 == 0;
    if reject_due_write {
        raw(&runtime, REJECT_OUTCOME, vec![]).await;
    }
    fixture
        .clock
        .0
        .store(mark.timestamp_millis(), Ordering::SeqCst);
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
        let attempts = runtime
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
        fixture
            .clock
            .0
            .store(at.timestamp_millis(), Ordering::SeqCst);
        runtime.after_event().await.unwrap();
        let attempts = runtime
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
        outcome,
    )
    .await;
    if order & 2 == 0 {
        fixture
            .clock
            .0
            .store(sent_at.timestamp_millis(), Ordering::SeqCst);
        let caller = fresh_auth(&fixture.host, &fixture.caller);
        let status = fixture
            .host
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
                arrived_at: sent_at,
                waiting: Some(WaitingReason::Busy),
                deadline_passed: true,
            }
        };
        assert_eq!(status, status_answer(expected));
        fixture
            .clock
            .0
            .store(mark.timestamp_millis(), Ordering::SeqCst);
    }
    runtime.cold_alarm().await.unwrap();

    let first = receipt(&fixture, &runtime, &message_id, final_receipt.clone()).await;
    let expected = if order & 2 != 0 {
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    } else {
        MailReceiptSettlement::Applied
    };
    assert_eq!(first, expected);
    assert_eq!(
        receipt(&fixture, &runtime, &message_id, final_receipt).await,
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        receipt(&fixture, &runtime, &message_id, delivered).await,
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        receipt(&fixture, &runtime, &message_id, contradiction).await,
        MailReceiptSettlement::Stop(ReceiptResult::AlreadySettled)
    );
    assert_store(&runtime, &message_id, terminal, outcome).await;
    let caller = fresh_auth(&fixture.host, &fixture.caller);
    let actual = fixture
        .host
        .request(&caller, status_query(message_id))
        .await
        .unwrap()
        .result;
    assert_eq!(actual, status_answer(expected_status));
    fixture.host.close().await;
}

async fn receipt(
    fixture: &Fixture,
    runtime: &RemoteRuntime,
    message: &MessageId,
    receipt: ReceiptKind,
) -> MailReceiptSettlement {
    submit_receipt(fixture, runtime, message, receipt)
        .await
        .unwrap()
}

async fn submit_receipt(
    fixture: &Fixture,
    central: &RemoteRuntime,
    message: &MessageId,
    receipt: ReceiptKind,
) -> Result<MailReceiptSettlement, StoreFailure> {
    let target = session(&fixture.caller);
    let authority = MachineConnectionAttachment::from_token_claims(
        fixture.caller.organization_id.clone(),
        target.machine_id,
        fixture.caller.enrollment_id.clone().try_into().unwrap(),
        fixture.caller.enrollment_id.clone().try_into().unwrap(),
        fleet_identity_protocol::UnixSeconds(fixture.clock.0.load(Ordering::SeqCst) / 1000 + 600),
    );
    central
        .control("direct-receipt-fixture", &(authority, message, receipt))
        .await
        .unwrap()
}

pub(super) async fn queue_direct(
    fixture: &Fixture,
    sent_at: workspace_model::Timestamp,
    request_number: u32,
) -> MessageId {
    let destination = session(&fixture.caller);
    let queue = fixture
        .host
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(request_number),
                operation: CentralMailOperation::SendMessage {
                    to: destination.clone(),
                    body: "receipt sequence".to_owned().try_into().unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                    sent_at,
                },
            },
        )
        .await
        .unwrap();
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = queue else {
        panic!("send did not queue: {queue:?}");
    };

    message_id
}

#[tokio::test]
async fn a_rejected_direct_mark_rolls_back_and_a_superseding_receipt_prevents_the_charge() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
    let message_id = queue_direct(&fixture, now, 981002).await;
    raw(&runtime, REJECT_OUTCOME, vec![]).await;

    let mut held = runtime
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, message_id.as_str())
        .await;
    fixture
        .clock
        .0
        .store(mark.timestamp_millis(), Ordering::SeqCst);
    let batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    held.until_held().await;

    assert_store(&runtime, &message_id, "queued", None).await;
    let attempts = || async {
        runtime.read(Statement {
            sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap().rows
    };
    assert!(attempts().await.is_empty());
    assert_eq!(
        receipt(
            &fixture,
            &runtime,
            &message_id,
            ReceiptKind::Seen { completed_at: now }
        )
        .await,
        MailReceiptSettlement::Applied
    );
    held.release().await;
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
        let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
        let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
        let message_id = queue_direct(&fixture, now, number).await;

        let fault = EngineCommitFault {
            key: collaboration_central_core::engine_probe::InstanceKey(message_id.to_string()),
            kind,
        };
        assert!(
            runtime
                .control::<bool>("engine-commit-fault", &fault)
                .await
                .unwrap()
        );
        fixture
            .clock
            .0
            .store(mark.timestamp_millis(), Ordering::SeqCst);
        runtime.after_event().await.unwrap();

        assert!(
            !runtime
                .control::<bool>("engine-commit-fault-pending", &())
                .await
                .unwrap()
        );
        assert_store(&runtime, &message_id, expected_state, expected_outcome).await;
        let attempts = runtime.read(Statement {
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
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
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
            .read(Statement {
                sql: "SELECT state,failure_reason FROM mail_messages WHERE message_id=?",
                args: vec![text(&notice_id)],
            })
            .await
            .unwrap()
            .rows
    };
    let submit = async |message_id: workspace_ids::MessageId, receipt| {
        self::receipt(&fixture, &runtime, &message_id, receipt).await
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
        let rejected = submit_receipt(&fixture, &runtime, &notice_id, receipt).await;
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
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let mark = fleet_mail::message_deadline::cancel_due_at(now, now);
    let message_id = queue_direct(&fixture, now, 981006).await;
    raw(
        &runtime,
        "UPDATE mail_messages SET sender=x'01' WHERE message_id=?",
        vec![text(&message_id)],
    )
    .await;

    fixture
        .clock
        .0
        .store(mark.timestamp_millis(), Ordering::SeqCst);
    runtime.after_event().await.unwrap();

    assert_store(&runtime, &message_id, "queued", None).await;
    let attempts = runtime.read(Statement {
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
    runtime: &RemoteRuntime,
    message_id: &MessageId,
    state: &str,
    outcome: Option<&str>,
) {
    let rows = runtime.read(Statement {
        sql: "SELECT state,(SELECT json_extract(body,'$.outcome.kind') FROM mail_messages WHERE message_kind='deliveryOutcome') AS outcome,(SELECT COUNT(*) FROM mail_messages WHERE message_kind='deliveryOutcome') AS outcome_count FROM mail_messages WHERE message_id=?",
        args: vec![text(message_id)],
    }).await.unwrap();
    assert_eq!(
        rows.rows,
        [vec![
            text(state),
            outcome.map_or(SqlValue::Null, text),
            SqlValue::Integer(i64::from(outcome.is_some()).try_into().unwrap())
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
