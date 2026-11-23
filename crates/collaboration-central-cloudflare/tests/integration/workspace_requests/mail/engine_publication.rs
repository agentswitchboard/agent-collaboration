use super::super::{Statement, support::*};
use central_storage::{
    AlarmPublication, AlarmPublicationFailure, AlarmUpdate, BatchOutcome, EngineAlarmPlan,
    GuardedStatement, RowExpectation, StatementBatch, StoreFailure,
};
use std::sync::atomic::Ordering;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
};

#[tokio::test]
async fn engine_alarm_publication_sets_the_real_retained_deadline() {
    let fixture = Fixture::new().await;
    let runtime = fixture.host.central(&fixture.caller.organization_id).await;
    // Embedded setup allowance: the fixture's setup claimed the due batch at T0, so
    // the held clock moves to T0 + 30 s before this publication; the query's own
    // unaligned deadline then reaches the physical alarm at the next mark.
    let now = fixture.clock.0.load(Ordering::SeqCst) + 30_000;
    fixture.clock.0.store(now, Ordering::SeqCst);
    let _: bool = runtime.control("clock", &now).await.unwrap();
    let due = now + 5_000;
    let mark = (due.div_euclid(300_000) + 1) * 300_000;
    let stamp = chrono::DateTime::from_timestamp_millis(due)
        .unwrap()
        .to_rfc3339();
    let result = runtime
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement {
                    sql: "INSERT INTO mail_notification_windows VALUES (?, ?, ?, ?, ?, ?)",
                    args: vec![
                        text(id::<workspace_ids::NotificationWindowId>(8521)),
                        text(id::<workspace_model::ActorIdentityId>(8522)),
                        text(&stamp),
                        text(&stamp),
                        text(&stamp),
                        text(&stamp),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            }],
            alarm: AlarmUpdate::Engine(EngineAlarmPlan::Recompute),
        })
        .await
        .unwrap();
    assert_eq!(
        result,
        BatchOutcome::Applied {
            rows_affected: vec![1],
            publication: AlarmPublication::Set { unix_millis: mark }
        }
    );
    assert_eq!(
        runtime
            .control::<Option<i64>>("raw-alarm-time", &())
            .await
            .unwrap(),
        Some(mark)
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn an_unpublishable_alarm_rolls_back_rows_and_keeps_the_real_deadline() {
    let fixture = Fixture::new().await;
    let start =
        chrono::DateTime::from_timestamp_millis(fixture.clock.0.load(Ordering::SeqCst)).unwrap();
    let CentralMailResult::Done(CentralMailAnswer::WakeSet { wake_id, .. }) = fixture
        .host
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(8520),
                operation: CentralMailOperation::SetWake {
                    target: session(&fixture.caller),
                    due_at: start + chrono::Duration::minutes(5),
                    text: "before failed publication".to_owned().try_into().unwrap(),
                },
            },
        )
        .await
        .unwrap()
    else {
        panic!("wake refused")
    };
    let runtime = fixture.host.central(&fixture.caller.organization_id).await;
    let prior = runtime
        .control::<Option<i64>>("raw-alarm-time", &())
        .await
        .unwrap();
    assert!(prior.is_some());
    fixture.clock.0.store(
        collaboration_central_core::MAX_ALARM_UNIX_MILLIS,
        Ordering::SeqCst,
    );
    let runtime = fixture.host.central(&fixture.caller.organization_id).await;

    let result = runtime
        .run_batch(StatementBatch {
            statements: vec![
                GuardedStatement {
                    statement: Statement {
                        sql: "UPDATE automation_wakes SET text='must roll back' WHERE wake_id=?",
                        args: vec![text(&wake_id)],
                    },
                    expect: RowExpectation::Exactly(1),
                },
                GuardedStatement {
                    // The pending item moves beyond the previously armed mark;
                    // holding the old alarm would no longer serve any due work.
                    statement: Statement {
                        sql: "UPDATE engine_due_entries SET effective_due_ms=? WHERE work_kind='wake_timer' AND subject_key=?",
                        args: vec![
                            central_storage::SqlValue::Integer(
                                (prior.unwrap() + 300_000).try_into().unwrap(),
                            ),
                            text(&wake_id),
                        ],
                    },
                    expect: RowExpectation::Exactly(1),
                },
            ],
            alarm: AlarmUpdate::Engine(EngineAlarmPlan::Recompute),
        })
        .await;

    assert_eq!(
        result,
        Err(StoreFailure::AlarmUnpublishable {
            reason: AlarmPublicationFailure::NoLaterGridMark
        })
    );
    let rows = runtime
        .read(Statement {
            sql: "SELECT text FROM automation_wakes WHERE wake_id=?",
            args: vec![text(&wake_id)],
        })
        .await
        .unwrap();
    assert_eq!(rows.rows, vec![vec![text("before failed publication")]]);
    assert_eq!(
        runtime
            .control::<Option<i64>>("raw-alarm-time", &())
            .await
            .unwrap(),
        prior
    );
    fixture.host.close().await;
}
