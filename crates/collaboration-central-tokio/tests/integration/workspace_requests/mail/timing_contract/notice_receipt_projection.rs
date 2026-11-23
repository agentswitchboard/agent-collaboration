use super::*;

pub(super) async fn exercise_non_utc_and_terminal_rows(f: &Fixture) {
    let target = session(&f.caller);
    let result=request(f,&f.caller,46100,Op::SetAutomation{target:target.clone(),timing:serde_json::from_value(serde_json::json!({"kind":"cron","expression":"*/5 * * * *","timezone":"Asia/Kathmandu"})).unwrap(),text:body()}).await;
    let Result::Done(Answer::AutomationSet {
        automation_id,
        next_due_at: Some(first),
        ..
    }) = result
    else {
        panic!("nonUTC admission: {result:?}")
    };
    assert_eq!(first.timestamp().rem_euclid(300), 0);
    dispatch_to(f, first).await;
    let r = f.host.central(f.caller.organization_id()).await.unwrap();
    let batch = r.queued_for_session(&target).await.unwrap();
    let fired:Vec<_>=batch.messages.iter().filter(|message|matches!(&message.body,MessageBody::Automation{automation_id:id,..}if id==&automation_id)).collect();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].created_at, first);
    let next = first + chrono::Duration::minutes(5);
    let row = r
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT next_due_at FROM automation_schedules WHERE automation_id=?",
            args: vec![text(&automation_id)],
        })
        .await
        .unwrap();
    assert_eq!(
        row.rows,
        vec![vec![text(
            next.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        )]]
    );
    drop(r);
    // A distinct admitted request re-decodes the active non-UTC row before commit.
    assert!(matches!(
        request(
            f,
            &f.caller,
            46101,
            Op::SetNoticeCadence {
                cadence: m::NoticeCadence::M15
            }
        )
        .await,
        Result::Done(_)
    ));
    assert!(matches!(
        request(
            f,
            &f.caller,
            46102,
            Op::DisableAutomation {
                automation_id: automation_id.clone()
            }
        )
        .await,
        Result::Done(_)
    ));
    use central_storage::{AlarmUpdate, GuardedStatement, RowExpectation, StatementBatch};
    let r = f.host.central(f.caller.organization_id()).await.unwrap();
    // A retired rule is not reinterpreted against the current timezone database.
    assert!(matches!(r.runtime().statement_host().run_batch(StatementBatch{statements:vec![GuardedStatement{statement:Statement{sql:"UPDATE automation_schedules SET timing_rule=? WHERE automation_id=?",args:vec![text(r#"{"kind":"cron","expression":"*/5 * * * *","timezone":"Retired/Timezone"}"#),text(&automation_id)]},expect:RowExpectation::Exactly(1)}],alarm:AlarmUpdate::Unchanged}).await.unwrap(),central_storage::BatchOutcome::Applied{..}));
    assert!(matches!(
        request(
            f,
            &f.caller,
            46103,
            Op::SetNoticeCadence {
                cadence: m::NoticeCadence::M15
            }
        )
        .await,
        Result::Done(_)
    ));
    let store = r.runtime().statement_host();
    let row = store.read(Statement {
        sql: "SELECT message_id FROM mail_messages WHERE message_kind='notice' AND state='queued' ORDER BY message_id LIMIT 1",
        args: vec![],
    }).await.unwrap();
    let central_storage::SqlValue::Text(notice) = &row.rows[0][0] else {
        panic!("queued notice id")
    };
    let notice: workspace_ids::MessageId = notice.parse().unwrap();
    let receipt = workspace_model::central_mail::ReceiptKind::Delivered { arrived_at: now(f) };
    let caller = f.fresh_caller(&f.caller).await;
    assert_eq!(
        r.apply_delivery_receipt(&caller.connection().attachment(), &notice, receipt)
            .await
            .unwrap(),
        fleet_contracts::mail_link::MailReceiptSettlement::Applied,
    );
    assert!(matches!(store.run_batch(StatementBatch {
        statements: vec![GuardedStatement {
            statement: Statement { sql: "UPDATE mail_messages SET body='retired corrupt body' WHERE message_id=?", args: vec![text(&notice)] },
            expect: RowExpectation::Exactly(1),
        }],
        alarm: AlarmUpdate::Unchanged,
    }).await.unwrap(), central_storage::BatchOutcome::Applied { .. }));
    let area = f.area(46104).await;
    let thread = f.thread(46105, area).await;
    created(
        f.call(
            46106,
            m::WorkspaceOperation::ThreadPost {
                thread_id: thread,
                body: body(),
                addressees: vec![],
            },
        )
        .await,
    );
}
