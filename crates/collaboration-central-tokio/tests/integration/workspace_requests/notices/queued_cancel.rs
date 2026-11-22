use super::*;
use router_clock::ClockInstant;
use std::time::Duration;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult, MessageBody,
    MessageSender,
};

#[tokio::test]
async fn leaving_cancels_the_fired_collated_notice_and_retains_direct_message() {
    let f = Fixture::new().await;
    let area = f.area(7400).await;
    let thread = f.thread(7401, area).await;
    let (observer, recipient) = joined_observer(&f, &thread).await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let posted = f.call(7402, post(&thread, vec![])).await;
    let m::WorkspaceResult::Done(m::WorkspaceAnswer::Created { position, .. }) = posted.result
    else {
        panic!("post")
    };
    let sent = f
        .host
        .request_central(
            &f.caller,
            CentralMailRequest {
                request_id: id(7403),
                operation: CentralMailOperation::SendMessage {
                    to: session(&observer),
                    body: "Direct stays queued".to_owned().try_into().unwrap(),
                    delivery: workspace_model::central_mail::MessageDelivery::Auto,
                    reply_to: None,
                    sent_at: "2100-01-01T00:00:00Z".parse().unwrap(),
                },
            },
        )
        .await
        .unwrap();
    let CentralMailResult::Done(CentralMailAnswer::Queued {
        message_id: direct_id,
    }) = sent
    else {
        panic!("direct mail")
    };
    let mut fired = runtime
        .runtime()
        .pause_points()
        .hold_central(PausePoint::AfterTimerCommit);
    f.clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(900)))
        .await;
    f.advance(Duration::from_secs(900)).await;
    fired.until_held().await;
    let rows = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT message_id,state,sender,body FROM mail_messages WHERE recipient_actor=? AND source_thread_id=? AND message_kind='notice'",
        args: vec![text(&recipient), text(&thread)],
    }).await.unwrap();
    assert_eq!(rows.rows.len(), 1);
    let (SqlValue::Text(notice_id), SqlValue::Text(sender), SqlValue::Text(body)) =
        (&rows.rows[0][0], &rows.rows[0][2], &rows.rows[0][3])
    else {
        panic!("notice encoding")
    };
    assert_eq!(rows.rows[0][1], text("queued"));
    assert_eq!(
        serde_json::from_str::<MessageSender>(sender).unwrap(),
        MessageSender::Central
    );
    let MessageBody::Notice { items } = serde_json::from_str::<MessageBody>(body).unwrap() else {
        panic!("notice body")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].position, position);
    let notice_id = notice_id.clone();
    let projected_mail = || Statement {
        sql: "SELECT work_kind, subject_key FROM engine_due_entries WHERE subject_key IN (?, ?) ORDER BY work_kind",
        args: vec![text(&notice_id), text(&direct_id)],
    };
    assert_eq!(
        runtime
            .runtime()
            .statement_host()
            .read(projected_mail())
            .await
            .unwrap()
            .rows,
        [
            [text("direct_message"), text(&direct_id)],
            [text("system_message_expiry"), text(&notice_id)]
        ],
    );
    let windows = runtime.runtime().statement_host().read(Statement { sql: "SELECT (SELECT COUNT(*) FROM mail_notification_windows WHERE recipient_actor=? AND window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)), (SELECT COUNT(*) FROM mail_notification_window_items WHERE change_id=?)", args: vec![text(&recipient), text(&thread), text(&items[0].change_id)] }).await.unwrap();
    assert_eq!(
        windows.rows,
        vec![vec![
            SqlValue::Integer(0.try_into().unwrap()),
            SqlValue::Integer(0.try_into().unwrap())
        ]]
    );
    fired.release();
    let observer = f.fresh_caller(&observer).await;
    let left = call(
        &f.host,
        &observer,
        Some(f.workspace.clone()),
        7404,
        m::WorkspaceOperation::LeaveThread {
            thread_id: thread.clone(),
        },
    )
    .await;
    assert!(matches!(
        left.result,
        m::WorkspaceResult::Done(m::WorkspaceAnswer::Updated { .. })
    ));
    let after = runtime.runtime().statement_host().read(Statement { sql: "SELECT message_id,state FROM mail_messages WHERE recipient_actor=? AND source_thread_id=? AND message_kind='notice'", args: vec![text(&recipient), text(&thread)] }).await.unwrap();
    assert_eq!(after.rows, vec![vec![text(&notice_id), text("cancelled")]]);
    let direct = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT message_id,state,message_kind FROM mail_messages WHERE message_id=?",
            args: vec![text(&direct_id)],
        })
        .await
        .unwrap();
    assert_eq!(
        direct.rows,
        vec![vec![text(&direct_id), text("queued"), text("direct")]]
    );
    assert_eq!(
        runtime
            .runtime()
            .statement_host()
            .read(projected_mail())
            .await
            .unwrap()
            .rows,
        [[text("direct_message"), text(&direct_id)]],
    );
    f.host.close().await;
}

#[tokio::test]
async fn leave_removes_only_the_cancelled_notice_projection_and_rehomes_the_partial_notice() {
    use central_storage::{
        AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, StatementBatch,
    };
    use workspace_model::central_mail::{NoticeItem, NoticeSource};

    let fixture = Fixture::new().await;
    let area = fixture.area(7450).await;
    let left_thread = fixture.thread(7451, area.clone()).await;
    let other_thread = fixture.thread(7452, area.clone()).await;
    let last_thread = fixture.thread(7457, area).await;
    let (observer, recipient) = joined_observer(&fixture, &left_thread).await;
    let runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let store = runtime.runtime().statement_host();
    let other_recipient: m::ActorIdentityId = id(7453);
    let cancelled: m::MessageId = id(7454);
    let partial: m::MessageId = id(7455);
    let unrelated: m::MessageId = id(7456);
    let item = |number, thread: &m::ThreadId| NoticeItem {
        workspace_id: fixture.workspace.clone(),
        thread_id: Some(thread.clone()),
        change_id: id(number),
        position: u64::from(number).try_into().unwrap(),
        item: m::ItemRef::Thread(thread.clone()),
        actor: recipient.clone(),
        event: m::ChangeEvent::Resolved,
    };
    let retained = item(7462, &other_thread);
    let last = item(7465, &last_thread);
    let mut statements = Vec::new();
    let now = fixture.clock.utc_now();
    for (message, actor, items) in [
        (&cancelled, &recipient, vec![item(7460, &left_thread)]),
        (
            &partial,
            &recipient,
            vec![item(7461, &left_thread), retained.clone(), last.clone()],
        ),
        (&unrelated, &other_recipient, vec![item(7463, &left_thread)]),
    ] {
        let first = &items[0];
        let source = NoticeSource {
            change_id: first.change_id.clone(),
            recipient_actor: actor.clone(),
            workspace_id: first.workspace_id.clone(),
            thread_id: first.thread_id.clone(),
        };
        let (insert, _) =
            fleet_mail::plan_sourced_notice(session(&observer), items, source, now, || {
                Ok(message.clone())
            })
            .unwrap();
        statements.extend(insert);
    }
    assert!(matches!(
        store
            .run_batch(StatementBatch {
                statements,
                alarm: AlarmUpdate::Unchanged
            })
            .await
            .unwrap(),
        BatchOutcome::Applied { .. }
    ));

    let projected = |message: &m::MessageId| Statement {
        sql: "SELECT due_entry_id,occurrence_ms,base_due_ms,effective_due_ms,updated_at FROM engine_due_entries WHERE work_kind='system_message_expiry' AND subject_key=?",
        args: vec![text(message)],
    };
    let owner = |message: &m::MessageId| Statement {
        sql: "SELECT body,state,source_change_id,source_workspace_id,source_thread_id,updated_at FROM mail_messages WHERE message_id=?",
        args: vec![text(message)],
    };
    let partial_before = store.read(projected(&partial)).await.unwrap();
    let unrelated_before = store.read(owner(&unrelated)).await.unwrap();
    let unrelated_due = store.read(projected(&unrelated)).await.unwrap();
    assert_eq!(partial_before.rows.len(), 1);
    assert_eq!(unrelated_due.rows.len(), 1);
    assert_eq!(
        store.read(projected(&cancelled)).await.unwrap().rows.len(),
        1
    );

    let observer = fixture.fresh_caller(&observer).await;
    let left = call(
        &fixture.host,
        &observer,
        Some(fixture.workspace.clone()),
        7464,
        m::WorkspaceOperation::LeaveThread {
            thread_id: left_thread,
        },
    )
    .await;
    assert!(matches!(
        left.result,
        m::WorkspaceResult::Done(m::WorkspaceAnswer::Updated { .. })
    ));
    let after = store.read(owner(&partial)).await.unwrap();
    assert_eq!(row_text(&after.rows[0][1]), "queued");
    let partial_body = serde_json::from_str::<MessageBody>(row_text(&after.rows[0][0])).unwrap();
    assert_eq!(
        partial_body,
        MessageBody::Notice {
            items: vec![retained.clone(), last]
        }
    );
    assert_eq!(
        &after.rows[0][2..5],
        [
            text(retained.change_id),
            text(retained.workspace_id),
            text(&other_thread)
        ]
    );
    assert_eq!(
        store.read(projected(&partial)).await.unwrap(),
        partial_before
    );
    assert_eq!(
        store.read(owner(&unrelated)).await.unwrap(),
        unrelated_before
    );
    assert_eq!(
        store.read(projected(&unrelated)).await.unwrap(),
        unrelated_due
    );
    assert_eq!(
        row_text(&store.read(owner(&cancelled)).await.unwrap().rows[0][1]),
        "cancelled"
    );
    assert!(
        store
            .read(projected(&cancelled))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    let plan = fleet_mail::plan_cancel_queued_notices_held(
        store,
        &fixture.workspace,
        [
            (&other_thread, Some(&recipient)),
            (&last_thread, Some(&recipient)),
        ],
        fixture.clock.utc_now(),
    )
    .await
    .unwrap();
    assert_eq!(plan.len(), 3, "two ordered edits and one projection delete");
    let mut rejected = plan.clone();
    rejected.push(GuardedStatement {
        statement: Statement {
            sql: "UPDATE mail_messages SET state='delivered' WHERE 0",
            args: vec![],
        },
        expect: RowExpectation::Exactly(1),
    });
    let rolled_back = store
        .run_batch(StatementBatch {
            statements: rejected,
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(rolled_back, BatchOutcome::GuardFailed { .. }));
    assert_eq!(store.read(owner(&partial)).await.unwrap(), after);
    assert_eq!(
        store.read(projected(&partial)).await.unwrap(),
        partial_before
    );
    let done = store
        .run_batch(StatementBatch {
            statements: plan,
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(done, BatchOutcome::Applied { .. }));
    let final_owner = store.read(owner(&partial)).await.unwrap();
    assert_eq!(row_text(&final_owner.rows[0][1]), "cancelled");
    let final_body =
        serde_json::from_str::<MessageBody>(row_text(&final_owner.rows[0][0])).unwrap();
    assert_eq!(final_body, MessageBody::Notice { items: vec![] });
    assert!(
        store
            .read(projected(&partial))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    fixture.host.close().await;
}

fn row_text(value: &SqlValue) -> &str {
    let SqlValue::Text(text) = value else {
        panic!("expected stored text, got {value:?}");
    };
    text
}
