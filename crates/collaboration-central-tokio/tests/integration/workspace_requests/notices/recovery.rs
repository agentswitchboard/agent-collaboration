use super::*;

#[path = "recovery/restart_cut.rs"]
mod restart_cut;

#[tokio::test]
async fn another_workspace_cannot_extend_a_pending_drain_deadline() {
    use central_storage::{
        AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, StatementBatch,
    };
    use std::time::Duration;
    let f = Fixture::new().await;
    let created_workspace = call(
        &f.host,
        &f.caller,
        None,
        7230,
        m::WorkspaceOperation::CreateWorkspace {
            name: name("Second workspace"),
        },
    )
    .await;
    let m::WorkspaceResult::Done(m::WorkspaceAnswer::WorkspaceCreated { workspace }) =
        created_workspace.result
    else {
        panic!("workspace creation")
    };
    let second_workspace = workspace.workspace_id;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    assert!(matches!(
        runtime
            .runtime()
            .statement_host()
            .run_batch(StatementBatch {
                statements: vec![GuardedStatement {
                    statement: Statement {
                        sql: collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
                        args: vec![
                            text(
                                serde_json::to_string(&vec![
                                    f.workspace.clone(),
                                    second_workspace.clone()
                                ])
                                .unwrap()
                            ),
                            text(f.caller.caller().machine_id())
                        ]
                    },
                    expect: RowExpectation::Exactly(2),
                }],
                alarm: AlarmUpdate::Unchanged
            })
            .await
            .unwrap(),
        BatchOutcome::Applied { .. }
    ));
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold(&f.workspace, PausePoint::AfterStoreApplied);
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        let caller = f.caller.clone();
        let workspace = f.workspace.clone();
        async move {
            runtime
                .request(
                    &caller,
                    m::WorkspaceRequest {
                        request_id: Some(id(7231)),
                        workspace_id: Some(workspace),
                        operation: m::WorkspaceOperation::CreateArea {
                            name: name("Pending first workspace"),
                            description: "Test description".to_owned().try_into().unwrap(),
                        },
                    },
                )
                .await
                .unwrap()
        }
    });
    held.until_held().await;
    let read_due = || Statement {
        sql: "SELECT drain_due_at FROM mail_workspace_drains WHERE workspace_id=?",
        args: vec![text(&f.workspace)],
    };
    let original = runtime
        .runtime()
        .statement_host()
        .read(read_due())
        .await
        .unwrap();
    assert!(matches!(&original.rows[0][0], SqlValue::Text(_)));
    let original_alarm = runtime
        .runtime()
        .statement_host()
        .alarm()
        .expect("pending drain alarm");
    let SqlValue::Text(due) = &original.rows[0][0] else {
        panic!("drain deadline")
    };
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(due)
            .unwrap()
            .timestamp_millis(),
        f.clock.utc_now().timestamp_millis() + 5_000
    );
    let original_due = f.clock.utc_now() + chrono::Duration::seconds(5);
    let expected_alarm = chrono::DateTime::from_timestamp_millis(
        (original_due.timestamp_millis().div_euclid(300_000) + 1) * 300_000,
    )
    .unwrap();
    assert_eq!(
        original_alarm.unix_millis(),
        expected_alarm.timestamp_millis()
    );
    for second in 1..=60 {
        f.advance(Duration::from_secs(1)).await;
        let caller = f.fresh_caller(&f.caller).await;
        let other = call(
            &f.host,
            &caller,
            Some(second_workspace.clone()),
            7300 + second,
            m::WorkspaceOperation::CreateArea {
                name: name(&format!("Second workspace activity {second}")),
                description: "Test description".to_owned().try_into().unwrap(),
            },
        )
        .await;
        assert!(matches!(
            other.result,
            m::WorkspaceResult::Done(m::WorkspaceAnswer::Created { .. })
        ));
        assert_eq!(
            runtime
                .runtime()
                .statement_host()
                .read(read_due())
                .await
                .unwrap()
                .rows,
            original.rows
        );
        assert_eq!(
            runtime.runtime().statement_host().alarm(),
            Some(original_alarm),
            "W2 moved W1's retained mark at second {second}"
        );
    }
    let completed = runtime.runtime().alarm_completion_count();
    f.advance_to(original_alarm).await;
    runtime.runtime().until_alarm_completed(completed + 1).await;
    assert_eq!(
        runtime
            .runtime()
            .statement_host()
            .read(read_due())
            .await
            .unwrap()
            .rows,
        original.rows
    );
    let mut drained = runtime
        .runtime()
        .pause_points()
        .hold(&f.workspace, PausePoint::AfterDrainCommit);
    pending.abort();
    let _ = pending.await;
    drop(held);
    // P5 change 6: no due batch waited behind the held turn, and the aborted write
    // never ran its own drain, so the next due batch (the published alarm) drains W1.
    let next = runtime
        .runtime()
        .statement_host()
        .alarm()
        .expect("W1 drain keeps an alarm");
    // Let the real native driver settle on the published deadline, then advance
    // the one clock: the driver itself runs the due batch that drains W1.
    f.clock.not_due_check_now(f.clock.wall_deadline(next)).await;
    let before_driver = runtime.runtime().alarm_completion_count();
    f.advance_to(next).await;
    let driver_drained = tokio::select! {
        () = drained.until_held() => true,
        () = runtime.runtime().until_alarm_completed(before_driver + 1) => false,
    };
    assert!(
        driver_drained,
        "the native driver completed without reaching the due drain"
    );
    let after = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,drain_due_at FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&f.workspace)] }).await.unwrap();
    assert_eq!(after.rows.len(), 1);
    assert!(matches!(&after.rows[0][0], SqlValue::Integer(position) if position.get() > 0));
    assert_eq!(after.rows[0][1], SqlValue::Null);
    drained.release();
    runtime
        .runtime()
        .until_alarm_completed(before_driver + 1)
        .await;
    f.host.close().await;
}

#[tokio::test]
async fn drain_advances_by_position_even_when_clock_and_change_id_move_backwards() {
    let mut f = Fixture::new().await;
    let area = f.area(7240).await;
    let thread = f.thread(7241, area.clone()).await;
    let (observer, actor) = joined_observer(&f, &thread).await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let first = f
        .call(
            7242,
            post(
                &thread,
                vec![m::Addressee::Actor {
                    actor_identity_id: actor.clone(),
                }],
            ),
        )
        .await;
    let m::WorkspaceResult::Done(m::WorkspaceAnswer::Created {
        item: first_item,
        position: first_position,
    }) = first.result
    else {
        panic!("first post")
    };
    let (_, first_item_id) = first_item.kind_and_id();
    let first_change = runtime.runtime().inspect_workspace(&f.workspace, Statement { sql: "SELECT change_id,created_at FROM workspace_changes WHERE item_id=? AND change_kind='posted'", args: vec![text(first_item_id)] }).await.unwrap();
    let SqlValue::Text(first_id) = &first_change.rows[0][0] else {
        panic!("first change id")
    };
    let first_id = first_id.clone();
    let prior = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?",
            args: vec![text(&f.workspace)],
        })
        .await
        .unwrap();
    let previous_time = f.clock.utc_now();
    f.set_utc(previous_time - chrono::Duration::hours(1)).await;
    drop(runtime);
    f.reopen().await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    assert_eq!(
        runtime
            .runtime().statement_host()
            .read(Statement {
                sql: "SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?",
                args: vec![text(&f.workspace)]
            })
            .await
            .unwrap()
            .rows,
        prior.rows
    );
    let second = f
        .call(
            7243,
            post(
                &thread,
                vec![m::Addressee::Actor {
                    actor_identity_id: actor.clone(),
                }],
            ),
        )
        .await;
    let m::WorkspaceResult::Done(m::WorkspaceAnswer::Created {
        item: second_item,
        position: second_position,
    }) = second.result
    else {
        panic!("second post")
    };
    let (_, second_item_id) = second_item.kind_and_id();
    let stored = runtime
        .runtime().inspect_workspace(
            &f.workspace,
            Statement {
                sql: "SELECT position,created_at,change_id FROM workspace_changes WHERE item_id=? AND change_kind='posted'",
                args: vec![text(second_item_id)],
            },
        )
        .await
        .unwrap();
    let SqlValue::Integer(previous_position) = &prior.rows[0][0] else {
        panic!("prior cursor")
    };
    let SqlValue::Integer(new_position) = &stored.rows[0][0] else {
        panic!("new position")
    };
    assert!(new_position.get() > previous_position.get());
    assert!(second_position > first_position);
    let SqlValue::Text(later_id) = &stored.rows[0][2] else {
        panic!("second change id")
    };
    assert!(
        later_id < &first_id,
        "clock rollback must produce a smaller change id"
    );
    let SqlValue::Text(created_at) = &stored.rows[0][1] else {
        panic!("change timestamp")
    };
    assert!(
        chrono::DateTime::parse_from_rfc3339(created_at)
            .unwrap()
            .timestamp_millis()
            < previous_time.timestamp_millis()
    );
    let cursor = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?",
            args: vec![text(&f.workspace)],
        })
        .await
        .unwrap();
    assert_eq!(cursor.rows[0][0], stored.rows[0][0]);
    let notice = runtime.runtime().statement_host().read(Statement { sql: "SELECT i.change_id FROM mail_notification_window_items i JOIN mail_notification_windows w ON w.window_id=i.window_id WHERE w.recipient_actor=? AND i.thread_id=? ORDER BY i.change_id", args: vec![text(&actor), text(&thread)] }).await.unwrap();
    assert_eq!(
        notice.rows,
        vec![vec![text(later_id)], vec![text(first_id)]]
    );
    let observer = f.fresh_caller(&observer).await;
    let inbox = call(
        &f.host,
        &observer,
        Some(f.workspace.clone()),
        0,
        m::WorkspaceOperation::Inbox {
            after: None,
            limit: 100.try_into().unwrap(),
        },
    )
    .await;
    let m::WorkspaceResult::Done(m::WorkspaceAnswer::Inbox { entries, .. }) = inbox.result else {
        panic!("observer inbox")
    };
    let post_positions: Vec<_> = entries
        .iter()
        .filter(|entry| entry.position == first_position || entry.position == second_position)
        .map(|entry| entry.position)
        .collect();
    assert_eq!(post_positions, vec![first_position, second_position]);
    f.host.close().await;
}
