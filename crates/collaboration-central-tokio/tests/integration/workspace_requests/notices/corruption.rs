use super::*;
use central_storage::{
    AlarmUpdate, BatchOutcome, ConstraintKind, CorruptReason, GuardedStatement, RowExpectation,
    StatementBatch, StoreFailure,
};

async fn execute(f: &Fixture, sql: &'static str, args: Vec<SqlValue>) {
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let result = runtime
        .runtime()
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![GuardedStatement {
                statement: Statement { sql, args },
                expect: RowExpectation::Any,
            }],
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(result, BatchOutcome::Applied { .. }));
}

#[tokio::test]
async fn invalid_open_window_is_rejected_before_alarm_changes_queue_or_cursor() {
    for (sql, expected_column) in [
        (
            "UPDATE mail_notification_windows SET due_at='not-a-timestamp' WHERE recipient_actor=?",
            "mail_notification_windows.due_at",
        ),
        (
            "UPDATE mail_notification_windows SET due_at='2026-11-13T12:05:00Z' WHERE recipient_actor=?",
            "mail_notification_windows.due_at",
        ),
        (
            "UPDATE mail_notification_windows SET recipient_actor='invalid-uuid' WHERE recipient_actor=?",
            "mail_notification_windows.recipient_actor",
        ),
    ] {
        let f = Fixture::new().await;
        let area = f.area(7500).await;
        let thread = f.thread(7501, area).await;
        let (_, actor) = joined_observer(&f, &thread).await;
        created(f.call(7502, post(&thread, vec![])).await);
        let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
        let read = || Statement {
            sql: "SELECT (SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?),(SELECT COUNT(*) FROM mail_notification_windows WHERE window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)),(SELECT COUNT(*) FROM mail_notification_window_items),(SELECT COUNT(*) FROM mail_messages WHERE message_kind='notice')",
            args: vec![text(&f.workspace), text(&thread)],
        };
        let before = runtime
            .runtime()
            .statement_host()
            .read(read())
            .await
            .unwrap();
        assert_eq!(before.rows[0][1], SqlValue::Integer(2.try_into().unwrap()));
        assert_eq!(before.rows[0][2], SqlValue::Integer(2.try_into().unwrap()));
        assert_eq!(before.rows[0][3], SqlValue::Integer(0.try_into().unwrap()));
        let prior_alarm = runtime.runtime().statement_host().alarm();
        execute(&f, sql, vec![text(&actor)]).await;
        let millis = prior_alarm
            .expect("the open windows have a retained alarm")
            .unix_millis();
        let due = chrono::DateTime::from_timestamp_millis(millis).unwrap();
        f.set_utc(due).await;
        // Isolation: the window unit fails as a whole with no window, queue or
        // cursor change; the alarm still re-arms and the failure is the unit's record.
        runtime.after_event().await.unwrap();
        assert_eq!(
            runtime
                .runtime()
                .statement_host()
                .read(read())
                .await
                .unwrap()
                .rows,
            before.rows
        );
        assert_eq!(
            runtime
                .runtime()
                .statement_host()
                .alarm()
                .map(|time| time.unix_millis()),
            Some((due.timestamp_millis().div_euclid(300_000) + 1) * 300_000)
        );
        let record = runtime.runtime().statement_host().read(Statement { sql: "SELECT state,failure_count,last_failure,last_failure_detail FROM engine_work_attempts WHERE machine_kind='legacy_notice_windows' AND subject_key='all'", args: vec![] }).await.unwrap();
        assert_eq!(
            &record.rows[0][..3],
            &[
                text("failing"),
                SqlValue::Integer(1.try_into().unwrap()),
                text("corrupt_row")
            ]
        );
        assert!(
            matches!(&record.rows[0][3], SqlValue::Text(detail) if detail.contains(expected_column))
        );
        let recipient = runtime.runtime().statement_host().read(Statement { sql: "SELECT COUNT(*) FROM mail_messages WHERE recipient_actor=? AND message_kind='notice'", args: vec![text(actor)] }).await.unwrap();
        assert_eq!(
            recipient.rows,
            vec![vec![SqlValue::Integer(0.try_into().unwrap())]]
        );
        f.host.close().await;
    }
}

#[tokio::test]
async fn later_drain_statement_failure_rolls_back_notices_windows_and_cursor_then_retries_once() {
    let f = Fixture::new().await;
    let area = f.area(7510).await;
    let thread = f.thread(7511, area).await;
    let (_, observer) = joined_observer(&f, &thread).await;
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    // Addressing the author adds a second recipient while the observer remains in the same batch window.
    let actors = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT actor_identity_id,identity FROM access_actor_identities",
            args: vec![],
        })
        .await
        .unwrap();
    let author: m::ActorIdentityId = actors
        .rows
        .iter()
        .find_map(|row| match (&row[0], &row[1]) {
            (SqlValue::Text(id), SqlValue::Text(identity)) if identity.contains("test-session") => {
                Some(id.parse().unwrap())
            }
            _ => None,
        })
        .unwrap();
    let prior = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?",
            args: vec![text(&f.workspace)],
        })
        .await
        .unwrap();
    let baseline = runtime.runtime().statement_host().read(Statement { sql: "SELECT (SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice'),(SELECT COUNT(*) FROM mail_notification_windows WHERE window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)),(SELECT COUNT(*) FROM mail_notification_window_items)", args: vec![text(&thread), text(&thread)] }).await.unwrap();
    assert_eq!(
        baseline.rows,
        vec![vec![
            SqlValue::Integer(0.try_into().unwrap()),
            SqlValue::Integer(1.try_into().unwrap()),
            SqlValue::Integer(1.try_into().unwrap())
        ]]
    );
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold(&f.workspace, PausePoint::AfterStoreApplied);
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        let caller = f.fresh_caller(&f.caller).await;
        let workspace = f.workspace.clone();
        let operation = post(
            &thread,
            vec![m::Addressee::Actor {
                actor_identity_id: author.clone(),
            }],
        );
        async move {
            runtime
                .request(
                    &caller,
                    m::WorkspaceRequest {
                        request_id: Some(id(7512)),
                        workspace_id: Some(workspace),
                        operation,
                    },
                )
                .await
        }
    });
    held.until_held().await;
    pending.abort();
    let _ = pending.await;
    drop(held);
    execute(&f, "CREATE TRIGGER reject_drain_cursor BEFORE UPDATE ON mail_workspace_drains WHEN NEW.drained_through_position != OLD.drained_through_position BEGIN SELECT RAISE(ABORT,'reject drain cursor'); END", vec![]).await;
    assert!(matches!(
        runtime.drain_workspace(&f.workspace).await,
        Err(StoreFailure::Unavailable)
    ));
    let after_failure = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,drain_due_at,(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice'),(SELECT COUNT(*) FROM mail_notification_windows WHERE window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)),(SELECT COUNT(*) FROM mail_notification_window_items) FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&thread), text(&thread), text(&f.workspace)] }).await.unwrap();
    assert_eq!(after_failure.rows[0][0], prior.rows[0][0]);
    assert!(matches!(&after_failure.rows[0][1], SqlValue::Text(_)));
    assert_eq!(&after_failure.rows[0][2..], &baseline.rows[0][..]);
    execute(&f, "DROP TRIGGER reject_drain_cursor", vec![]).await;
    let result = runtime.drain_workspace(&f.workspace).await.unwrap();
    assert_eq!(result.notices_queued, 0);
    assert_eq!(result.window_items_added, 2);
    let again = runtime.drain_workspace(&f.workspace).await.unwrap();
    assert_eq!(again.notices_queued, 0);
    assert_eq!(again.window_items_added, 0);
    let after_retry = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,drain_due_at,(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND recipient_actor=? AND message_kind='notice'),(SELECT COUNT(*) FROM mail_notification_windows WHERE window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?) AND recipient_actor=?),(SELECT COUNT(*) FROM mail_notification_window_items) FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&thread), text(&author), text(&thread), text(&observer), text(&f.workspace)] }).await.unwrap();
    assert_ne!(after_retry.rows[0][0], prior.rows[0][0]);
    assert_eq!(after_retry.rows[0][1], SqlValue::Null);
    assert_eq!(
        &after_retry.rows[0][2..],
        &[
            SqlValue::Integer(0.try_into().unwrap()),
            SqlValue::Integer(1.try_into().unwrap()),
            SqlValue::Integer(3.try_into().unwrap())
        ]
    );
    let duplicate = runtime.runtime().statement_host().run_batch(StatementBatch { statements: vec![GuardedStatement { statement: Statement {
        sql: "INSERT INTO mail_notification_window_items(window_item_id,window_id,workspace_id,thread_id,change_id,created_at,updated_at) SELECT ?,i.window_id,i.workspace_id,i.thread_id,i.change_id,i.created_at,i.updated_at FROM mail_notification_window_items i JOIN mail_notification_windows w ON w.window_id=i.window_id WHERE w.recipient_actor=? AND i.thread_id=?", args: vec![text(id::<m::WorkspaceId>(7513)), text(&observer), text(&thread)],
    }, expect: RowExpectation::Exactly(1) }], alarm: AlarmUpdate::Unchanged }).await;
    assert!(matches!(
        duplicate,
        Ok(BatchOutcome::ConstraintViolated {
            index: 0,
            kind: ConstraintKind::Unique
        })
    ));
    f.host.close().await;
}

#[tokio::test]
async fn corrupt_existing_notice_body_stops_drain_before_cursor_or_new_effects() {
    let f = Fixture::new().await;
    let area = f.area(7520).await;
    let thread = f.thread(7521, area).await;
    let (_, recipient) = joined_observer(&f, &thread).await;
    created(
        f.call(
            7522,
            post(
                &thread,
                vec![m::Addressee::Actor {
                    actor_identity_id: recipient.clone(),
                }],
            ),
        )
        .await,
    );
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    f.advance(std::time::Duration::from_secs(900)).await;
    runtime.after_event().await.unwrap();
    let before = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice') FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&thread), text(&f.workspace)] }).await.unwrap();
    assert_eq!(before.rows[0][1], SqlValue::Integer(2.try_into().unwrap()));
    execute(
        &f,
        "UPDATE mail_messages SET body='not-json' WHERE source_thread_id=? AND message_kind='notice'",
        vec![text(&thread)],
    )
    .await;
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold(&f.workspace, PausePoint::AfterStoreApplied);
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        let caller = f.fresh_caller(&f.caller).await;
        let workspace = f.workspace.clone();
        let operation = post(
            &thread,
            vec![m::Addressee::Actor {
                actor_identity_id: recipient.clone(),
            }],
        );
        async move {
            runtime
                .request(
                    &caller,
                    m::WorkspaceRequest {
                        request_id: Some(id(7523)),
                        workspace_id: Some(workspace),
                        operation,
                    },
                )
                .await
        }
    });
    held.until_held().await;
    pending.abort();
    let _ = pending.await;
    drop(held);
    let error = runtime.drain_workspace(&f.workspace).await.unwrap_err();
    assert!(
        matches!(error, StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } } if column == "mail_messages.body")
    );
    let after = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice'),drain_due_at FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&thread), text(&f.workspace)] }).await.unwrap();
    assert_eq!(&after.rows[0][..2], &before.rows[0][..]);
    assert!(matches!(&after.rows[0][2], SqlValue::Text(_)));
    f.host.close().await;
}

#[tokio::test]
async fn derived_window_above_alarm_ceiling_preserves_workspace_write_and_rolls_back_drain() {
    let mut f = Fixture::new().await;
    let area = f.area(7530).await;
    let thread = f.thread(7531, area).await;
    let (_, recipient) = joined_observer(&f, &thread).await;
    // Retire the creator's join-activity window before moving the one clock.
    // Otherwise the real driver correctly fires that unrelated old window.
    assert!(matches!(
        f.call(
            7533,
            m::WorkspaceOperation::SetNotifications {
                thread_id: thread.clone(),
                setting: m::NotificationSetting::Off
            }
        )
        .await
        .result,
        m::WorkspaceResult::Done(m::WorkspaceAnswer::Updated { .. })
    ));
    let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
    let prior = runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT drained_through_position FROM mail_workspace_drains WHERE workspace_id=?",
            args: vec![text(&f.workspace)],
        })
        .await
        .unwrap();
    let effects_before = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT (SELECT COUNT(*) FROM mail_notification_windows WHERE recipient_actor=? AND window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)),(SELECT COUNT(*) FROM mail_notification_window_items),(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice')",
        args: vec![text(&recipient), text(&thread), text(&thread)],
    }).await.unwrap();
    assert_eq!(
        effects_before.rows,
        vec![vec![
            SqlValue::Integer(0.try_into().unwrap()),
            SqlValue::Integer(0.try_into().unwrap()),
            SqlValue::Integer(0.try_into().unwrap())
        ]]
    );
    let near_ceiling = chrono::DateTime::from_timestamp_millis(
        collaboration_central_core::MAX_ALARM_UNIX_MILLIS - 30_000,
    )
    .unwrap();
    f.relocate_authority(near_ceiling).await;
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold(&f.workspace, PausePoint::AfterStoreApplied);
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        let caller = f.caller.clone();
        let workspace = f.workspace.clone();
        let operation = post(&thread, vec![]);
        async move {
            runtime
                .request(
                    &caller,
                    m::WorkspaceRequest {
                        request_id: Some(id(7532)),
                        workspace_id: Some(workspace),
                        operation,
                    },
                )
                .await
        }
    });
    held.until_held().await;
    pending.abort();
    let _ = pending.await;
    drop(held);
    let applied = runtime.runtime().inspect_workspace(&f.workspace, Statement {
        sql: "SELECT (SELECT COUNT(*) FROM workspace_changes WHERE thread_id=? AND change_kind='posted'),(SELECT COUNT(*) FROM workspace_request_log WHERE request_id=?)",
        args: vec![text(&thread), text(id::<m::RequestId>(7532))],
    }).await.unwrap();
    assert_eq!(
        applied.rows,
        vec![vec![
            // The thread's first post and this one.
            SqlValue::Integer(2.try_into().unwrap()),
            SqlValue::Integer(1.try_into().unwrap())
        ]]
    );
    let pending_before = runtime.runtime().statement_host().read(Statement { sql: "SELECT drained_through_position,drain_due_at FROM mail_workspace_drains WHERE workspace_id=?", args: vec![text(&f.workspace)] }).await.unwrap();
    assert_eq!(pending_before.rows[0][0], prior.rows[0][0]);
    assert!(matches!(&pending_before.rows[0][1], SqlValue::Text(_)));
    let error = runtime.drain_workspace(&f.workspace).await.unwrap_err();
    assert!(
        matches!(error, StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } } if column == "mail_notification_windows.due_at")
    );
    let after = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT drained_through_position,drain_due_at,(SELECT COUNT(*) FROM mail_notification_windows WHERE recipient_actor=? AND window_id IN (SELECT window_id FROM mail_notification_window_items WHERE thread_id=?)),(SELECT COUNT(*) FROM mail_notification_window_items),(SELECT COUNT(*) FROM mail_messages WHERE source_thread_id=? AND message_kind='notice') FROM mail_workspace_drains WHERE workspace_id=?",
        args: vec![text(&recipient), text(&thread), text(&thread), text(&f.workspace)],
    }).await.unwrap();
    assert_eq!(&after.rows[0][..2], &pending_before.rows[0][..]);
    assert_eq!(&after.rows[0][2..], &effects_before.rows[0][..]);
    assert_eq!(runtime.runtime().inspect_workspace(&f.workspace, Statement { sql: "SELECT (SELECT COUNT(*) FROM workspace_changes WHERE thread_id=? AND change_kind='posted'),(SELECT COUNT(*) FROM workspace_request_log WHERE request_id=?)", args: vec![text(&thread), text(id::<m::RequestId>(7532))] }).await.unwrap().rows, applied.rows);
    f.host.close().await;
}

#[tokio::test]
async fn workspace_drain_preset_validates_the_deadline_before_any_write() {
    let mut fixture = Fixture::new().await;
    let area = fixture.area(7540).await;
    let thread = fixture.thread(7541, area).await;
    let runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let store = runtime.runtime().statement_host();
    let workspace = fixture.workspace.clone();
    let read_preset = || Statement {
        sql: "SELECT drain_due_at FROM mail_workspace_drains WHERE workspace_id=?",
        args: vec![text(&workspace)],
    };
    assert_eq!(
        store.read(read_preset()).await.unwrap().rows,
        [[SqlValue::Null]]
    );

    let maximum = collaboration_central_core::MAX_ALARM_UNIX_MILLIS;
    fixture
        .relocate_authority(m::Timestamp::from_timestamp_millis(maximum - 5_001).unwrap())
        .await;
    let mut pause = runtime
        .runtime()
        .pause_points()
        .hold(&fixture.workspace, PausePoint::AfterStoreApplied);
    let request = |number| m::WorkspaceRequest {
        request_id: Some(id(number)),
        workspace_id: Some(fixture.workspace.clone()),
        operation: post(&thread, vec![]),
    };
    let accepted = tokio::spawn({
        let runtime = runtime.clone();
        let caller = fixture.caller.clone();
        let request = request(7542);
        async move { runtime.request(&caller, request).await }
    });
    pause.until_held().await;
    assert_eq!(
        store.read(read_preset()).await.unwrap().rows,
        [[text("2262-04-11T23:47:16.853Z")]],
    );
    accepted.abort();
    assert!(accepted.await.unwrap_err().is_cancelled());
    drop(pause);

    execute(
        &fixture,
        "UPDATE mail_workspace_drains SET drain_due_at=NULL WHERE workspace_id=?",
        vec![text(&fixture.workspace)],
    )
    .await;
    let refused_request = request(7543);
    fixture
        .set_utc(m::Timestamp::from_timestamp_millis(maximum - 1).unwrap())
        .await;
    let result = runtime.request(&fixture.caller, refused_request).await;
    assert!(
        matches!(result, Err(StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } }) if column == "drain_due_at")
    );
    assert_eq!(
        store.read(read_preset()).await.unwrap().rows,
        [[SqlValue::Null]]
    );
    let retained = runtime
        .runtime()
        .inspect_workspace(
            &fixture.workspace,
            Statement {
                sql: "SELECT request_id FROM workspace_request_log WHERE request_id=?",
                args: vec![text(id::<m::RequestId>(7543))],
            },
        )
        .await
        .unwrap();
    assert!(retained.rows.is_empty());
    fixture.host.close().await;
}

#[tokio::test]
async fn cadence_retime_writes_canonical_utc_and_rejects_the_next_mark_above_the_ceiling() {
    use workspace_model::central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
    };

    let mut fixture = Fixture::new().await;
    let area = fixture.area(7600).await;
    let thread = fixture.thread(7601, area).await;
    let (observer, actor) = joined_observer(&fixture, &thread).await;
    fixture.call(7602, post(&thread, vec![])).await;
    let runtime = fixture
        .host
        .central(fixture.caller.organization_id())
        .await
        .unwrap();
    let store = runtime.runtime().statement_host();
    let snapshot = || Statement {
        sql: "SELECT a.notice_cadence,w.due_at,w.updated_at,(SELECT COUNT(*) FROM runtime_request_log WHERE request_id=?1) FROM mail_notification_windows w JOIN access_actor_identities a ON a.actor_identity_id=w.recipient_actor WHERE w.recipient_actor=?2",
        args: vec![text(id::<m::RequestId>(7604)), text(&actor)],
    };
    let operation = |number, cadence| CentralMailRequest {
        request_id: id(number),
        operation: CentralMailOperation::SetNoticeCadence { cadence },
    };
    let now = fixture.clock.utc_now();
    assert_eq!(
        fixture
            .host
            .request_central(&observer, operation(7603, m::NoticeCadence::M15))
            .await
            .unwrap(),
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet {
            cadence: m::NoticeCadence::M15
        }),
    );
    let before = store.read(snapshot()).await.unwrap();
    let expected = chrono::DateTime::<chrono::Utc>::from_timestamp(
        (now.timestamp().div_euclid(900) + 1) * 900,
        0,
    )
    .unwrap();
    assert_eq!(
        before.rows[0][1],
        text(expected.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    );

    let edge = m::Timestamp::from_timestamp_millis(
        collaboration_central_core::MAX_ALARM_UNIX_MILLIS - 30_000,
    )
    .unwrap();
    fixture.relocate_authority(edge).await;
    let observer = fixture.fresh_caller(&observer).await;
    let result = fixture
        .host
        .request_central(&observer, operation(7604, m::NoticeCadence::M55))
        .await;
    assert!(
        matches!(result, Err(StoreFailure::Corrupt { reason: CorruptReason::BadValue { column } }) if column == "mail_notification_windows.due_at")
    );
    assert_eq!(store.read(snapshot()).await.unwrap(), before);
    fixture.host.close().await;
}
