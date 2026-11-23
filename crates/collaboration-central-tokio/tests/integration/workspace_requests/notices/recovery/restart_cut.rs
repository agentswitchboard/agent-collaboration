use super::*;

#[tokio::test]
async fn drain_commit_crash_cuts_replay_without_loss_or_duplicate() {
    use workspace_model::central_mail::MessageBody;
    for point in [PausePoint::BeforeDrainCommit, PausePoint::AfterDrainCommit] {
        let mut f = Fixture::new().await;
        let area = f.area(7210).await;
        let thread = f.thread(7211, area).await;
        let (_, actor) = joined_observer(&f, &thread).await;
        let runtime = f.host.central(f.caller.organization_id()).await.unwrap();
        let workspace_id = f.workspace.clone();
        let read_cursor = || Statement {
            sql: "SELECT drained_through_position,drain_due_at FROM mail_workspace_drains WHERE workspace_id=?",
            args: vec![text(&workspace_id)],
        };
        let initial = runtime
            .runtime()
            .statement_host()
            .read(read_cursor())
            .await
            .unwrap();
        let SqlValue::Integer(initial_position) = &initial.rows[0][0] else {
            panic!("initial cursor")
        };
        let initial_position = *initial_position;
        assert_eq!(initial.rows[0][1], SqlValue::Null);
        for request_id in 7212..=7214 {
            let mut applied = runtime
                .runtime()
                .pause_points()
                .hold(&f.workspace, PausePoint::AfterStoreApplied);
            let pending = tokio::spawn({
                let runtime = runtime.clone();
                let caller = f.caller.clone();
                let workspace = f.workspace.clone();
                let operation = post(
                    &thread,
                    vec![m::Addressee::Actor {
                        actor_identity_id: actor.clone(),
                    }],
                );
                async move {
                    runtime
                        .request(
                            &caller,
                            m::WorkspaceRequest {
                                request_id: Some(id(request_id)),
                                workspace_id: Some(workspace),
                                operation,
                            },
                        )
                        .await
                }
            });
            applied.until_held().await;
            pending.abort();
            let _ = pending.await;
            drop(applied);
        }
        let changes = runtime.runtime().inspect_workspace(&f.workspace, Statement { sql: "SELECT change_id,position FROM workspace_changes WHERE position>? ORDER BY position", args: vec![SqlValue::Integer(initial_position)] }).await.unwrap();
        assert_eq!(changes.rows.len(), 3);
        for (offset, row) in changes.rows.iter().enumerate() {
            assert_eq!(
                row[1],
                SqlValue::Integer(
                    (initial_position.get() + i64::try_from(offset).unwrap() + 1)
                        .try_into()
                        .unwrap()
                )
            );
        }
        let ledger = runtime
            .runtime()
            .inspect_workspace(
                &f.workspace,
                Statement {
                    sql: "SELECT COUNT(*) FROM workspace_request_log WHERE request_id IN (?,?,?)",
                    args: vec![
                        text(id::<m::RequestId>(7212)),
                        text(id::<m::RequestId>(7213)),
                        text(id::<m::RequestId>(7214)),
                    ],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            ledger.rows,
            vec![vec![SqlValue::Integer(3.try_into().unwrap())]]
        );
        let before = runtime
            .runtime()
            .statement_host()
            .read(read_cursor())
            .await
            .unwrap();
        assert_eq!(before.rows[0][0], SqlValue::Integer(initial_position));
        assert!(matches!(&before.rows[0][1], SqlValue::Text(_)));
        let final_position = SqlValue::Integer((initial_position.get() + 3).try_into().unwrap());
        let mut held = runtime.runtime().pause_points().hold(&f.workspace, point);
        let pending = tokio::spawn({
            let runtime = runtime.clone();
            let workspace = f.workspace.clone();
            async move { runtime.drain_workspace(&workspace).await }
        });
        held.until_held().await;
        let read_messages = || Statement {
            sql: "SELECT i.change_id,w.recipient_actor FROM mail_notification_window_items i JOIN mail_notification_windows w ON w.window_id=i.window_id WHERE w.recipient_actor=? AND i.thread_id=?",
            args: vec![text(&actor), text(&thread)],
        };
        let cut_cursor = runtime
            .runtime()
            .statement_host()
            .read(read_cursor())
            .await
            .unwrap();
        let cut_messages = runtime
            .runtime()
            .statement_host()
            .read(read_messages())
            .await
            .unwrap();
        let before_commit = matches!(point, PausePoint::BeforeDrainCommit);
        if before_commit {
            assert_eq!(cut_cursor.rows, before.rows);
            assert!(cut_messages.rows.is_empty());
        } else {
            assert_eq!(
                cut_cursor.rows,
                vec![vec![final_position.clone(), SqlValue::Null]]
            );
            assert_eq!(cut_messages.rows.len(), 3);
            let mut actual: Vec<_> = cut_messages
                .rows
                .iter()
                .map(|row| (row[0].clone(), row[1].clone()))
                .collect();
            let mut expected: Vec<_> = changes
                .rows
                .iter()
                .map(|row| (row[0].clone(), text(&actor)))
                .collect();
            actual.sort_by_key(|entry| format!("{:?}", entry.0));
            expected.sort_by_key(|entry| format!("{:?}", entry.0));
            assert_eq!(actual, expected);
        }
        pending.abort();
        let _ = pending.await;
        drop(held);
        drop(runtime);
        f.reopen().await;
        let reopened = f.host.central(f.caller.organization_id()).await.unwrap();
        assert_eq!(
            reopened
                .runtime()
                .statement_host()
                .read(read_cursor())
                .await
                .unwrap()
                .rows,
            cut_cursor.rows
        );
        if before_commit {
            assert!(
                reopened
                    .runtime()
                    .statement_host()
                    .read(read_messages())
                    .await
                    .unwrap()
                    .rows
                    .is_empty()
            );
            let mut drained = reopened
                .runtime()
                .pause_points()
                .hold(&f.workspace, PausePoint::AfterDrainCommit);
            let physical_alarm = reopened.runtime().statement_host().alarm().unwrap();
            f.clock
                .not_due_check_now(f.clock.wall_deadline(physical_alarm))
                .await;
            f.advance_to(physical_alarm).await;
            drained.until_held().await;
            assert_eq!(
                reopened
                    .runtime()
                    .statement_host()
                    .read(read_cursor())
                    .await
                    .unwrap()
                    .rows,
                vec![vec![final_position.clone(), SqlValue::Null]]
            );
            drained.release();
        } else {
            assert_eq!(
                reopened
                    .runtime()
                    .statement_host()
                    .read(read_messages())
                    .await
                    .unwrap()
                    .rows,
                cut_messages.rows
            );
            let replay = reopened.drain_workspace(&f.workspace).await.unwrap();
            assert_eq!(replay.notices_queued, 0);
        }
        let rows = reopened
            .runtime()
            .statement_host()
            .read(read_messages())
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 3);
        // The recovered window is still pending; release it only at the original cadence mark.
        let due = reopened
            .runtime()
            .statement_host()
            .read(Statement {
                sql: "SELECT due_at FROM mail_notification_windows WHERE recipient_actor=?",
                args: vec![text(&actor)],
            })
            .await
            .unwrap();
        let SqlValue::Text(due) = &due.rows[0][0] else {
            panic!("window due")
        };
        let due: m::Timestamp = due.parse().unwrap();
        f.set_utc(due).await;
        reopened.after_event().await.unwrap();
        let batch = reopened
            .queued_for_session(&workspace_ids::SessionRef {
                session_id: "notice-observer".to_owned().try_into().unwrap(),
                ..session(&f.caller)
            })
            .await
            .unwrap();
        let encoded = serde_json::to_vec(&batch).unwrap();
        let batch: workspace_model::central_mail::SessionQueue =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let MessageBody::Notice { items } = &batch.messages[0].body else {
            panic!("notice body")
        };
        assert_eq!(items.len(), 3);
        let mut delivered = Vec::new();
        for item in items {
            assert_eq!(item.workspace_id, f.workspace);
            assert_eq!(item.thread_id.as_ref(), Some(&thread));
            delivered.push((item.position.get(), item.change_id.to_string()));
        }
        delivered.sort_by_key(|entry| entry.0);
        let expected: Vec<_> = changes
            .rows
            .iter()
            .map(|row| {
                let (SqlValue::Text(id), SqlValue::Integer(position)) = (&row[0], &row[1]) else {
                    panic!("change fields")
                };
                (u64::try_from(position.get()).unwrap(), id.clone())
            })
            .collect();
        assert_eq!(delivered, expected);
        assert_eq!(
            reopened
                .runtime()
                .statement_host()
                .read(read_cursor())
                .await
                .unwrap()
                .rows,
            vec![vec![final_position, SqlValue::Null]]
        );
        f.host.close().await;
    }
}
