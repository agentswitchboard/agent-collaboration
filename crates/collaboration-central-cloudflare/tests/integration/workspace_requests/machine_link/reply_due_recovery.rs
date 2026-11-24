use super::*;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn a_running_reply_drain_can_be_cut_after_one_commit_and_recovered_either_way() -> Result<()>
{
    for recover_by_fresh_reply in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime();
        let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
        let first: uuid::Uuid = "019f0000-0000-7000-8000-00000000db21".parse()?;
        let tail: uuid::Uuid = "019f0000-0000-7000-8000-00000000db22".parse()?;
        for key in [first, tail] {
            runtime
                .seed_probe(SeedProbe {
                    key,
                    group: key,
                    due: Some(due),
                    claim: key.to_string(),
                })
                .await?;
        }
        let committed = runtime
            .hold_engine("afterEngineCommit", &first.to_string())
            .await;
        let waiting = runtime
            .hold_engine("afterEngineLoad", &tail.to_string())
            .await;
        let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
        sign_in_digest(&fixture).await?;
        runtime.until_held_engine(committed).await;
        runtime.until_held_engine(waiting).await;
        let effects = |key| {
            let runtime = runtime.clone();
            async move {
                runtime
                    .read(Statement {
                        sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                        args: vec![SqlValue::Text(key)],
                    })
                    .await
                    .map(|read| read.rows)
            }
        };
        assert_eq!(
            effects(first.to_string()).await?,
            [vec![SqlValue::Integer(1.try_into()?)]]
        );
        assert_eq!(
            effects(tail.to_string()).await?,
            [vec![SqlValue::Integer(0.try_into()?)]]
        );
        let _: bool = runtime.control("cut-running-reply-due-work", &()).await?;
        assert!(
            observe_event(|| async {
                Ok(runtime
                    .control::<(u64, u64)>("raw-reply-due-activity", &())
                    .await?
                    == (activity.0 + 1, 0))
            })
            .await?
        );
        runtime.release_engine(committed).await;
        runtime.release_engine(waiting).await;
        assert_eq!(
            runtime
                .control::<Option<i64>>("raw-alarm-time", &())
                .await?,
            Some(due.timestamp_millis())
        );
        assert_eq!(
            effects(tail.to_string()).await?,
            [vec![SqlValue::Integer(0.try_into()?)]]
        );
        if recover_by_fresh_reply {
            sign_in_digest(&fixture).await?;
            assert!(
                observe_event(|| async {
                    Ok(runtime
                        .control::<(u64, u64)>("raw-reply-due-activity", &())
                        .await?
                        == (activity.0 + 2, 0))
                })
                .await?
            );
            assert!(fixture.clock.utc_now() < due);
        } else {
            fixture.clock.advance(std::time::Duration::from_secs(5))?;
            assert!(runtime.dispatch_due_alarm().await?.is_some());
        }
        for key in [first, tail] {
            assert_eq!(
                effects(key.to_string()).await?,
                [vec![SqlValue::Integer(1.try_into()?)]]
            );
        }
        assert_eq!(
            runtime
                .control::<Option<i64>>("raw-alarm-time", &())
                .await?,
            None
        );
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn the_deferred_drain_refuses_a_stamp_changed_after_the_reply() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime();
    let key: uuid::Uuid = "019f0000-0000-7000-8000-00000000db23".parse()?;
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    runtime
        .seed_probe(SeedProbe {
            key,
            group: key,
            due: Some(due),
            claim: "stamp witness".into(),
        })
        .await?;
    let identity = runtime
        .read(Statement {
            sql: "SELECT organization_statement FROM signin_organization_identity",
            args: vec![],
        })
        .await?
        .rows;
    let [[SqlValue::Text(statement)]] = identity
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>()
        .as_slice()
    else {
        return Err("fixture has no signed statement".into());
    };
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let _: bool = runtime.control("hold-central", &BEFORE_DEFERRED).await?;
    sign_in_digest(&fixture).await?;
    let _: bool = runtime
        .control("until-held-central", &BEFORE_DEFERRED)
        .await?;
    let _: std::result::Result<BatchOutcome, central_storage::StoreFailure> = runtime
        .control(
            "raw-set-stamps",
            &vec![("019f0000-0000-7000-8000-00000000db29", statement.as_str())],
        )
        .await?;
    let _: bool = runtime
        .control("raw-release-deferred-due-work", &())
        .await?;
    assert!(
        observe_event(|| async {
            Ok(runtime
                .control::<(u64, u64)>("raw-reply-due-activity", &())
                .await?
                == (activity.0 + 1, 0))
        })
        .await?
    );
    assert_eq!(
        runtime
            .control::<Option<i64>>("raw-alarm-time", &())
            .await?,
        Some(due.timestamp_millis())
    );
    let _: std::result::Result<BatchOutcome, central_storage::StoreFailure> = runtime
        .control(
            "raw-set-stamps",
            &vec![(fixture.organization.as_str(), statement.as_str())],
        )
        .await?;
    let facts = runtime.read(Statement {
        sql: "SELECT (SELECT state FROM engine_probe_items WHERE item_id=?1), (SELECT effect_count FROM engine_probe_groups WHERE group_id=?1), (SELECT COUNT(*) FROM engine_work_attempts WHERE subject_key=?1)",
        args: vec![SqlValue::Text(key.to_string())],
    }).await?.rows;
    assert_eq!(
        facts,
        [vec![
            SqlValue::Text("waiting".into()),
            SqlValue::Integer(0.try_into()?),
            SqlValue::Integer(0.try_into()?)
        ]]
    );
    fixture.close().await
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn a_competing_reply_at_the_fourth_pass_and_a_fresh_reply_each_take_the_tail() -> Result<()> {
    for accept_before_release in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime();
        let now = fixture.clock.utc_now();
        let due = now + chrono::Duration::seconds(5);
        let group: uuid::Uuid = "019f0000-0000-7000-8000-00000000dbee".parse()?;
        for number in 0..=1000 {
            let key: uuid::Uuid = format!("019f0000-0000-7000-8000-{number:012}").parse()?;
            let due = if number == 1000 {
                now + chrono::Duration::seconds(51)
            } else {
                due
            };
            assert!(matches!(
                runtime
                    .seed_probe(SeedProbe {
                        key,
                        group,
                        due: Some(due),
                        claim: key.to_string(),
                    })
                    .await?,
                BatchOutcome::Applied { .. }
            ));
        }
        let store = &*runtime;
        let held_key = format!("019f0000-0000-7000-8000-{:012}", 750);
        let fourth_pass = runtime.hold_engine("afterEngineLoad", &held_key).await;
        let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
        sign_in_digest(&fixture).await?;
        runtime.until_held_engine(fourth_pass).await;
        let read_tail = || async {
            let rows = store.read(Statement {
                sql: "SELECT state FROM engine_probe_items WHERE item_id='019f0000-0000-7000-8000-000000001000'",
                args: vec![],
            }).await?.rows;
            Ok::<_, central_storage::StoreFailure>(rows)
        };
        assert_eq!(read_tail().await?, [vec![SqlValue::Text("waiting".into())]]);
        fixture.clock.advance(std::time::Duration::from_secs(2))?;
        let _: bool = runtime
            .control("clock", &fixture.clock.utc_now().timestamp_millis())
            .await?;
        if accept_before_release {
            sign_in_digest(&fixture).await?;
        }
        runtime.release_engine(fourth_pass).await;
        if !accept_before_release {
            assert!(
                observe_event(|| async {
                    let receipt: (u64, u64) =
                        runtime.control("raw-reply-due-activity", &()).await?;
                    Ok(receipt == (activity.0 + 1, 0))
                })
                .await?
            );
            assert_eq!(read_tail().await?, [vec![SqlValue::Text("waiting".into())]]);
            sign_in_digest(&fixture).await?;
        }
        assert!(
            observe_event(|| async {
                let receipt: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
                Ok(receipt == (activity.0 + 2, 0))
            })
            .await?
        );
        assert_eq!(read_tail().await?, [vec![SqlValue::Text("fired".into())]]);
        let effects = store
            .read(Statement {
                sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                args: vec![SqlValue::Text(group.to_string())],
            })
            .await?
            .rows;
        assert_eq!(effects, [vec![SqlValue::Integer(1001.try_into()?)]]);
        assert!(fixture.clock.utc_now() < due);
        fixture.close().await?;
    }
    Ok(())
}
