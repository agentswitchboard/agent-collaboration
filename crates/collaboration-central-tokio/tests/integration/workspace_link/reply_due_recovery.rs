use super::*;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn a_running_reply_drain_can_be_cut_after_one_commit_and_recovered_either_way() -> Result<()>
{
    for recover_by_fresh_reply in [true, false] {
        let fixture = start_fixture().await?;
        let runtime = fixture.host.central(&fixture.organization_id).await?;
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
        let execution = runtime.runtime();
        let store = execution.statement_host();
        let mut committed = execution
            .pause_points()
            .hold_engine(PausePoint::AfterEngineCommit, &first.to_string());
        let mut waiting = execution
            .pause_points()
            .hold_engine(PausePoint::AfterEngineLoad, &tail.to_string());
        sign_in_digest(&fixture).await?;
        committed.until_held().await;
        waiting.until_held().await;
        let effects = |key| async move {
            store
                .read(Statement {
                    sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                    args: vec![SqlValue::Text(key)],
                })
                .await
                .map(|read| read.rows)
        };
        assert_eq!(
            effects(first.to_string()).await?,
            [vec![SqlValue::Integer(1.try_into()?)]]
        );
        assert_eq!(
            effects(tail.to_string()).await?,
            [vec![SqlValue::Integer(0.try_into()?)]]
        );
        execution.test_faults().cut_running_reply_drain();
        execution.until_reply_drains_settle().await;
        committed.release();
        waiting.release();
        assert_eq!(
            store.alarm().map(|at| at.unix_millis()),
            Some(due.timestamp_millis())
        );
        assert_eq!(
            effects(tail.to_string()).await?,
            [vec![SqlValue::Integer(0.try_into()?)]]
        );
        if recover_by_fresh_reply {
            sign_in_digest(&fixture).await?;
            execution.until_reply_drains_settle().await;
            assert!(fixture.clock.utc_now() < due);
        } else {
            let before = execution.alarm_completion_count();
            fixture.clock.advance(std::time::Duration::from_secs(5))?;
            execution.until_alarm_completed(before + 1).await;
        }
        for key in [first, tail] {
            assert_eq!(
                effects(key.to_string()).await?,
                [vec![SqlValue::Integer(1.try_into()?)]]
            );
        }
        assert!(store.alarm().is_none());
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
    use central_storage::{AlarmUpdate, GuardedStatement, RowExpectation, StatementBatch};

    let fixture = start_fixture().await?;
    let runtime = fixture.host.central(&fixture.organization_id).await?;
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
    let execution = runtime.runtime();
    let store = execution.statement_host();
    let mut held = execution
        .pause_points()
        .hold_central(PausePoint::BeforeDeferredDueWork);
    sign_in_digest(&fixture).await?;
    held.until_held().await;
    let alarm = store.alarm();
    let stamp = |value: String| async move {
        let result = store
            .run_batch(StatementBatch {
                statements: vec![GuardedStatement {
                    statement: Statement {
                        sql: "UPDATE signin_organization_identity SET id=?",
                        args: vec![SqlValue::Text(value)],
                    },
                    expect: RowExpectation::Exactly(1),
                }],
                alarm: AlarmUpdate::Unchanged,
            })
            .await?;
        assert!(matches!(result, BatchOutcome::Applied { .. }));
        Ok::<_, central_storage::StoreFailure>(())
    };
    stamp(super::super::fixture::OTHER_ORG.into()).await?;
    held.release();
    execution.until_reply_drains_settle().await;
    assert_eq!(store.alarm(), alarm);
    assert!(matches!(runtime.after_event().await,
        Err(fleet_contracts::AlarmNotPublished { failure: central_storage::StoreFailure::Corrupt {
            reason: central_storage::CorruptReason::BadValue { column },
        } }) if column == "signin_organization_identity.id"));
    stamp(fixture.organization_id.as_str().into()).await?;
    let facts = store.read(Statement {
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
    fixture.close().await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn a_competing_reply_at_the_fourth_pass_and_a_fresh_reply_each_take_the_tail() -> Result<()> {
    for accept_before_release in [true, false] {
        let fixture = start_fixture().await?;
        let runtime = fixture.host.central(&fixture.organization_id).await?;
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
        let store = runtime.runtime().statement_host();
        let held_key = format!("019f0000-0000-7000-8000-{:012}", 750);
        let mut fourth_pass = runtime
            .runtime()
            .pause_points()
            .hold_engine(PausePoint::AfterEngineLoad, &held_key);

        sign_in_digest(&fixture).await?;
        fourth_pass.until_held().await;
        let read_tail = || async {
            let rows = store.read(Statement {
                sql: "SELECT state FROM engine_probe_items WHERE item_id='019f0000-0000-7000-8000-000000001000'",
                args: vec![],
            }).await?.rows;
            Ok::<_, central_storage::StoreFailure>(rows)
        };
        assert_eq!(read_tail().await?, [vec![SqlValue::Text("waiting".into())]]);
        fixture.clock.advance(std::time::Duration::from_secs(2))?;
        if accept_before_release {
            sign_in_digest(&fixture).await?;
        }
        fourth_pass.release();
        if !accept_before_release {
            runtime.runtime().until_reply_drains_settle().await;
            assert_eq!(read_tail().await?, [vec![SqlValue::Text("waiting".into())]]);
            sign_in_digest(&fixture).await?;
        }
        runtime.runtime().until_reply_drains_settle().await;
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
