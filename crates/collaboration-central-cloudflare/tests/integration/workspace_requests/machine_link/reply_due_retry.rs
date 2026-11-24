use super::*;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn exact_retry_marks_schedule_no_worker_task_or_timer_during_lookahead() -> Result<()> {
    use central_storage::{AlarmUpdate, GuardedStatement, RowExpectation, StatementBatch};
    use workspace_model::{
        Caller,
        central_mail::{CentralMailAnswer, CentralMailResult},
    };

    for source_control in [true, false] {
        let fixture = Fixture::new().await?;
        let runtime = fixture.runtime();
        let mut link = ConnectedLink::open(&fixture).await?;
        let start = fixture.clock.utc_now();
        let retry = central_engine_model::aligned_strictly_after(
            start + chrono::Duration::seconds(75),
            300,
        )
        .ok_or("fixture retry mark is out of range")?;
        let Caller::Session(target) = super::super::fixture::session_caller()? else {
            return Err("fixture caller must be a session".into());
        };
        let (_, replies) = link
            .batch(vec![RequestPayload::Mail(CentralMailRequest {
                request_id: "019f0000-0000-7000-8000-00000000db30".parse()?,
                operation: CentralMailOperation::SetWake {
                    target,
                    due_at: retry,
                    text: "exact retry witness".to_owned().try_into()?,
                },
            })])
            .await?;
        let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
            return Err("expected the wake creation reply".into());
        };
        let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::WakeSet {
            wake_id, ..
        })) = result.as_ref()
        else {
            return Err(format!("the wake was not created: {result:?}").into());
        };
        let subject = if source_control {
            "wake_timer"
        } else {
            wake_id.as_str()
        };
        let kind = if source_control {
            "engine_source"
        } else {
            "wake_timer"
        };
        let text = |value: &str| SqlValue::Text(value.to_owned());
        let retry_text = retry.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let start_text = start.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut statements = vec![GuardedStatement {
            statement: Statement {
                sql: "INSERT INTO engine_work_attempts VALUES ('019f0000-0000-7000-8000-00000000db31',?1,?2,'failing',1,?3,'statement_rejected',NULL,?4,?4,NULL,?4,?4)",
                args: vec![
                    text(kind),
                    text(subject),
                    text(&retry_text),
                    text(&start_text),
                ],
            },
            expect: RowExpectation::Exactly(1),
        }];
        if source_control {
            statements.push(GuardedStatement {
                statement: Statement {
                    sql: "INSERT INTO engine_due_entries VALUES ('019f0000-0000-7000-8000-00000000db32','wake_timer','source_control','wake_timer',NULL,NULL,?1,?2,?2)",
                    args: vec![SqlValue::Integer(retry.timestamp_millis().try_into()?), text(&start_text)],
                },
                expect: RowExpectation::Exactly(1),
            });
        }
        assert!(matches!(
            runtime
                .run_batch(StatementBatch {
                    statements,
                    alarm: AlarmUpdate::Unchanged,
                })
                .await?,
            BatchOutcome::Applied { .. }
        ));
        let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
        let before = retry - chrono::Duration::seconds(30);
        fixture.clock.advance((before - start).to_std()?)?;
        let _: bool = runtime
            .control("clock", &fixture.clock.utc_now().timestamp_millis())
            .await?;
        let log_start = fixture.host.captured_logs().len();

        sign_in_digest(&fixture).await?;

        assert_eq!(
            runtime
                .control::<(u64, u64)>("raw-reply-due-activity", &())
                .await?,
            activity
        );
        assert!(
            !fixture
                .host
                .captured_logs()
                .iter()
                .skip(log_start)
                .any(|line| line.contains("Central due yield")),
            "the pre-retry request created a worker timer"
        );
        assert_eq!(
            runtime
                .control::<Option<i64>>("raw-alarm-time", &())
                .await?,
            Some(retry.timestamp_millis())
        );
        fixture.clock.advance(std::time::Duration::from_secs(30))?;
        let _: bool = runtime
            .control("clock", &fixture.clock.utc_now().timestamp_millis())
            .await?;

        sign_in_digest(&fixture).await?;

        assert!(
            observe_event(|| async {
                Ok(runtime
                    .control::<(u64, u64)>("raw-reply-due-activity", &())
                    .await?
                    == (activity.0 + 1, 0))
            })
            .await?
        );
        let facts = runtime.read(Statement {
            sql: "SELECT state, (SELECT COUNT(*) FROM engine_work_attempts WHERE machine_kind=?1 AND subject_key=?2) FROM automation_wakes WHERE wake_id=?3",
            args: vec![text(kind), text(subject), text(wake_id.as_str())],
        }).await?.rows;
        assert_eq!(
            facts,
            [vec![text("done"), SqlValue::Integer(0.try_into()?)]]
        );
        assert_eq!(
            runtime
                .control::<Option<i64>>("raw-alarm-time", &())
                .await?,
            None
        );
        link.close().await?;
        fixture.close().await?;
    }
    Ok(())
}
