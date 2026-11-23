use super::super::{
    Statement,
    support::{Fixture, id, session, text},
};
use super::engine_event_support::{at, count, driver_fires_at, ms, stamp};
use super::engine_probe_support::{alarm_ms, runtime};
use central_storage::SqlValue;
use router_clock::RuntimeClock;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
};

#[tokio::test]
async fn live_capped_schedule_queues_each_occurrence_once() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    fixture.set_utc(at(start, 0, 1, 0)).await;
    let first_mark = at(start, 5, 0, 0);
    let first_tail = at(start, 5, 30, 0);
    let second_mark = at(start, 10, 0, 0);
    let second_tail = at(start, 10, 30, 0);
    let third_mark = at(start, 15, 0, 0);

    let mut automation_ids = Vec::new();
    for index in 0..251 {
        let result = fixture
            .host
            .request_central(
                &fixture.caller,
                CentralMailRequest {
                    request_id: id(49000 + index),
                    operation: CentralMailOperation::SetAutomation {
                        target: session(&fixture.caller),
                        timing: serde_json::from_str(r#"{"kind":"interval","seconds":300}"#)
                            .unwrap(),
                        text: "five-minute recurrence".to_owned().try_into().unwrap(),
                    },
                },
            )
            .await
            .unwrap();
        let CentralMailResult::Done(CentralMailAnswer::AutomationSet {
            automation_id,
            next_due_at,
            ..
        }) = result
        else {
            panic!("schedule creation failed: {result:?}");
        };
        assert_eq!(next_due_at, Some(first_mark));
        automation_ids.push(automation_id.to_string());
    }
    let tail = automation_ids.iter().max().unwrap();

    for (alarm, total, through, next_due, tail_messages) in [
        (first_mark, 250, SqlValue::Null, first_mark, vec![]),
        (
            first_tail,
            251,
            stamp(first_tail),
            second_mark,
            vec![vec![stamp(first_tail)]],
        ),
        (
            second_mark,
            501,
            stamp(first_tail),
            second_mark,
            vec![vec![stamp(first_tail)]],
        ),
        (
            second_tail,
            502,
            stamp(second_tail),
            third_mark,
            vec![vec![stamp(first_tail)], vec![stamp(second_tail)]],
        ),
    ] {
        driver_fires_at(&fixture, &runtime, alarm).await;
        assert_eq!(
            count(
                &runtime,
                "SELECT COUNT(*) FROM mail_messages WHERE message_kind = 'automation'",
                vec![],
            )
            .await,
            total,
            "each mark applies the first 250 and each continuation applies the tail"
        );
        let schedule = runtime
            .runtime().statement_host()
            .read(Statement {
                sql: "SELECT evaluated_through_at, next_due_at FROM automation_schedules WHERE automation_id = ?",
                args: vec![text(tail)],
            })
            .await
            .unwrap();
        assert_eq!(schedule.rows, vec![vec![through, stamp(next_due)]]);
        let messages = runtime
            .runtime().statement_host()
            .read(Statement {
                sql: "SELECT created_at FROM mail_messages WHERE message_kind = 'automation' AND state = 'queued' AND json_extract(body, '$.automationId') = ? ORDER BY created_at",
                args: vec![text(tail)],
            })
            .await
            .unwrap();
        assert_eq!(messages.rows, tail_messages);
    }
    assert_eq!(alarm_ms(&runtime), ms(third_mark));
    fixture.host.close().await;
}
