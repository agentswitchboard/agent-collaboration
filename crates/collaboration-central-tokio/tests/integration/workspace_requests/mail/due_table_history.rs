//! The owning request paths and the frozen queries agree after each generated step.
use super::super::support::*;
use super::engine_event_support::{at, due_batch_at, open_notice_window, set_wake};
use super::engine_probe_support::{integer, runtime};
use central_storage::{EngineAlarmTimes, SqlValue, Statement};
use fleet_contracts::mail_link::MailReceiptSettlement;
use fleet_runtime_ports::{CentralStore, DueListKind};
use hegel::{TestCase, generators as gs};
use router_clock::RuntimeClock;
use workspace_model::central_mail::{
    CentralMailAnswer as Answer, CentralMailOperation as Op, CentralMailRequest, CentralMailResult,
    MessageDelivery, ReceiptKind,
};

async fn request(fixture: &Fixture, number: u32, operation: Op) -> Answer {
    let caller = fixture.fresh_caller(&fixture.caller).await;
    let reply = fixture
        .host
        .request_central(
            &caller,
            CentralMailRequest {
                request_id: id(number),
                operation,
            },
        )
        .await
        .unwrap();
    match reply {
        CentralMailResult::Done(answer) => answer,
        other => panic!("history request {number}: {other:?}"),
    }
}

async fn assert_due_projection(fixture: &Fixture, stage: &str) {
    let runtime = runtime(fixture).await;
    let store = runtime.runtime().statement_host();
    let now = fixture.clock.utc_now();
    let horizon = now + chrono::Duration::hours(48);
    for (kind, sql) in [
        (
            DueListKind::Wakes,
            include_str!("../../../../../fleet-wakes/src/queries/read_due_wakes.sql"),
        ),
        (
            DueListKind::Schedules,
            include_str!("../../../../../fleet-schedules/src/queries/read_due_schedules.sql"),
        ),
        (
            DueListKind::DirectMessages,
            include_str!("../../../../../fleet-mail/src/queries/read_due_direct_messages.sql"),
        ),
        (
            DueListKind::SystemExpiries,
            include_str!("../../../../../fleet-mail/src/queries/read_due_system_expiries.sql"),
        ),
    ] {
        let expected = store
            .read(Statement {
                sql,
                args: vec![integer(horizon.timestamp_millis()), integer(250)],
            })
            .await
            .unwrap();
        let found = fleet_engine::read_due_rows(store, kind, horizon, 250)
            .await
            .unwrap_or_else(|_| panic!("{stage}: projected {kind:?} failed"));
        let found: Vec<_> = found
            .into_iter()
            .map(|row| {
                vec![
                    text(row.key.0),
                    integer(row.occurrence_ms),
                    integer(row.effective_due_ms),
                ]
            })
            .collect();
        assert_eq!(found, expected.rows, "{stage}: {kind:?}");
    }
    let read = store
        .read(Statement {
            sql: include_str!("../../../../../fleet-engine/src/queries/read_alarm_inputs.sql"),
            args: vec![],
        })
        .await
        .unwrap();
    let cell = |index| match read.rows[0][index] {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value.get()),
        ref other => panic!("{stage} invalid reference alarm cell: {other:?}"),
    };
    let reference = EngineAlarmTimes {
        earliest_on_grid_due_unix_millis: cell(0),
        earliest_legacy_due_unix_millis: cell(1),
    };
    let projected = store.engine_alarm_inputs().await.unwrap();
    assert_eq!(projected, reference, "{stage}: alarm inputs");
    let snapshot = runtime.runtime().due_batch_snapshot();
    let batch_state = fleet_runtime::DueBatchState::default();
    if let Some(batch_start) = snapshot.last_start_unix_millis {
        assert!(
            batch_state
                .try_claim(batch_start, chrono::Duration::seconds(30))
                .unwrap()
        );
        assert!(batch_state.finish(batch_start, snapshot.leftovers_pending));
    }
    let decide = fleet_runtime::engine_alarm_decider(std::sync::Arc::new(batch_state));
    let reference_alarm = decide(reference, now.timestamp_millis(), &mut |_| {});
    let projected_alarm = decide(projected, now.timestamp_millis(), &mut |_| {});
    assert_eq!(
        projected_alarm, reference_alarm,
        "{stage}: physical alarm decision"
    );
}

#[hegel::test(test_cases = 8, suppress_health_check = [hegel::HealthCheck::TooSlow])]
fn real_writes_and_dispatch_keep_the_due_projection_in_step(tc: TestCase) {
    let wake_minutes = tc.draw(gs::integers::<i64>().min_value(1).max_value(2)) * 5;
    let schedule_minutes = tc.draw(gs::integers::<i64>().min_value(1).max_value(2)) * 5;
    let initial_receipt = tc.draw(gs::booleans());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::new().await;
            let start = fixture.clock.utc_now();
            let wake = set_wake(&fixture, 19000, at(start, wake_minutes, 0, 0)).await;
            assert_due_projection(&fixture, "set wake").await;
            let schedule_operation = || Op::SetAutomation {
                target: session(&fixture.caller),
                timing: serde_json::from_value(serde_json::json!({
                    "kind": "after",
                    "seconds": schedule_minutes * 60,
                })).unwrap(),
                text: "history schedule".to_owned().try_into().unwrap(),
            };
            let schedule = request(&fixture, 19001, schedule_operation()).await;
            assert!(matches!(schedule, Answer::AutomationSet { .. }));
            assert_due_projection(&fixture, "set automation").await;

            let Answer::AutomationSet { automation_id, .. } =
                request(&fixture, 19004, schedule_operation()).await else {
                    panic!("schedule to disable admitted");
                };
            assert_due_projection(&fixture, "set automation to disable").await;
            assert!(matches!(
                request(&fixture, 19005, Op::DisableAutomation { automation_id }).await,
                Answer::AutomationDisabled { .. }
            ));
            assert_due_projection(&fixture, "schedule disabled").await;

            let send = || Op::SendMessage {
                to: session(&fixture.caller),
                body: "history direct".to_owned().try_into().unwrap(),
                delivery: MessageDelivery::Auto,
                reply_to: None,
                sent_at: start,
            };
            let Answer::Queued { message_id } = request(&fixture, 19002, send()).await else {
                panic!("direct send");
            };
            let central = runtime(&fixture).await;
            assert_due_projection(&fixture, "direct send").await;
            if initial_receipt {
                let received = ReceiptKind::Delivered {
                    arrived_at: start,
                };
                assert_eq!(
                    central
                        .apply_delivery_receipt(
                            &fixture.caller.connection().attachment(),
                            &message_id,
                            received
                        )
                        .await
                        .unwrap(),
                    MailReceiptSettlement::Applied,
                );
                assert_due_projection(&fixture, "direct Delivered").await;
            }

            open_notice_window(&fixture, 19010).await;
            assert_due_projection(&fixture, "workspace change and notice window").await;
            if wake_minutes != schedule_minutes {
                assert!(matches!(
                    request(&fixture, 19003, Op::CancelWake { wake_id: wake }).await,
                    Answer::WakeCancelled { .. }
                ));
                assert_due_projection(&fixture, "wake cancelled").await;
            }

            for (minute, label) in [
                (5, "first timer mark"),
                (10, "second timer mark"),
                (15, "window closes"),
                (20, "direct mark"),
            ] {
                due_batch_at(&fixture, &central, at(start, minute, 0, 0)).await;
                assert_due_projection(&fixture, label).await;
                if minute == 15 && initial_receipt {
                    let result = central.runtime().statement_host().read(Statement {
                        sql: "SELECT message_id FROM mail_messages WHERE message_kind='notice' AND state='queued' ORDER BY message_id LIMIT 1",
                        args: vec![],
                    }).await.unwrap();
                    let SqlValue::Text(notice) = &result.rows[0][0] else { panic!("notice id") };
                    let caller = fixture.fresh_caller(&fixture.caller).await;
                    assert_eq!(central.apply_delivery_receipt(
                        &caller.connection().attachment(),
                        &notice.parse().unwrap(),
                        ReceiptKind::Delivered { arrived_at: fixture.clock.utc_now() },
                    ).await.unwrap(), MailReceiptSettlement::Applied);
                    assert_due_projection(&fixture, "notice delivered").await;
                }
            }
            due_batch_at(&fixture, &central, at(start, 24 * 60 + 20, 0, 0)).await;
            assert_due_projection(&fixture, "system messages expire").await;
            fixture.host.close().await;
        });
}
