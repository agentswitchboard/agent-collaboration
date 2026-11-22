use super::super::support::*;
use super::direct_receipt_sequences::{assert_store, queue_direct};
use super::engine_probe_support::{integer, raw, runtime};
use central_storage::Statement;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn one_direct_mark_guard_reads_bounded_rows_as_pending_marks_grow() {
    const SEED_PENDING_MARKS: &str =
        include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_seed.sql");
    let mut measured_reads = Vec::new();
    for pending_marks in [20_i64, 10_000] {
        let fixture = Fixture::new().await;
        let central = runtime(&fixture).await;
        let sent_at = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
        let mark = fleet_mail::message_deadline::direct_receipt_mark(sent_at, sent_at);
        let message_id = queue_direct(&fixture, sent_at, 981004).await;
        raw(
            &central,
            SEED_PENDING_MARKS,
            vec![integer(pending_marks), text(&message_id)],
        )
        .await;
        assert_eq!(
            central
                .read(Statement {
                    sql: "SELECT count(*) FROM mail_messages WHERE message_kind='direct' AND cancel_due_at IS NOT NULL",
                    args: vec![],
                })
                .await
                .unwrap()
                .rows,
            [vec![integer(pending_marks)]]
        );
        let unarmed: Option<Vec<u64>> = central
            .control("direct-mark-guard-read-observations", &true)
            .await
            .unwrap();
        assert_eq!(unarmed, None);
        fixture
            .clock
            .0
            .store(mark.timestamp_millis(), Ordering::SeqCst);
        central.after_event().await.unwrap();
        let observed: Option<Vec<u64>> = central
            .control("direct-mark-guard-read-observations", &false)
            .await
            .unwrap();
        let Some([rows_read]) = observed.as_deref() else {
            panic!("expected exactly one direct mark guard read, got {observed:?}");
        };
        println!("direct mark guard: {pending_marks} marks, {rows_read} Workerd rows read");
        measured_reads.push(*rows_read);
        assert_store(
            &central,
            &message_id,
            "pastTheMark",
            Some("deliveryNotConfirmed"),
        )
        .await;
        fixture.host.close().await;
    }
    assert_eq!(measured_reads[0], measured_reads[1]);
    assert!(
        (1..=4).contains(&measured_reads[0]),
        "guard rows read: {measured_reads:?}"
    );
}

#[tokio::test]
async fn the_direct_mark_guard_matches_the_due_view_on_workerd() {
    const GUARD: &str =
        include_str!("../../../../../fleet-mail/src/queries/read_due_mark_guard.sql");
    const BASELINE_GUARD: &str =
        include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_baseline.sql");
    const SEED_CASE_MESSAGES: &str = include_str!(
        "../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_messages.sql"
    );
    const SEED_CASE_ATTEMPTS: &str = include_str!(
        "../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_attempts.sql"
    );
    const SEED_SOURCE_HOLD: &str = include_str!(
        "../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_source_hold.sql"
    );
    const PLAN: &str = concat!(
        "EXPLAIN QUERY PLAN ",
        include_str!("../../../../../fleet-mail/src/queries/read_due_mark_guard.sql")
    );
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let sent_at = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let template = queue_direct(&fixture, sent_at, 981005).await;
    raw(&central, SEED_CASE_MESSAGES, vec![text(&template)]).await;
    raw(&central, SEED_CASE_ATTEMPTS, vec![]).await;

    let plan = central
        .read(Statement {
            sql: PLAN,
            args: vec![text("019d0000-0000-7000-8000-000000000001")],
        })
        .await
        .unwrap();
    println!("direct mark guard Workerd plan: {:?}", plan.rows);
    let cases = [
        Some((60_000, 60_000)),
        None,
        Some((120_001, 120_001)),
        Some((0, 0)),
        Some((60_000, 180_000)),
        None,
        None,
        None,
        Some((60_000, 0)),
        None,
    ];
    for (index, expected) in cases.into_iter().enumerate() {
        let subject = format!("019d0000-0000-7000-8000-{:012}", index + 1);
        let expected_rows: Vec<_> = expected
            .into_iter()
            .map(|(occurrence, due)| vec![integer(occurrence), integer(due)])
            .collect();
        for statement in [BASELINE_GUARD, GUARD] {
            let rows = central
                .read(Statement {
                    sql: statement,
                    args: vec![text(&subject)],
                })
                .await
                .unwrap()
                .rows;
            assert_eq!(rows, expected_rows, "guard case {}", index + 1);
        }
    }
    raw(&central, SEED_SOURCE_HOLD, vec![]).await;
    for statement in [BASELINE_GUARD, GUARD] {
        let rows = central
            .read(Statement {
                sql: statement,
                args: vec![text(&template)],
            })
            .await
            .unwrap()
            .rows;
        assert!(rows.is_empty(), "the source hold must mask the subject");
    }
    fixture.host.close().await;
}
