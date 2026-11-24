use super::*;
use central_storage::SqlValue;

#[test]
fn attempt_health_rejects_an_unknown_latest_failure_class() {
    let report = ReadOutcome {
        columns: [
            "machine_kind",
            "oldest",
            "failing",
            "retired",
            "last_failure",
        ]
        .map(str::to_owned)
        .to_vec(),
        rows: vec![],
    };
    let row = [
        SqlValue::Text("wake_timer".into()),
        SqlValue::Null,
        SqlValue::Integer(1.try_into().unwrap()),
        SqlValue::Integer(0.try_into().unwrap()),
        SqlValue::Text("unknown_failure".into()),
    ];
    assert_eq!(
        AttemptHealthProjection::read_named(&report, &row)
            .unwrap()
            .decode_visible_health()
            .err(),
        Some(corrupt("engine_work_attempts.last_failure"))
    );
}
