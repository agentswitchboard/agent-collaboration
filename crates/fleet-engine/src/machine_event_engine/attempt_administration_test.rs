use super::*;

#[test]
fn attempt_health_rejects_an_unknown_latest_failure_class() {
    let report = ReadOutcome {
        columns: ["oldest", "failing", "retired", "last_failure"]
            .map(str::to_owned)
            .to_vec(),
        rows: vec![],
    };
    let row = [
        SqlValue::Null,
        SqlValue::Integer(1.try_into().unwrap()),
        SqlValue::Integer(0.try_into().unwrap()),
        SqlValue::Text("unknown_failure".into()),
    ];
    assert_eq!(
        DecodedAttemptHealth::read_visible_health(&report, &row).err(),
        Some(corrupt("engine_work_attempts.last_failure"))
    );
}
