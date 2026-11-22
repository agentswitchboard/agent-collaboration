use super::*;

#[test]
fn the_transactional_attempt_reads_and_writes_fit_the_central_schema() {
    let subject = EngineSubject {
        kind: central_engine_model::MachineKind("engine_fixture"),
        key: central_engine_model::InstanceKey("fixture".to_owned()),
    };
    let now = "2030-01-01T00:00:00Z".parse().unwrap();
    let FailureTransition::Record(record) = after_failure(
        None,
        ItemFailureClass::StatementRejected,
        now,
        RetirementThreshold::DEFAULT,
        "019f0000-0000-7000-8000-000000000006".parse().unwrap(),
    )
    .unwrap() else {
        panic!("first attempt must be written");
    };
    macro_rules! read {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file_as!(StoredCheckedAttempt, $file, $($arg),+) };
    }
    macro_rules! write {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file!($file, $($arg),+) };
    }
    let _ = attempt_read!(read, subject);
    let _ = attempt_write!(
        write,
        subject,
        record,
        "failing",
        Some(format_utc_millis(now)),
        None::<String>
    );
}
