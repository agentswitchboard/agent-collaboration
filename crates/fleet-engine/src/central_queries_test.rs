use super::StoredAlarmInputs;

#[test]
fn aggregate_sql_describes_the_rows_executed_by_both_hosts() {
    let _ = sqlx::query_file_as!(StoredAlarmInputs, "src/queries/read_alarm_inputs.sql");
}

#[test]
fn indexed_queries_describe_the_shipped_rows() {
    macro_rules! check_alarm {
        ($file:literal, $tags:expr) => {
            sqlx::query_file_as!(super::StoredIndexedAlarmInputs, $file, $tags)
        };
    }
    let _ = indexed_alarm_query!(check_alarm, super::grid_tags().unwrap());
    macro_rules! check_due {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file_as!(super::StoredDueRow, $file, $($arg),+) };
    }
    let _ = projected_due_query!(
        check_due,
        crate::DueKind::WakeTimer.tag().to_owned(),
        0_i64,
        1_i64
    );
}
