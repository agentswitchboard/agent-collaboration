use super::StoredAlarmInputs;

#[test]
fn aggregate_sql_describes_the_rows_executed_by_both_hosts() {
    let _ = sqlx::query_file_as!(StoredAlarmInputs, "src/queries/read_alarm_inputs.sql");
}
