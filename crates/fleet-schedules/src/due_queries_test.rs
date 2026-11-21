#[test]
fn read_due_schedules_describes_the_owner_query() {
    let _ = sqlx::query_file!("src/queries/read_due_schedules.sql", 0_i64, 1_i64);
}
