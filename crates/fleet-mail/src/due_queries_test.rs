#[test]
fn read_due_direct_messages_describes_the_owner_query() {
    let _ = sqlx::query_file!("src/queries/read_due_direct_messages.sql", 0_i64, 1_i64);
}

#[test]
fn read_due_system_expiries_describes_the_owner_query() {
    let _ = sqlx::query_file!("src/queries/read_due_system_expiries.sql", 0_i64, 1_i64);
}
