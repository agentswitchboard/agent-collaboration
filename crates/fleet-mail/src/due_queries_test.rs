#[test]
fn read_due_direct_messages_describes_the_owner_query() {
    let _ = fleet_engine::check_timed_due_list!(DirectMessages);
}

#[test]
fn read_due_system_expiries_describes_the_owner_query() {
    let _ = fleet_engine::check_timed_due_list!(SystemExpiries);
}
