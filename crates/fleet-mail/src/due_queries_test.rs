#[test]
fn read_due_direct_messages_describes_the_owner_query() {
    let _ = fleet_engine::check_timed_due_list!(
        DirectMessages,
        central_engine_model::due_list_query(
            central_engine_model::DueListKind::DirectMessages,
            central_engine_model::EngineTime::UNIX_EPOCH,
            1
        )
        .unwrap()
    );
}

#[test]
fn read_due_system_expiries_describes_the_owner_query() {
    let _ = fleet_engine::check_timed_due_list!(
        SystemExpiries,
        central_engine_model::due_list_query(
            central_engine_model::DueListKind::SystemExpiries,
            central_engine_model::EngineTime::UNIX_EPOCH,
            1
        )
        .unwrap()
    );
}
