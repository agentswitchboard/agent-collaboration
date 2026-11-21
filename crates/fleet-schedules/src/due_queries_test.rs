#[test]
fn read_due_schedules_describes_the_owner_query() {
    let _ = fleet_engine::check_timed_due_list!(
        Schedules,
        central_engine_model::due_list_query(
            central_engine_model::DueListKind::Schedules,
            central_engine_model::EngineTime::UNIX_EPOCH,
            1
        )
        .unwrap()
    );
}
