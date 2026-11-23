#[test]
fn owner_deadline_reference_uses_the_checked_engine_row() {
    let _ = fleet_engine::check_timed_due_list!(ApprovalExpiries);
}
