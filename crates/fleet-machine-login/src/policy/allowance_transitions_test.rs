use super::*;

#[test]
fn allowance_changes_follow_the_pair_transition_table() {
    use AllowanceEvent::{ApprovalSelectedWorkspace, EnrollmentEnded};
    use AllowanceStatus::{Absent, Active, Withdrawn};

    let approved = Some(MachineAllowanceState::Active);
    let withdrawn = Some(MachineAllowanceState::Withdrawn);
    let cases = [
        (Absent, ApprovalSelectedWorkspace, approved),
        (Active, ApprovalSelectedWorkspace, None),
        (Withdrawn, ApprovalSelectedWorkspace, approved),
        (Absent, EnrollmentEnded, None),
        (Active, EnrollmentEnded, withdrawn),
        (Withdrawn, EnrollmentEnded, None),
    ];
    state_machine_core::check_state_machine_transitions::<AllowanceLifecycle>(
        cases.map(|(starting, event, _)| (starting, event)),
    )
    .unwrap();
    for (starting, event, effect) in cases {
        assert_eq!(allowance_effect(starting, event).unwrap(), effect);
    }
}
