use super::*;
use crate::policy::policy_fixture::allowance;
use hegel::generators as gs;

#[test]
fn allowance_changes_follow_the_pair_transition_table() {
    use AllowanceEvent::{ApprovalSelectedWorkspace, EnrollmentEnded};
    use AllowanceStatus::{Absent, Active, Withdrawn};

    let active = allowance(50, StoredAllowanceState::Active);
    let withdrawn = allowance(50, StoredAllowanceState::Withdrawn);
    let cases = [
        (
            Absent,
            None,
            ApprovalSelectedWorkspace,
            Ok(Some(AllowanceEffect::Insert)),
        ),
        (Active, Some(&active), ApprovalSelectedWorkspace, Ok(None)),
        (
            Withdrawn,
            Some(&withdrawn),
            ApprovalSelectedWorkspace,
            Ok(Some(AllowanceEffect::Reactivate(withdrawn.id))),
        ),
        (
            Absent,
            None,
            EnrollmentEnded,
            Err(StoreFailure::Unavailable),
        ),
        (
            Active,
            Some(&active),
            EnrollmentEnded,
            Ok(Some(AllowanceEffect::Withdraw(active.id))),
        ),
        (Withdrawn, Some(&withdrawn), EnrollmentEnded, Ok(None)),
    ];
    state_machine_core::check_state_machine_transitions::<AllowanceLifecycle>(
        cases
            .iter()
            .map(|&(starting, _, event, _)| (starting, event)),
    )
    .unwrap();
    for (_, observed, event, effect) in cases {
        assert_eq!(allowance_effect(observed, event), effect);
    }
}

#[hegel::test(test_cases = 200)]
fn allowance_pair_changes_follow_insert_reactivate_withdraw_and_hold_over_traces(
    case: hegel::TestCase,
) {
    let mut observed = None;
    let mut recorded = false;
    let mut permitted = false;
    let new_pair = allowance(51, StoredAllowanceState::Active);
    let operations = case.draw(gs::integers::<u8>().min_value(1).max_value(40));
    for _ in 0..operations {
        if case.draw(gs::booleans()) {
            let expected = if permitted {
                None
            } else if recorded {
                Some(AllowanceEffect::Reactivate(new_pair.id))
            } else {
                Some(AllowanceEffect::Insert)
            };
            let effect =
                allowance_effect(observed.as_ref(), AllowanceEvent::ApprovalSelectedWorkspace)
                    .unwrap();
            assert_eq!(effect, expected);
            if effect.is_some() {
                observed = Some(new_pair.clone());
            }
            recorded = true;
            permitted = true;
        } else if recorded {
            let expected = permitted.then_some(AllowanceEffect::Withdraw(new_pair.id));
            let effect =
                allowance_effect(observed.as_ref(), AllowanceEvent::EnrollmentEnded).unwrap();
            assert_eq!(effect, expected);
            if effect.is_some() {
                observed = Some(StoredAllowance {
                    state: StoredAllowanceState::Withdrawn,
                    ..new_pair.clone()
                });
            }
            permitted = false;
        } else {
            // An ending only enumerates persisted rows.
            assert!(observed.is_none());
        }
        assert_eq!(
            observed.as_ref().map(|pair| pair.id),
            recorded.then_some(new_pair.id)
        );
        assert_eq!(
            observed
                .as_ref()
                .is_some_and(|pair| pair.state == StoredAllowanceState::Active),
            permitted
        );
    }
}
