use super::*;
use crate::policy::policy_fixture::{NOW_MS, NOW_S, approved, enrolled, time};

#[test]
fn enrollment_lifecycle_reaches_its_edges_and_refuses_other_state_event_pairs() {
    let pending = approved(40, 1);
    let active = enrolled(40, 1, NOW_S);
    let client_id = active.ended_client().unwrap().client_id;
    let applying = time(NOW_MS + 14);
    let table = [
        (
            EnrollmentEvent::ApprovalDeadlineElapsed(applying),
            "approved",
            "lapsed",
        ),
        (
            EnrollmentEvent::FirstGrant {
                client_id,
                time: applying,
            },
            "approved",
            "enrolled",
        ),
        (
            EnrollmentEvent::ActiveGrant(applying),
            "enrolled",
            "enrolled",
        ),
        (
            EnrollmentEvent::OwnerRevoked(applying),
            "enrolled",
            "revoked",
        ),
        (EnrollmentEvent::Superseded(applying), "enrolled", "revoked"),
        (
            EnrollmentEvent::IdleDeadlineElapsed(applying),
            "enrolled",
            "expired",
        ),
    ];
    let terminal = [
        EnrollmentLifecycle::transition(&pending, table[0].0.clone())
            .unwrap()
            .0,
        EnrollmentLifecycle::transition(&active, table[3].0.clone())
            .unwrap()
            .0,
        EnrollmentLifecycle::transition(&active, table[5].0.clone())
            .unwrap()
            .0,
    ];
    let cases = [pending, active]
        .into_iter()
        .chain(terminal)
        .flat_map(|row| {
            table
                .iter()
                .map(move |(event, _, _)| (row.clone(), event.clone()))
        })
        .collect::<Vec<_>>();
    state_machine_core::check_state_machine_transitions::<EnrollmentLifecycle>(cases.clone())
        .unwrap();
    for ((row, event), (_, from, to)) in cases.into_iter().zip(table.iter().cycle()) {
        let next = EnrollmentLifecycle::transition(&row, event);
        if row.state_tag() == *from {
            let (next, _) = next.unwrap();
            assert_eq!(next.state_tag(), *to);
            assert_eq!(next.id, row.id);
            assert_eq!(next.request_id, row.request_id);
            assert_eq!(next.allowed_workspaces, row.allowed_workspaces);
            assert_eq!(
                next.ended_at_ms,
                next.is_terminal().then_some(applying.now_ms)
            );
        } else {
            assert!(matches!(next, Err(TransitionError::NotAllowed { .. })));
        }
    }
}
