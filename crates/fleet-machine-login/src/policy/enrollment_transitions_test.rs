use super::*;
use crate::policy::policy_fixture::{NOW_MS, NOW_S, approval, approved, enrolled, time};

#[test]
fn enrollment_lifecycle_reaches_its_edges_and_refuses_other_state_event_pairs() {
    let pending = approved(40, 1);
    let active = enrolled(40, 1, NOW_S);
    let client_id = active.ended_client().unwrap().client_id;
    let pending = EnrollmentLifecycleState::Stored(Box::new(pending));
    let active = EnrollmentLifecycleState::Stored(Box::new(active));
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
        (
            EnrollmentEvent::ReserveApproval(Box::new(ApprovalReservation {
                request: approval(),
                id: approved(40, 1).id,
                base_generation: 1_u64.try_into().unwrap(),
                time: applying,
            })),
            "absent",
            "approved",
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
    let cases = [EnrollmentLifecycleState::Absent, pending, active]
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
            let EnrollmentLifecycleState::Stored(next) = next else {
                panic!("a successful enrollment transition keeps its row");
            };
            if let EnrollmentLifecycleState::Stored(row) = row {
                assert_eq!(next.id, row.id);
                assert_eq!(next.request_id, row.request_id);
                assert_eq!(next.allowed_workspaces, row.allowed_workspaces);
            }
            assert_eq!(
                next.ended_at_ms,
                next.is_terminal().then_some(applying.now_ms)
            );
        } else {
            assert!(matches!(next, Err(TransitionError::NotAllowed { .. })));
        }
    }
}

#[test]
fn machine_lifecycle_reaches_every_edge_and_fences_generation() {
    use crate::policy::policy_fixture::{label, machine};
    let mut revoked = machine(2);
    let enrolled = MachineRow {
        state: MachineState::Enrolled,
        ..revoked.clone()
    };
    let events = [
        MachineEvent::Reserve {
            machine_id: revoked.machine_id.clone(),
            label: label("renamed"),
        },
        MachineEvent::FirstGrant {
            machine_id: revoked.machine_id.clone(),
            base_generation: revoked.generation,
        },
        MachineEvent::EndCurrent,
    ];
    let cases = [
        MachineObservation::Absent,
        MachineObservation::Stored(revoked.clone()),
        MachineObservation::Stored(enrolled),
    ]
    .into_iter()
    .flat_map(|observed| {
        events
            .iter()
            .map(move |event| (observed.clone(), event.clone()))
    });
    state_machine_core::check_state_machine_transitions::<MachineLifecycle>(cases).unwrap();
    assert!(matches!(
        MachineLifecycle::transition(
            &MachineObservation::Stored(revoked.clone()),
            MachineEvent::EndCurrent
        ),
        Err(TransitionError::NotAllowed { .. })
    ));
    revoked.generation = 3_u64.try_into().unwrap();
    assert!(matches!(
        MachineLifecycle::transition(&MachineObservation::Stored(revoked), events[1].clone()),
        Err(TransitionError::Domain(MachineRefusal::DifferentGeneration))
    ));
}

#[hegel::test(test_cases = 200)]
fn machine_generation_advances_for_each_first_grant_and_ending(case: hegel::TestCase) {
    use crate::policy::policy_fixture::{label, machine_id};
    use hegel::generators as gs;

    let mut observed = None;
    let mut expected_generation = None;
    let mut enrolled = false;
    let mut expected_label = label("initial");
    let operations = case.draw(gs::integers::<u8>().min_value(1).max_value(40));
    for step in 0..operations {
        let event = match case.draw(gs::integers::<u8>().max_value(2)) {
            0 => {
                expected_label = label(&format!("Label {step}"));
                MachineEvent::Reserve {
                    machine_id: machine_id(),
                    label: expected_label.clone(),
                }
            }
            1 => {
                let stale = case.draw(gs::booleans());
                let generation = expected_generation.unwrap_or(0) + u64::from(stale);
                MachineEvent::FirstGrant {
                    machine_id: machine_id(),
                    base_generation: generation.try_into().unwrap(),
                }
            }
            2 => MachineEvent::EndCurrent,
            _ => panic!("generator exceeded its alphabet"),
        };
        let applies = match &event {
            MachineEvent::Reserve { .. } => {
                expected_generation.get_or_insert(0);
                true
            }
            MachineEvent::FirstGrant {
                base_generation, ..
            } if expected_generation == Some(base_generation.get()) => {
                expected_generation = expected_generation.map(|generation| generation + 1);
                enrolled = true;
                true
            }
            MachineEvent::EndCurrent if enrolled => {
                expected_generation = expected_generation.map(|generation| generation + 1);
                enrolled = false;
                true
            }
            MachineEvent::FirstGrant { .. } | MachineEvent::EndCurrent => false,
        };
        let next = next_machine(observed.as_ref(), event);
        assert_eq!(next.is_ok(), applies);
        if let Ok(next) = next {
            observed = Some(next);
        }
        assert_eq!(
            observed.as_ref().map(|row| row.generation.get()),
            expected_generation
        );
        assert_eq!(
            observed
                .as_ref()
                .is_some_and(|row| row.state == MachineState::Enrolled),
            enrolled
        );
        if let Some(row) = &observed {
            assert_eq!(row.machine_id, machine_id());
            assert_eq!(row.label, expected_label);
        }
    }
}
