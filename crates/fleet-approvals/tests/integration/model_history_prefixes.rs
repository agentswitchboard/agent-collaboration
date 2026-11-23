use super::{Actor, DeliverEndingReply, HOUR, Step, gs};

pub(super) fn apply_fixed_prefix(case: &hegel::TestCase, mut compare: impl FnMut(Step, i64)) {
    // Prefixes keep correlated delivery, visibility, and batch settlement reachable in short histories.
    let prefix = case.draw(gs::integers::<u8>().max_value(5));
    if prefix == 5 {
        compare(
            Step::Raise {
                row: 1,
                changed: false,
            },
            0,
        );
        compare(
            Step::Alarm {
                row: 1,
                stale_token: true,
            },
            HOUR,
        );
        compare(
            Step::Alarm {
                row: 1,
                stale_token: false,
            },
            HOUR,
        );
    }
    if (1..=3).contains(&prefix) {
        compare(
            Step::Raise {
                row: 0,
                changed: false,
            },
            0,
        );
        compare(
            Step::Raise {
                row: 1,
                changed: false,
            },
            0,
        );
    }
    if prefix == 2 {
        compare(
            Step::Forward {
                row: 0,
                actor: Actor::Parent,
            },
            1,
        );
        compare(
            Step::Decide {
                row: 0,
                operation: 101,
                choice: 2,
                actor: Actor::FirstPerson,
                fresh_factor: true,
            },
            2,
        );
        compare(
            Step::Decide {
                row: 1,
                operation: 101,
                choice: 2,
                actor: Actor::FirstPerson,
                fresh_factor: true,
            },
            3,
        );
        compare(
            Step::Read {
                row: 0,
                actor: Actor::FirstPerson,
            },
            3,
        );
        compare(
            Step::Read {
                row: 0,
                actor: Actor::SecondPerson,
            },
            3,
        );
        compare(
            Step::Deliver {
                row: 0,
                reply: DeliverEndingReply::NotHeld {},
                wrong_ending: false,
            },
            4,
        );
        compare(
            Step::Deliver {
                row: 0,
                reply: DeliverEndingReply::Received {},
                wrong_ending: false,
            },
            5,
        );
    } else if prefix == 3 {
        compare(
            Step::EndSession {
                enrollment: 0,
                parent: false,
            },
            1,
        );
        compare(Step::Register(1), 2);
        compare(
            Step::Raise {
                row: 0,
                changed: false,
            },
            3,
        );
        compare(Step::ReplaceEnrollment, 4);
        compare(Step::Register(2), 5);
        compare(
            Step::Raise {
                row: 3,
                changed: false,
            },
            6,
        );
        compare(Step::EndEnrollment(1), 7);
    }
    if prefix == 4 {
        compare(
            Step::EndSession {
                enrollment: 0,
                parent: true,
            },
            1,
        );
        compare(
            Step::Raise {
                row: 0,
                changed: false,
            },
            2,
        );
    }
    if prefix == 4 {
        compare(
            Step::Raise {
                row: 4,
                changed: false,
            },
            0,
        );
        compare(
            Step::Forward {
                row: 4,
                actor: Actor::Parent,
            },
            1,
        );
        for (operation, choice) in [(112, 1), (113, 2)] {
            compare(
                Step::Decide {
                    row: 4,
                    operation,
                    choice,
                    actor: Actor::FirstPerson,
                    fresh_factor: false,
                },
                1,
            );
        }
        compare(
            Step::Decide {
                row: 4,
                operation: 114,
                choice: if case.draw(gs::booleans()) { 1 } else { 2 },
                actor: Actor::FirstPerson,
                fresh_factor: true,
            },
            2,
        );
        compare(
            Step::Read {
                row: 4,
                actor: Actor::SecondPerson,
            },
            2,
        );
    }
}
