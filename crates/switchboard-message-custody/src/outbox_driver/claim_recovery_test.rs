use super::{ClaimRecovery, ClaimRecoveryEvent};
use hegel::{TestCase, generators};
use state_machine_core::check_state_machine_transitions;

use ClaimRecovery::{Clear, ParkedForReturn, ParkedForScan, ScanOwed};
use ClaimRecoveryEvent::{ClaimEmpty, ClaimFailed, DrainReset, LocalRetryFinished, ScanFinished};

const PHASES: [ClaimRecovery; 4] = [Clear, ScanOwed, ParkedForScan, ParkedForReturn];
const EVENTS: [ClaimRecoveryEvent; 7] = [
    ClaimFailed,
    ClaimEmpty,
    ScanFinished {
        owes_local_return: false,
    },
    ScanFinished {
        owes_local_return: true,
    },
    LocalRetryFinished {
        owes_local_return: false,
    },
    LocalRetryFinished {
        owes_local_return: true,
    },
    DrainReset,
];

#[test]
fn claim_recovery_table_and_reducer_agree_in_both_directions() {
    let witnesses = PHASES
        .into_iter()
        .flat_map(|phase| EVENTS.map(|event| (phase, event)));
    check_state_machine_transitions::<ClaimRecovery>(witnesses).unwrap();
}

#[hegel::test(test_cases = 80)]
#[hegel::explicit_test_case(steps = vec![0_usize, 1, 3, 2, 5, 4, 6])]
fn generated_claim_recovery_observations_keep_the_gate_for_the_right_evidence(case: TestCase) {
    // The baseline's next phase by prior phase and observation. The
    // source table groups outcomes by event tag, while this oracle
    // distinguishes both payloads of each completed observation.
    const NEXT: [[usize; 7]; 4] = [
        [1, 0, 0, 0, 0, 0, 0],
        [1, 2, 0, 3, 1, 1, 0],
        [1, 2, 0, 3, 2, 2, 0],
        [1, 3, 3, 3, 0, 3, 0],
    ];
    let steps = case.draw(
        generators::vecs(generators::integers::<usize>().max_value(EVENTS.len() - 1)).max_size(40),
    );
    let mut phase = Clear;
    let mut expected = 0;
    for step in steps {
        let event = EVENTS[step];
        expected = NEXT[expected][step];
        phase.observe(event);
        assert_eq!(phase, PHASES[expected], "after {event:?}");
    }
}
