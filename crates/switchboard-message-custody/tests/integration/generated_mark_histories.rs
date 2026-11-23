//! Generated timelines exercise the custody worker, SQLite and the admitted lane boundary.

use hegel::{TestCase, generators};

use super::{
    claim_ack_drain::check_empty_retry_before_claim_recovery,
    claim_recovery::check_subsequent_claim_recovery,
    mark_drain::{
        check_closed_claim, check_rebased_admission, check_untouched_intake,
        check_waiting_read_fault,
    },
    repeated_welcome::check_repeated_welcome,
    write_cutoff_faults::{
        check_active_cutoff_fault, check_cutoff_during_orphan_probe, check_hidden_member_cutoff,
        check_worker_waiting_cutoff,
    },
};

// Keep one real SQLite history per generated input. Explicit examples pin
// every family, including the three interacting histories added by the review.
#[hegel::test(test_cases = 24)]
#[hegel::explicit_test_case(family = 0_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 1_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 2_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 3_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 4_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 5_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 6_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 7_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 8_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 9_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 10_u8, parameter = 0_u16)]
#[hegel::explicit_test_case(family = 11_u8, parameter = 0_u16)]
fn generated_mark_histories_preserve_cutoffs_receipts_and_request_identity(case: TestCase) {
    let family = case.draw(generators::integers::<u8>().min_value(0).max_value(11));
    let parameter = case.draw(generators::integers::<u16>().min_value(0).max_value(1_899));
    case.note(&format!("mark history {family}, parameter {parameter}"));
    let offset = u64::from(parameter);
    let count = 65 + parameter % 5;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match family {
                0 => check_hidden_member_cutoff(1 + offset % 900).await,
                1 => check_active_cutoff_fault(2 + u32::from(parameter % 999)).await,
                2 => check_untouched_intake(1 + parameter % 8).await,
                3 => check_waiting_read_fault(count).await,
                4 => check_rebased_admission(count).await,
                5 => check_empty_retry_before_claim_recovery(1_100 + i64::from(parameter)).await,
                6 => check_repeated_welcome(false, 1 + parameter % 4).await,
                7 => check_repeated_welcome(true, 1 + parameter % 4).await,
                8 => check_worker_waiting_cutoff(1 + u32::from(parameter % 1_000)).await,
                9 => check_cutoff_during_orphan_probe(1 + u32::from(parameter % 1_000)).await,
                10 => check_closed_claim(offset % 21).await,
                _ => check_subsequent_claim_recovery(count).await,
            }
        });
}
