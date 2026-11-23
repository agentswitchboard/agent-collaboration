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

#[derive(Debug)]
enum MarkHistory {
    HiddenCheckedReply(u64),
    IndependentCutoffFault(u32),
    UntouchedLaneRefusal(u16),
    ResidualReadFault(u16),
    AdmissionAfterRebase(u16),
    EmptyWakeBeforeClaimRecovery(i64),
    RepeatedWelcome {
        visited_named_mark: bool,
        older_suffix: u16,
    },
    WaitingCutoffDuringHeldFrame(u32),
    OrphanProbeCrossesWaitingCutoff(u32),
    ClaimCompletesAtClosedMark(u64),
    RestartWithSecondFrameHeld(u16),
}

#[hegel::test(test_cases = 6)]
fn generated_mark_histories_preserve_cutoffs_receipts_and_request_identity(case: TestCase) {
    let mut histories = [
        MarkHistory::HiddenCheckedReply(
            case.draw(generators::integers().min_value(1).max_value(900)),
        ),
        MarkHistory::IndependentCutoffFault(
            case.draw(generators::integers().min_value(2).max_value(1_000)),
        ),
        MarkHistory::UntouchedLaneRefusal(
            case.draw(generators::integers().min_value(1).max_value(8)),
        ),
        MarkHistory::ResidualReadFault(
            case.draw(generators::integers().min_value(65).max_value(69)),
        ),
        MarkHistory::AdmissionAfterRebase(
            case.draw(generators::integers().min_value(65).max_value(69)),
        ),
        MarkHistory::EmptyWakeBeforeClaimRecovery(
            case.draw(generators::integers().min_value(1_100).max_value(2_999)),
        ),
        MarkHistory::RepeatedWelcome {
            visited_named_mark: false,
            older_suffix: case.draw(generators::integers().min_value(1).max_value(4)),
        },
        MarkHistory::RepeatedWelcome {
            visited_named_mark: true,
            older_suffix: case.draw(generators::integers().min_value(1).max_value(4)),
        },
        MarkHistory::WaitingCutoffDuringHeldFrame(
            case.draw(generators::integers().min_value(1).max_value(1_000)),
        ),
        MarkHistory::OrphanProbeCrossesWaitingCutoff(
            case.draw(generators::integers().min_value(1).max_value(1_000)),
        ),
        MarkHistory::ClaimCompletesAtClosedMark(
            case.draw(generators::integers().min_value(0).max_value(20)),
        ),
        MarkHistory::RestartWithSecondFrameHeld(
            case.draw(generators::integers().min_value(65).max_value(69)),
        ),
    ];
    // Each example covers every transition family, in a drawn order and with
    // drawn clock offsets and suffix lengths. Each history has its own store.
    let start = case.draw(
        generators::integers::<usize>()
            .min_value(0)
            .max_value(histories.len() - 1),
    );
    histories.rotate_left(start);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for history in histories {
                case.note(&format!("{history:?}"));
                match history {
                    MarkHistory::HiddenCheckedReply(lead) => check_hidden_member_cutoff(lead).await,
                    MarkHistory::IndependentCutoffFault(gap) => {
                        check_active_cutoff_fault(gap).await
                    }
                    MarkHistory::UntouchedLaneRefusal(count) => check_untouched_intake(count).await,
                    MarkHistory::ResidualReadFault(count) => check_waiting_read_fault(count).await,
                    MarkHistory::AdmissionAfterRebase(count) => {
                        check_rebased_admission(count).await
                    }
                    MarkHistory::EmptyWakeBeforeClaimRecovery(cutoff) => {
                        check_empty_retry_before_claim_recovery(cutoff).await;
                    }
                    MarkHistory::RepeatedWelcome {
                        visited_named_mark,
                        older_suffix,
                    } => {
                        check_repeated_welcome(visited_named_mark, older_suffix).await;
                    }
                    MarkHistory::WaitingCutoffDuringHeldFrame(gap) => {
                        check_worker_waiting_cutoff(gap).await
                    }
                    MarkHistory::OrphanProbeCrossesWaitingCutoff(gap) => {
                        check_cutoff_during_orphan_probe(gap).await
                    }
                    MarkHistory::ClaimCompletesAtClosedMark(gap) => check_closed_claim(gap).await,
                    MarkHistory::RestartWithSecondFrameHeld(count) => {
                        check_subsequent_claim_recovery(count).await
                    }
                }
            }
        });
}
