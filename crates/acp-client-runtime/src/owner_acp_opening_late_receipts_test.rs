//! Close displacement and route loss retain only the receipts they still own.

use super::*;
use switchboard_harness_contracts::LifecycleRefusal;

#[tokio::test]
async fn a_closing_restore_accepts_only_the_exact_late_open_receipt_classes() {
    let fixture = Fixture::new().await;
    let instance = OpeningInstance::default();
    let cases = [
        (
            RestoreMilestone::held(*fixture.session(), None, None, true),
            true,
        ),
        (
            RestoreMilestone::held(*fixture.session(), None, None, false),
            true,
        ),
        (RestoreMilestone::Failed, true),
        (RestoreMilestone::ReplayAdmitted, false),
        (RestoreMilestone::ReplayRejected, false),
        (RestoreMilestone::WorkerExited, false),
    ];
    for (milestone, accepts) in cases {
        let event = milestone.event();
        let row = super::super::super::OpeningRow {
            attempt: fixture.attempt,
            instance: instance.clone(),
            state: OpeningState::ClosingRestore {
                run: fixture.run.clone(),
                attempt: fixture.close.attempt,
            },
        };
        let result = {
            let _table = lock(&fixture.table);
            row.apply(&instance, Command::RestoreProgress(milestone, None))
        };
        let row = match (accepts, result) {
            (true, Ok((Some(row), Effect::Discarded(discarded)))) => {
                assert!(
                    matches!(*discarded, Command::RestoreProgress(milestone, None) if milestone.event() == event)
                );
                row
            }
            (false, Err(refused)) => {
                assert_eq!(refused.rejection.reason, LifecycleRefusal::RouteClosing);
                assert!(
                    matches!(*refused.rejection._command, Command::RestoreProgress(milestone, None) if milestone.event() == event)
                );
                *refused.row
            }
            _ => panic!(
                "closing restore must distinguish late receipt from replay milestone {event:?}"
            ),
        };
        let OpeningState::ClosingRestore { run, attempt } = row.state else {
            panic!("only the exact original close owns its row");
        };
        assert!(run.route.is_same_as(&fixture.run.route));
        assert_eq!(attempt, fixture.close.attempt);
    }
    fixture.assert_disposals();
}

#[tokio::test]
async fn writer_stop_keeps_only_the_lost_routes_exact_remaining_receipt() {
    let fixture = Fixture::new().await;
    let instance = OpeningInstance::default();
    for receipt in [
        LostReceipt::None,
        LostReceipt::Open(RestoreReceiptStage::AwaitingPeer),
        LostReceipt::Open(RestoreReceiptStage::PublishingLoad),
        LostReceipt::Close(fixture.close.attempt),
    ] {
        let row = super::super::super::OpeningRow {
            attempt: fixture.attempt,
            instance: instance.clone(),
            state: OpeningState::RouteLost {
                run: fixture.run.clone(),
                receipt,
            },
        };
        let result = {
            let _table = lock(&fixture.table);
            row.apply(&instance, Command::WriterStopped)
        };
        let Ok((stored, Effect::Release(release))) = result else {
            panic!("the stopped writer must settle the matched lost-route receipt class");
        };
        assert!(release.instance.is_same_as(&instance));
        assert!(
            release
                .state
                .run()
                .expect("release carries the original route")
                .route
                .is_same_as(&fixture.run.route)
        );
        match receipt {
            LostReceipt::None => assert!(stored.is_none()),
            LostReceipt::Open(expected) => {
                let Some(super::super::super::OpeningRow {
                    state:
                        OpeningState::StoppedAwaitingReply(StoppedReceipt::OpenRestore(run, stage)),
                    ..
                }) = stored
                else {
                    panic!("the original restore still owns the live receipt");
                };
                assert!(run.route.is_same_as(&fixture.run.route));
                assert_eq!(
                    std::mem::discriminant(&stage),
                    std::mem::discriminant(&expected)
                );
            }
            LostReceipt::Close(expected) => {
                let Some(super::super::super::OpeningRow {
                    state: OpeningState::StoppedAwaitingReply(StoppedReceipt::Close(run, attempt)),
                    ..
                }) = stored
                else {
                    panic!("the original close still owns the live receipt");
                };
                assert!(run.route.is_same_as(&fixture.run.route));
                assert_eq!(attempt, expected);
            }
        }
    }
}
