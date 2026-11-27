use super::*;
use crate::opening::opening_test_adapter::TestOpening;
use tokio_util::sync::CancellationToken;

#[test]
fn an_undeclared_outcome_removes_the_row_and_keeps_a_typed_fault() {
    let instance = OpeningInstance::default();
    let stopped = CancellationToken::new();
    let (stored, effect) = DeclaredOutcome {
        phase: OpeningPhase::QueuedCreate,
        edge: Edge::WriterAccepted,
        declared: OpeningPhase::AwaitingCreate,
        instance: &instance,
        previous_run: None,
        previous_offer: None,
    }
    .validate::<TestOpening>(Some(OpeningState::ConfiguringCreate(stopped)), Effect::None);
    assert!(stored.is_none());
    let Effect::Fault(fault) = effect else {
        panic!("the invalid handler outcome must be an internal fault");
    };
    assert_eq!(fault.phase, OpeningPhase::QueuedCreate);
    assert_eq!(fault.edge, Edge::WriterAccepted);
    assert_eq!(fault.declared, OpeningPhase::AwaitingCreate);
    assert_eq!(fault.actual, Some(OpeningPhase::ConfiguringCreate));
}

#[test]
fn an_already_answered_gone_route_has_no_restore_receipt_to_await() {
    use switchboard_harness_contracts::{
        ConnectionEpoch, OwnerAddress, OwnerIncarnation, SessionGeneration,
    };

    let owner = OwnerAddress {
        incarnation: OwnerIncarnation("019f0000-0000-7000-8000-000000001274".parse().unwrap()),
        connection_epoch: ConnectionEpoch("019f0000-0000-7000-8000-000000001334".parse().unwrap()),
    };
    let run = super::super::RunResources::new(
        owner,
        SessionGeneration::from_number(1),
        &CancellationToken::new(),
    );
    let route = run.route.clone();
    let instance = OpeningInstance::default();
    let Ok((Some(lost), Effect::Release(_))) = step::<TestOpening>(
        OpeningState::Active(run),
        Command::RouteRemoved(route),
        &instance,
        false,
    ) else {
        panic!("route loss must keep only the facts that are still owed");
    };
    assert!(matches!(
        lost,
        OpeningState::RouteLost {
            receipt: LostReceipt::None,
            ..
        }
    ));
    let Ok((stored, Effect::Release(_))) = step(lost, Command::WriterStopped, &instance, false)
    else {
        panic!("writer loss must release an already answered opening");
    };
    assert!(
        stored.is_none(),
        "a disappeared active route must not invent a pending restore receipt"
    );
}
