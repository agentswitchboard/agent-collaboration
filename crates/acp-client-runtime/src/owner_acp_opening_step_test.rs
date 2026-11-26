use super::*;
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
    .validate(Some(OpeningState::ConfiguringCreate(stopped)), Effect::None);
    assert!(stored.is_none());
    let Effect::Fault(fault) = effect else {
        panic!("the invalid handler outcome must be an internal fault");
    };
    assert_eq!(fault.phase, OpeningPhase::QueuedCreate);
    assert_eq!(fault.edge, Edge::WriterAccepted);
    assert_eq!(fault.declared, OpeningPhase::AwaitingCreate);
    assert_eq!(fault.actual, Some(OpeningPhase::ConfiguringCreate));
}
