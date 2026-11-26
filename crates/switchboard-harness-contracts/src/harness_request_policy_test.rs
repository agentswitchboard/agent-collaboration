use crate::{
    ApprovalLease, ApprovalLeaseBinding, ApprovalLeaseRefusalReason,
    BeforeOfferApprovalLeaseRefusal, HarnessOwnerRecovery, ObservedTurnApprovalLeaseRefusal,
    RefusedApprovalLeaseMove, RefusedRequestMove, RequestCancellationSignal,
    RequestDecisionRefusal, RequestPolicy,
};

#[test]
fn request_and_recovery_ports_can_be_injected_as_trait_objects() {
    type TestPayloads = ((), (), (), (), (), ());
    let _: Option<&dyn RequestPolicy<TestPayloads>> = None;
    let _: Option<&dyn ApprovalLeaseBinding<()>> = None;
    let _: Option<&dyn ApprovalLease> = None;
    let _: Option<&dyn RequestCancellationSignal> = None;
    let _: Option<&dyn HarnessOwnerRecovery> = None;
}

#[test]
fn request_and_lease_refusals_list_only_the_moves_available_at_their_boundary() {
    use ApprovalLeaseRefusalReason::{AlreadyActive, ContextUnavailable};
    use RefusedApprovalLeaseMove::{KeepObservedTurn, SettleAsNotSent};
    use RefusedRequestMove::{KeepExistingAnswer, ReturnRefusal};
    use RequestDecisionRefusal::{AlreadyAnswered, AnsweredElsewhere, NoOpenScope, ScopeEnded};

    for refusal in [NoOpenScope, ScopeEnded, AlreadyAnswered, AnsweredElsewhere] {
        let (reason, listed, omitted) = match refusal {
            NoOpenScope => (
                "the request has no open session or turn scope",
                ReturnRefusal,
                KeepExistingAnswer,
            ),
            ScopeEnded => (
                "the request's admitted scope has ended",
                ReturnRefusal,
                KeepExistingAnswer,
            ),
            AlreadyAnswered => (
                "this request already has an answer",
                KeepExistingAnswer,
                ReturnRefusal,
            ),
            AnsweredElsewhere => (
                "another client answered this request",
                KeepExistingAnswer,
                ReturnRefusal,
            ),
        };
        assert_eq!(refusal.reason(), reason);
        assert_eq!(refusal.valid_moves(), [listed], "{refusal:?}");
        assert!(!refusal.valid_moves().contains(&omitted), "{refusal:?}");
    }

    for refusal in [ContextUnavailable, AlreadyActive] {
        let reason = match refusal {
            ContextUnavailable => "the harness approval context is unavailable",
            AlreadyActive => "this session already has an active approval context",
        };
        let before_offer = BeforeOfferApprovalLeaseRefusal(refusal);
        let observed_turn = ObservedTurnApprovalLeaseRefusal(refusal);
        assert_eq!(before_offer.reason(), reason);
        assert_eq!(observed_turn.reason(), reason);
        assert_eq!(before_offer.valid_moves(), [SettleAsNotSent], "{refusal:?}");
        assert!(
            !before_offer.valid_moves().contains(&KeepObservedTurn),
            "{refusal:?}"
        );
        assert_eq!(
            observed_turn.valid_moves(),
            [KeepObservedTurn],
            "{refusal:?}"
        );
        assert!(
            !observed_turn.valid_moves().contains(&SettleAsNotSent),
            "{refusal:?}"
        );
    }
}
