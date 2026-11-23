use super::{
    DeliveryCustodyEffect, DeliveryCustodyEvent, DeliveryCustodyMachine, DeliveryCustodyState,
};
use hegel::{TestCase, generators};
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};
use switchboard_machine_core_store::{InboxDecision, InboxRefusalReason};

const UNKNOWN_SESSION: InboxDecision = InboxDecision::Refused {
    reason: InboxRefusalReason::UnknownSession,
};
const AMBIGUOUS_SESSION: InboxDecision = InboxDecision::Refused {
    reason: InboxRefusalReason::AmbiguousSession,
};

const DELIVERY_EVENTS: [DeliveryCustodyEvent; 4] = [
    DeliveryCustodyEvent::DecisionCommitted(InboxDecision::Accepted),
    DeliveryCustodyEvent::DecisionCommitted(UNKNOWN_SESSION),
    DeliveryCustodyEvent::DecisionCommitted(AMBIGUOUS_SESSION),
    DeliveryCustodyEvent::ReceiptConfirmed,
];

/// Records the first committed decision and the position of its confirmation.
/// Duplicates and a result without a decision cannot change either record.
#[derive(Default)]
struct DeliveryJournal {
    first_commit: Option<InboxDecision>,
    receipt_confirmed: bool,
}

fn compare_steps(events: impl IntoIterator<Item = DeliveryCustodyEvent>) {
    use DeliveryCustodyEvent::{DecisionCommitted, ReceiptConfirmed};
    use DeliveryCustodyState::{
        AwaitingDecision, KeptAcknowledged, KeptAwaitingReceipt, RefusalAcknowledged,
        RefusedAwaitingReceipt,
    };
    let mut journal = DeliveryJournal::default();
    let mut production = AwaitingDecision;
    for event in events {
        let step = DeliveryCustodyMachine::transition(&production, event);
        let receipt = match event {
            DecisionCommitted(decision) if journal.first_commit.is_none() => {
                journal.first_commit = Some(decision);
                Some(decision)
            }
            ReceiptConfirmed if journal.first_commit.is_some() && !journal.receipt_confirmed => {
                journal.receipt_confirmed = true;
                None
            }
            ReceiptConfirmed if journal.first_commit.is_none() => {
                assert_eq!(
                    step,
                    Err(TransitionError::NotAllowed {
                        state: "awaitingDecision",
                        event: "receiptConfirmed",
                    }),
                    "cannot acknowledge before a durable decision"
                );
                continue;
            }
            _ => {
                assert_eq!(
                    step,
                    Err(TransitionError::Stale),
                    "a repeat cannot replace the first decision"
                );
                continue;
            }
        };
        let expected = match (journal.first_commit, journal.receipt_confirmed) {
            (None, _) => AwaitingDecision,
            (Some(InboxDecision::Accepted), false) => KeptAwaitingReceipt,
            (Some(InboxDecision::Accepted), true) => KeptAcknowledged,
            (Some(InboxDecision::Refused { reason }), false) => RefusedAwaitingReceipt { reason },
            (Some(InboxDecision::Refused { reason }), true) => RefusalAcknowledged { reason },
        };
        let effects = receipt
            .map(DeliveryCustodyEffect::ReceiptPending)
            .into_iter()
            .collect();
        assert_eq!(step, Ok((expected, effects)), "{production:?} + {event:?}");
        production = step.unwrap().0;
    }
}

#[hegel::test(test_cases = 128)]
fn generated_delivery_orders_require_a_commit_and_preserve_the_first_decision(case: TestCase) {
    use DeliveryCustodyEvent::{DecisionCommitted, ReceiptConfirmed};
    compare_steps([
        ReceiptConfirmed,
        DecisionCommitted(UNKNOWN_SESSION),
        DecisionCommitted(InboxDecision::Accepted),
        ReceiptConfirmed,
        ReceiptConfirmed,
    ]);

    let count = case.draw(generators::integers::<u8>().min_value(1).max_value(24));
    compare_steps(
        (0..count).map(|_| case.draw(generators::sampled_from(DELIVERY_EVENTS.as_slice()))),
    );
}

#[test]
fn the_delivery_reducer_and_declared_transition_table_agree() {
    use DeliveryCustodyState::{
        AwaitingDecision, KeptAcknowledged, KeptAwaitingReceipt, RefusalAcknowledged,
        RefusedAwaitingReceipt,
    };
    let reason = InboxRefusalReason::UnknownSession;
    let cases = [
        AwaitingDecision,
        KeptAwaitingReceipt,
        KeptAcknowledged,
        RefusedAwaitingReceipt { reason },
        RefusalAcknowledged { reason },
    ];
    let result = check_state_machine_transitions::<DeliveryCustodyMachine>(
        cases
            .into_iter()
            .flat_map(|state| DELIVERY_EVENTS.into_iter().map(move |event| (state, event))),
    );
    assert_eq!(result, Ok(()));
}
