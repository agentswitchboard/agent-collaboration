use super::{
    OutboxItemState, OutboxRowEffect, OutboxRowEvent, OutboxRowMachine, OutboxRowRefusal,
    OutboxRowState,
};
use crate::OutboxCancellation::{NotConfirmedWithin15Minutes, NotSentWithin15Minutes};
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};

use OutboxItemState::{
    Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
};
use OutboxRowEvent::{Claim, EndDue, ReturnNotTransmitted, ReturnUnanswered};

const MARK: i64 = 60_000;
const DEADLINE: i64 = 900_000;

fn row(item_state: OutboxItemState) -> OutboxRowState {
    OutboxRowState {
        item_state,
        flush_at_ms: MARK,
        deadline_ms: DEADLINE,
    }
}

#[test]
fn declared_edges_match_the_reducer() {
    let cases = [
        InOutbox,
        SentAnswerLost,
        Sent,
        Resent,
        Answered,
        Refused,
        NotSent,
        Unconfirmed,
    ]
    .into_iter()
    .flat_map(|state| {
        [
            Claim {
                now_ms: MARK,
                eligible_through_ms: MARK,
                required_deadline_after_ms: MARK,
            },
            ReturnNotTransmitted { now_ms: DEADLINE },
            ReturnUnanswered {
                now_ms: DEADLINE - 1,
            },
            ReturnUnanswered { now_ms: DEADLINE },
            EndDue { now_ms: DEADLINE },
        ]
        .map(|event| (row(state), event))
    });
    check_state_machine_transitions::<OutboxRowMachine>(cases).unwrap();
}

#[test]
fn cutoff_uses_the_departure_evidence_and_excludes_equality_from_claims() {
    for (before, untouched_wait, untouched_ending, untouched_reason) in [
        (Sent, InOutbox, NotSent, NotSentWithin15Minutes),
        (
            Resent,
            SentAnswerLost,
            Unconfirmed,
            NotConfirmedWithin15Minutes,
        ),
    ] {
        let (waiting, effects) =
            OutboxRowMachine::transition(&row(before), ReturnNotTransmitted { now_ms: DEADLINE })
                .unwrap();
        assert_eq!(waiting.item_state, untouched_wait);
        assert_eq!(effects, [OutboxRowEffect::Stamp { now_ms: DEADLINE }]);
        let (ended, effects) =
            OutboxRowMachine::transition(&waiting, EndDue { now_ms: DEADLINE }).unwrap();
        assert_eq!(ended.item_state, untouched_ending);
        assert_eq!(
            effects,
            [OutboxRowEffect::Finish {
                now_ms: DEADLINE,
                reason: untouched_reason
            }]
        );

        let (ended, effects) =
            OutboxRowMachine::transition(&row(before), ReturnUnanswered { now_ms: DEADLINE })
                .unwrap();
        assert_eq!(ended.item_state, Unconfirmed);
        assert_eq!(
            effects,
            [OutboxRowEffect::Finish {
                now_ms: DEADLINE,
                reason: NotConfirmedWithin15Minutes
            }]
        );
    }

    let waiting = row(InOutbox);
    for (mark, threshold) in [(MARK - 1, MARK), (MARK, DEADLINE)] {
        assert!(matches!(
            OutboxRowMachine::transition(
                &waiting,
                Claim {
                    now_ms: MARK,
                    eligible_through_ms: mark,
                    required_deadline_after_ms: threshold
                },
            ),
            Err(TransitionError::Domain(OutboxRowRefusal::NotDue))
        ));
    }
}
