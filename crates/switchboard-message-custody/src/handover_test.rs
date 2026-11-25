use super::{
    DirectHandoff, DirectHandoffMachine, Handoff, HandoffEffect, HandoffEvent, HandoffRefusal,
    NoticeAttemptId, NoticePreTakeRefusal, NoticeRefusalReason, NoticeUncertainty,
    NoticeWriteOutcome, SeenReceiptState,
};
use hegel::generators as gs;
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};
use workspace_model::Timestamp;

const CUTOFF: i64 = 60_000;

#[test]
fn cannot_steer_retires_the_notice_before_the_next_mark_without_a_receipt() {
    let claim = attempt(1);
    let writing = Handoff {
        expires_at: at(CUTOFF),
        phase: DirectHandoff::Writing { attempt: claim },
    };
    let refused = HandoffEvent::WriteFinished {
        attempt: claim,
        outcome: NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::CannotSteer),
        now: at(5),
    };
    let (settled, effects) = DirectHandoffMachine::transition(&writing, refused).unwrap();
    assert_eq!(
        settled.phase,
        DirectHandoff::NoticeRefused {
            reason: NoticeRefusalReason::CannotSteer
        }
    );
    assert!(effects.is_empty());
    for later in [
        HandoffEvent::RecoverWriting,
        HandoffEvent::EligibleAtMark {
            attempt: attempt(2),
            now: at(30),
        },
    ] {
        assert!(matches!(
            DirectHandoffMachine::transition(&settled, later),
            Err(TransitionError::NotAllowed { .. })
        ));
    }
}

fn at(millis: i64) -> Timestamp {
    Timestamp::from_timestamp_millis(millis).unwrap()
}

fn attempt(sequence: u8) -> NoticeAttemptId {
    NoticeAttemptId::try_from(format!("019f0000-0000-7000-8000-{sequence:012x}")).unwrap()
}

fn seen(completed_at: Timestamp) -> DirectHandoff {
    DirectHandoff::Seen {
        completed_at,
        receipt: SeenReceiptState::Owed,
    }
}

fn events(active: NoticeAttemptId, stale: NoticeAttemptId) -> [HandoffEvent; 14] {
    let before = at(CUTOFF - 1);
    let cutoff = at(CUTOFF);
    let after = at(CUTOFF + 1);
    let written = NoticeWriteOutcome::Written { taken_at: before };
    let missed = NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::RejectedBeforeTake);
    [
        HandoffEvent::EligibleAtMark {
            attempt: active,
            now: before,
        },
        HandoffEvent::EligibleAtMark {
            attempt: stale,
            now: before,
        },
        HandoffEvent::EligibleAtMark {
            attempt: active,
            now: cutoff,
        },
        HandoffEvent::Cutoff { now: before },
        HandoffEvent::Cutoff { now: cutoff },
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: written,
            now: after,
        },
        HandoffEvent::WriteFinished {
            attempt: stale,
            outcome: written,
            now: after,
        },
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: NoticeWriteOutcome::Written { taken_at: cutoff },
            now: after,
        },
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: missed,
            now: before,
        },
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: missed,
            now: cutoff,
        },
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: NoticeWriteOutcome::NotKnown(NoticeUncertainty::WriterOutcomeLost),
            now: after,
        },
        HandoffEvent::RecoverWriting,
        HandoffEvent::WriteFinished {
            attempt: active,
            outcome: NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::CannotSteer),
            now: before,
        },
        HandoffEvent::ReadAcknowledged {
            completed_at: after,
        },
    ]
}

#[test]
fn the_declared_handover_table_matches_the_reducer() {
    let current = attempt(1);
    let cases = [
        DirectHandoff::Waiting,
        DirectHandoff::Writing { attempt: current },
        DirectHandoff::NotKnown,
        DirectHandoff::NotSeen,
        DirectHandoff::NoticeRefused {
            reason: NoticeRefusalReason::CannotSteer,
        },
        seen(at(CUTOFF - 1)),
    ]
    .into_iter()
    .flat_map(|phase| {
        let state = Handoff {
            expires_at: at(CUTOFF),
            phase,
        };
        events(current, attempt(2))
            .into_iter()
            .map(move |event| (state, event))
    });
    check_state_machine_transitions::<DirectHandoffMachine>(cases).unwrap();
}

#[test]
fn acknowledging_while_waiting_owes_seen_and_cannot_claim_a_notice() {
    let completed_at = at(CUTOFF - 1);
    let state = Handoff {
        expires_at: at(CUTOFF),
        phase: DirectHandoff::Waiting,
    };
    let acknowledgment = HandoffEvent::ReadAcknowledged { completed_at };
    let transitioned = Handoff {
        phase: seen(completed_at),
        ..state
    };
    assert_eq!(
        DirectHandoffMachine::transition(&state, acknowledgment),
        Ok((transitioned, vec![HandoffEffect::WakeReceiptPump]))
    );
    assert!(matches!(
        DirectHandoffMachine::transition(
            &transitioned,
            HandoffEvent::EligibleAtMark {
                attempt: attempt(1),
                now: completed_at
            }
        ),
        Err(TransitionError::NotAllowed { .. })
    ));
    let missed = Handoff {
        phase: DirectHandoff::NotSeen,
        ..state
    };
    assert!(matches!(
        DirectHandoffMachine::transition(&missed, acknowledgment),
        Err(TransitionError::NotAllowed { .. })
    ));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedFailure {
    Inapplicable,
    Stale,
    TimeRejected(HandoffRefusal),
}

#[derive(Default)]
struct NoticeHistory {
    offered: Option<NoticeAttemptId>,
    expired_waiting: bool,
    cannot_offer_again: bool,
    confirmed_at: Option<Timestamp>,
    refused: bool,
}

impl NoticeHistory {
    fn phase(&self) -> DirectHandoff {
        if self.refused {
            return DirectHandoff::NoticeRefused {
                reason: NoticeRefusalReason::CannotSteer,
            };
        }
        if let Some(completed_at) = self.confirmed_at {
            return seen(completed_at);
        }
        if self.cannot_offer_again {
            return DirectHandoff::NotKnown;
        }
        if self.expired_waiting {
            return DirectHandoff::NotSeen;
        }
        self.offered
            .map_or(DirectHandoff::Waiting, |attempt| DirectHandoff::Writing {
                attempt,
            })
    }

    fn apply(&mut self, event: HandoffEvent) -> Result<Vec<HandoffEffect>, ExpectedFailure> {
        let waiting = self.offered.is_none()
            && !self.expired_waiting
            && !self.cannot_offer_again
            && !self.refused;
        match event {
            HandoffEvent::EligibleAtMark { attempt, now } => {
                if !waiting {
                    return Err(ExpectedFailure::Inapplicable);
                }
                if now.timestamp_millis() < CUTOFF {
                    self.offered = Some(attempt);
                    return Ok(vec![HandoffEffect::OfferClaimedNotice]);
                }
                self.expired_waiting = true;
            }
            HandoffEvent::Cutoff { now } => {
                if !waiting && self.offered.is_none() {
                    return Err(ExpectedFailure::Inapplicable);
                }
                if now.timestamp_millis() < CUTOFF {
                    return Err(ExpectedFailure::TimeRejected(
                        HandoffRefusal::CutoffNotReached,
                    ));
                }
                self.expired_waiting = waiting;
            }
            HandoffEvent::WriteFinished {
                attempt,
                outcome,
                now,
            } => {
                if self.offered != Some(attempt) {
                    return Err(ExpectedFailure::Stale);
                }
                match outcome {
                    NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::CannotSteer) => {
                        self.refused = true;
                    }
                    NoticeWriteOutcome::DefinitelyNotWritten(_) => {
                        self.expired_waiting = now.timestamp_millis() >= CUTOFF;
                    }
                    NoticeWriteOutcome::NotKnown(_) => {
                        self.cannot_offer_again = true;
                    }
                    NoticeWriteOutcome::Written { taken_at } => {
                        if taken_at.timestamp_millis() >= CUTOFF {
                            return Err(ExpectedFailure::TimeRejected(
                                HandoffRefusal::TakeOutsideDeliveryWindow,
                            ));
                        }
                        self.offered = None;
                        self.cannot_offer_again = true;
                        self.confirmed_at = Some(taken_at);
                        return Ok(vec![HandoffEffect::WakeReceiptPump]);
                    }
                }
                self.offered = None;
            }
            HandoffEvent::RecoverWriting => {
                if self.offered.take().is_none() {
                    return Err(ExpectedFailure::Inapplicable);
                }
                self.cannot_offer_again = true;
            }
            HandoffEvent::ReadAcknowledged { completed_at } => {
                if !matches!(
                    self.phase(),
                    DirectHandoff::Waiting | DirectHandoff::NotKnown
                ) {
                    return Err(ExpectedFailure::Inapplicable);
                }
                self.cannot_offer_again = true;
                self.confirmed_at = Some(completed_at);
                return Ok(vec![HandoffEffect::WakeReceiptPump]);
            }
        }
        Ok(vec![])
    }
}

#[hegel::test(test_cases = 250)]
fn generated_notice_histories_only_retry_proved_unsent_attempts(case: hegel::TestCase) {
    let mut state = Handoff {
        expires_at: at(CUTOFF),
        phase: DirectHandoff::Waiting,
    };
    let mut reference = NoticeHistory::default();
    let active = attempt(1);
    let stale = attempt(2);
    for _ in 0..case.draw(gs::integers::<u8>().min_value(8).max_value(45)) {
        let step = case.draw(gs::sampled_from(events(active, stale).to_vec()));
        let expected = reference.apply(step);
        let actual = DirectHandoffMachine::transition(&state, step)
            .map(|(next, effects)| {
                state = next;
                effects
            })
            .map_err(|error| match error {
                TransitionError::NotAllowed { .. } => ExpectedFailure::Inapplicable,
                TransitionError::Stale => ExpectedFailure::Stale,
                TransitionError::Domain(reason) => ExpectedFailure::TimeRejected(reason),
            });
        assert_eq!(actual, expected, "{state:?} after {step:?}");
        assert_eq!(state.phase, reference.phase(), "after {step:?}");
    }
}
