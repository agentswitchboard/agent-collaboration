use super::*;
use central_engine_model::check_transitions;
use central_engine_test_support::timer::{ReplayIds, context, time};
use mail::{MessageOutcome, MessageRefusal, MessageStatus, ReceiptKind};

fn initial(tell_sender: bool) -> DirectReducerState {
    let machine_id: MachineId = "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap();
    DirectReducerState {
        subject: DirectMessageSubject {
            id: "01890f2e-7b4c-7cc0-98c4-0000000000a1".parse().unwrap(),
            sender_session: tell_sender.then(|| SessionRef {
                machine_id: machine_id.clone(),
                session_id: "sender".to_owned().try_into().unwrap(),
            }),
            sender_request_id: "01890f2e-7b4c-7cc0-98c4-0000000000b1".parse().unwrap(),
            recipient: machine_id,
            mark: time(15),
        },
        lifecycle: DirectLifecycle::Queued,
        deadline_settled_at: None,
    }
}

fn outcome(effects: &[DirectMessageEffect], state: &DirectReducerState) -> Option<MessageOutcome> {
    match effects {
        [] => None,
        [DirectMessageEffect::TellSender(message)] => {
            let mail::MessageBody::DeliveryOutcome {
                message_id,
                request_id,
                outcome,
            } = &message.body
            else {
                panic!("expected an outcome");
            };
            assert_eq!(message_id, &state.subject.id);
            assert_eq!(request_id, &state.subject.sender_request_id);
            assert_eq!(Some(&message.target), state.subject.sender_session.as_ref());
            Some(*outcome)
        }
        _ => panic!("one transition may produce at most one outcome"),
    }
}

fn event(choice: u8, observation_time: Timestamp) -> DirectMessageEvent {
    match choice {
        0 => DirectMessageEvent::Receipt(ReceiptKind::Delivered {
            arrived_at: observation_time,
        }),
        1 => DirectMessageEvent::Receipt(ReceiptKind::Seen {
            completed_at: observation_time,
        }),
        2 => DirectMessageEvent::Receipt(ReceiptKind::Refused {
            reason: MessageRefusal::AmbiguousSession,
        }),
        3 => DirectMessageEvent::Receipt(ReceiptKind::Refused {
            reason: MessageRefusal::UnknownSession,
        }),
        4 => DirectMessageEvent::Due {
            occurrence: time(15),
        },
        _ => DirectMessageEvent::Due {
            occurrence: time(21),
        },
    }
}

/// The model stores independent observations, and projects the status after each input.
#[derive(Default)]
struct Oracle {
    saved_at: Option<Timestamp>,
    first_final: Option<(u8, Timestamp)>,
    marked: bool,
}

impl Oracle {
    fn expected_message_status(&self, now: Timestamp) -> MessageStatus {
        match self.first_final {
            Some((1, completed_at)) => MessageStatus::Seen { completed_at },
            Some((2, _)) => MessageStatus::Refused {
                reason: MessageRefusal::AmbiguousSession,
            },
            Some((3, _)) => MessageStatus::Refused {
                reason: MessageRefusal::UnknownSession,
            },
            Some((other, _)) => panic!("invalid oracle ending {other}"),
            None => {
                let deadline_passed = now >= time(15);
                match self.saved_at {
                    Some(arrived_at) => MessageStatus::Delivered {
                        arrived_at,

                        deadline_passed,
                    },
                    None => MessageStatus::Sent { deadline_passed },
                }
            }
        }
    }

    fn apply_oracle_event(
        &mut self,
        choice: u8,
        at: Timestamp,
    ) -> Result<Option<MessageOutcome>, TransitionError<DirectMessageRefusal>> {
        let covered = TransitionError::Domain(DirectMessageRefusal::AlreadyRecorded);
        let settled = TransitionError::Domain(DirectMessageRefusal::AlreadySettled);
        if choice >= 4 {
            if choice != 4 || self.marked || self.first_final.is_some() {
                return Err(TransitionError::Stale);
            }
            self.marked = true;
            return Ok(Some(match self.saved_at {
                Some(arrived_at) => MessageOutcome::DeliveredSeenNotConfirmed { arrived_at },
                None => MessageOutcome::DeliveryNotConfirmed,
            }));
        }
        if choice == 0 {
            if self.first_final.is_some() || self.saved_at.is_some() {
                return Err(covered);
            }
            self.saved_at = Some(at);
            return Ok(None);
        }
        if let Some((first, first_at)) = self.first_final {
            return Err(if first == choice && (choice != 1 || first_at == at) {
                covered
            } else {
                settled
            });
        }
        self.first_final = Some((choice, at));
        Ok(match (self.marked, choice) {
            (true, _) | (false, 1) => None,
            (false, 2) => Some(MessageOutcome::Refused {
                reason: MessageRefusal::AmbiguousSession,
            }),
            (false, 3) => Some(MessageOutcome::Refused {
                reason: MessageRefusal::UnknownSession,
            }),
            (_, _) => {
                return Err(TransitionError::Domain(
                    DirectMessageRefusal::InvalidIdentifier,
                ));
            }
        })
    }
}

#[hegel::test(test_cases = 400)]
fn every_generated_receipt_and_mark_matches_the_independent_oracle(case: hegel::TestCase) {
    use hegel::generators as gs;
    let session_sender = case.draw(gs::booleans());
    let initial = initial(session_sender);
    let mut state = initial.clone();
    let mut oracle = Oracle::default();
    let mut outcomes = 0;
    for _ in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(40)) {
        let choice = case.draw(gs::integers::<u8>().max_value(5));
        let at = time(case.draw(gs::integers::<i64>().max_value(40).min_value(0)));
        let expected = oracle.apply_oracle_event(choice, at);
        let actual =
            DirectMessage::transition(&state, event(choice, at), &mut context(20, &mut ReplayIds));
        match (expected, actual) {
            (Ok(expected), Ok((next, effects))) => {
                let actual = outcome(&effects, &initial);
                assert_eq!(actual, if session_sender { expected } else { None });
                outcomes += usize::from(actual.is_some());
                assert_eq!(next.subject, initial.subject);
                state = next;
            }
            (Err(expected), Err(actual)) => assert_eq!(actual, expected),
            pair => panic!("oracle and reducer differ: {pair:?}"),
        }
        for now in [time(0), time(15), time(40)] {
            assert_eq!(
                state.lifecycle.project_message_status(now >= time(15)),
                oracle.expected_message_status(now)
            );
        }
        assert_eq!(state.deadline_settled_at, oracle.marked.then(|| time(20)));
        assert_eq!(state.is_terminal(), oracle.first_final.is_some());
        assert_eq!(
            DirectMessage::next_due(&state),
            (!oracle.marked && oracle.first_final.is_none()).then_some(initial.subject.mark)
        );
    }
    assert!(outcomes <= 1);
}

#[test]
fn declared_edges_cover_every_receipt_and_mark_state_pair() {
    let mut states = vec![initial(true)];
    for kinds in [&[0][..], &[4], &[0, 4], &[1], &[2], &[3]] {
        let mut state = initial(true);
        for &kind in kinds {
            state = DirectMessage::transition(
                &state,
                event(kind, time(8)),
                &mut context(20, &mut ReplayIds),
            )
            .unwrap()
            .0;
        }
        states.push(state);
    }
    let cases = states
        .into_iter()
        .flat_map(|state| (0..=5).map(move |choice| (state.clone(), event(choice, time(8)))));
    assert_eq!(
        check_transitions::<DirectMessage>(cases, &mut context(20, &mut ReplayIds)),
        Ok(())
    );
}

#[test]
fn a_deadline_settles_once_without_changing_evidence_and_late_refusal_is_silent() {
    let queued = initial(true);
    let deadline = || DirectMessageEvent::Due {
        occurrence: time(15),
    };
    assert!(matches!(
        DirectMessage::transition(&queued, deadline(), &mut context(14, &mut ReplayIds)),
        Err(TransitionError::Stale)
    ));

    let (settled, effects) =
        DirectMessage::transition(&queued, deadline(), &mut context(15, &mut ReplayIds)).unwrap();
    assert_eq!(settled.lifecycle, DirectLifecycle::Queued);
    assert_eq!(settled.deadline_settled_at, Some(time(15)));
    assert_eq!(
        outcome(&effects, &queued),
        Some(MessageOutcome::DeliveryNotConfirmed)
    );
    assert!(matches!(
        DirectMessage::transition(&settled, deadline(), &mut context(20, &mut ReplayIds)),
        Err(TransitionError::Stale)
    ));

    let (refused, effects) = DirectMessage::transition(
        &settled,
        DirectMessageEvent::Receipt(ReceiptKind::Refused {
            reason: MessageRefusal::UnknownSession,
        }),
        &mut context(20, &mut ReplayIds),
    )
    .unwrap();
    assert!(effects.is_empty());
    assert_eq!(refused.deadline_settled_at, Some(time(15)));
    assert_eq!(
        refused.lifecycle.project_message_status(true),
        MessageStatus::Refused {
            reason: MessageRefusal::UnknownSession
        }
    );
}
