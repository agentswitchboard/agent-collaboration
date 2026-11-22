use super::*;
use central_engine_model::check_transitions;
use central_engine_test_support::timer::{ReplayIds, context, time};
use mail::{MessageOutcome, MessageRefusal, MessageStatus, ReceiptKind, WaitingReason};

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
            cutoff: time(15),
            mark: time(20),
        },
        lifecycle: DirectLifecycle::Queued,
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
            waiting: Some(WaitingReason::Busy),
        }),
        1 => DirectMessageEvent::Receipt(ReceiptKind::Seen {
            completed_at: observation_time,
        }),
        2 => DirectMessageEvent::Receipt(ReceiptKind::NotSeen {
            waiting: WaitingReason::SessionNotOpen,
        }),
        3 => DirectMessageEvent::Receipt(ReceiptKind::Refused {
            reason: MessageRefusal::UnknownSession,
        }),
        4 => DirectMessageEvent::Receipt(ReceiptKind::Refused {
            reason: MessageRefusal::SessionClosed,
        }),
        5 => DirectMessageEvent::Due {
            occurrence: time(20),
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
            Some((2, _)) => MessageStatus::NotSeen {
                waiting: WaitingReason::SessionNotOpen,
            },
            Some((3, _)) => MessageStatus::Refused {
                reason: MessageRefusal::UnknownSession,
            },
            Some((4, _)) => MessageStatus::Refused {
                reason: MessageRefusal::SessionClosed,
            },
            Some((other, _)) => panic!("invalid oracle ending {other}"),
            None => {
                let deadline_passed = self.marked || now >= time(15);
                match self.saved_at {
                    Some(arrived_at) => MessageStatus::Delivered {
                        arrived_at,
                        waiting: Some(WaitingReason::Busy),
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
        if choice >= 5 {
            if choice != 5 || self.marked || self.first_final.is_some() {
                return Err(TransitionError::Stale);
            }
            self.marked = true;
            return Ok(Some(match self.saved_at {
                Some(arrived_at) => MessageOutcome::DeliveredSeenNotConfirmed {
                    arrived_at,
                    waiting: Some(WaitingReason::Busy),
                },
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
            (false, 2) => Some(MessageOutcome::NotSeen {
                waiting: WaitingReason::SessionNotOpen,
            }),
            (false, 3) => Some(MessageOutcome::Refused {
                reason: MessageRefusal::UnknownSession,
            }),
            (false, 4) => Some(MessageOutcome::Refused {
                reason: MessageRefusal::SessionClosed,
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
        let choice = case.draw(gs::integers::<u8>().max_value(6));
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
                state
                    .lifecycle
                    .project_message_status(initial.subject.cutoff, now),
                oracle.expected_message_status(now)
            );
        }
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
    for kinds in [&[0][..], &[5], &[0, 5], &[1], &[2], &[3]] {
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
        .flat_map(|state| (0..=6).map(move |choice| (state.clone(), event(choice, time(8)))));
    assert_eq!(
        check_transitions::<DirectMessage>(cases, &mut context(20, &mut ReplayIds)),
        Ok(())
    );
}
