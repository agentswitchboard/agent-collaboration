use super::*;
use crate::session_turn_machine::{TurnRecordDisposition, TurnSettlement};
use session_event_model::{StopReason, TurnOutcome};

fn provider_finished(authorization: DispatchAuthorization) -> OwnerEvent {
    OwnerEvent::Dispatch(DispatchResult::PromptFinished {
        authorization,
        outcome: WireOutcome::Confirmed(PromptCompletion {
            stop_reason: StopReason::EndTurn,
            local_cause: None,
            response: String::new(),
        }),
    })
}

fn settlement(input: &OwnerInput) -> TurnSettlement {
    TurnSettlement {
        turn: input.turn,
        operation: input.operation,
    }
}

fn witnessed_end(effect: &OwnerEffect, turn: TurnId) -> bool {
    matches!(effect, OwnerEffect::Publish(PublicationBatch::Commit { facts, .. })
        if facts.iter().any(|fact| matches!(fact, PublicationFact::Live(
            SessionPublication::TurnEnded {
                turn: current,
                outcome: TurnOutcome::Ended { stop_reason: StopReason::EndTurn, .. },
                ..
            }
        ) if *current == turn)))
}

#[test]
fn close_waits_for_settlement_and_does_not_cancel_an_ended_turn() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    let key = settlement(&prompt);
    let observed = apply_pending(&mut state, provider_finished(wire));
    assert_eq!(state.snapshot().turn, SessionTurn::Settling(key));
    assert!(state.snapshot().last_live_turn.is_none());
    assert!(
        !observed
            .iter()
            .any(|effect| witnessed_end(effect, prompt.turn))
    );

    let close = apply_pending(
        &mut state,
        OwnerEvent::Command(SessionOwnerCommand::Close {
            generation: run.generation,
            attempt: CloseAttempt(uuid(3)),
        }),
    );
    assert!(!close.iter().any(|effect| matches!(
        effect,
        OwnerEffect::Wire(SessionWireRequest::Close(_)) | OwnerEffect::DrainCancel(_)
    )));
    assert!(state.has_pending_deadline(&OwnerDeadline::SettleTurn {
        run: run.generation,
        settlement: key,
    }));
    let closed = apply_pending(
        &mut state,
        OwnerEvent::TurnAdmissionSettled {
            run: run.generation,
            settlement: key,
            evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Saved),
        },
    );
    let ended_at = closed
        .iter()
        .position(|effect| witnessed_end(effect, prompt.turn))
        .unwrap();
    let close_at = closed
        .iter()
        .position(|effect| matches!(effect, OwnerEffect::Wire(SessionWireRequest::Close(_))))
        .unwrap();
    assert!(ended_at < close_at);
    assert_eq!(state.snapshot().last_live_turn.unwrap().turn, prompt.turn);
}

#[test]
fn retiring_a_settling_generation_publishes_the_witnessed_end_before_unload() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    apply_pending(&mut state, provider_finished(wire));
    let retired = apply_pending(&mut state, OwnerEvent::RunLost(run.generation));
    let ended_at = retired
        .iter()
        .position(|effect| witnessed_end(effect, prompt.turn))
        .unwrap();
    let unloaded_at = retired.iter().position(|effect| matches!(
        effect, OwnerEffect::Publish(PublicationBatch::Commit { facts, .. })
            if facts.contains(&PublicationFact::Live(SessionPublication::Unloaded(run.generation)))
    )).unwrap();
    assert!(ended_at < unloaded_at);
    assert_eq!(state.snapshot().turn, SessionTurn::Idle);
    let late = apply_pending(
        &mut state,
        OwnerEvent::TurnAdmissionSettled {
            run: run.generation,
            settlement: settlement(&prompt),
            evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Saved),
        },
    );
    assert!(!late.iter().any(|effect| witnessed_end(effect, prompt.turn)));
}

#[test]
fn the_fenced_operation_can_replay_and_its_late_receipt_clears_after_run_loss() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    let key = settlement(&prompt);
    let new_operation = uuid(3);
    apply_pending(&mut state, provider_finished(wire));
    apply_pending(
        &mut state,
        OwnerEvent::Deadline(OwnerDeadline::SettleTurn {
            run: run.generation,
            settlement: key,
        }),
    );
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(new_operation, true),
        PromptStartDisposition::Fenced(key)
    );
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(prompt.operation, true),
        PromptStartDisposition::Proceed
    );

    apply_pending(&mut state, OwnerEvent::RunLost(run.generation));
    let late = apply_pending(
        &mut state,
        OwnerEvent::TurnAdmissionSettled {
            run: run.generation,
            settlement: key,
            evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Saved),
        },
    );
    assert!(!late.iter().any(|effect| witnessed_end(effect, prompt.turn)));
    assert_eq!(
        state
            .snapshot()
            .prompt_start_disposition(new_operation, true),
        PromptStartDisposition::Proceed
    );
}

#[test]
fn closing_and_reopening_the_same_owner_preserves_the_fence_until_exact_clear() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    let key = settlement(&prompt);
    apply_pending(&mut state, provider_finished(wire));
    apply_pending(
        &mut state,
        OwnerEvent::Deadline(OwnerDeadline::SettleTurn {
            run: run.generation,
            settlement: key,
        }),
    );
    let owner = state.snapshot().owner;
    let close = start_close(&mut state, run.generation, 3);
    apply(&mut state, close_reply(close, WireOutcome::Confirmed(())));
    let reopened = restore_live(&mut state, 4);
    let next = input(reopened.generation, 5);
    let start = SessionOwnerCommand::Prompt {
        input: next.clone(),
        admission: PromptAdmission::Direct,
    };

    assert_eq!(state.snapshot().owner, owner);
    assert_ne!(reopened.generation, run.generation);
    assert!(matches!(
        decide(&state, OwnerEvent::Command(start.clone())),
        Err(SessionOwnerRejection::Refused(
            SessionOwnerRefusal::PreviousTurnSettlementUnresolved(fenced)
        )) if fenced == key
    ));
    apply_pending(
        &mut state,
        OwnerEvent::TurnAdmissionSettled {
            run: run.generation,
            settlement: key,
            evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Saved),
        },
    );

    assert!(
        command(&mut state, start)
            .contains(&OwnerEffect::Store(SessionStoreRequest::MarkDispatch(next)))
    );
}

#[test]
fn retiring_an_untaken_open_turn_publishes_no_completion_and_the_next_start_can_run() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let untaken = input(run.generation, 2);
    enqueue_prompt(&mut state, &untaken);
    let offered = apply(&mut state, mark_prompt(&untaken));
    assert_eq!(state.turn(), SessionTurn::Running { turn: untaken.turn });
    assert_eq!(state.snapshot().turn, SessionTurn::Idle);
    assert!(!offered.contains(&OwnerEffect::PromptTaken(untaken.clone())));

    let retired = apply(&mut state, OwnerEvent::RunLost(run.generation));

    assert_eq!(state.snapshot().turn, SessionTurn::Idle);
    assert!(state.snapshot().last_live_turn.is_none());
    assert!(!retired.iter().any(|effect| matches!(effect,
        OwnerEffect::Publish(PublicationBatch::Commit { facts, .. })
            if facts.iter().any(|fact| matches!(fact, PublicationFact::Live(
                SessionPublication::TurnEnded { turn, .. }
                | SessionPublication::PreviousTurnEnded { turn, .. }
            ) if *turn == untaken.turn))
    )));
    let reopened = restore_live(&mut state, 3);
    let next = input(reopened.generation, 4);
    command(
        &mut state,
        SessionOwnerCommand::Prompt {
            input: next.clone(),
            admission: PromptAdmission::Direct,
        },
    );
    let mark = apply(&mut state, mark_prompt(&next));
    assert!(
        apply(
            &mut state,
            OwnerEvent::Dispatch(DispatchResult::Taken(authorization(&mark)))
        )
        .contains(&OwnerEffect::PromptTaken(next))
    );
}

#[test]
fn same_tick_store_and_admission_events_leave_one_witnessed_end_and_the_same_fence() {
    use std::time::Duration;

    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    let key = settlement(&prompt);
    let armed = apply_pending(&mut state, provider_finished(wire));
    let deadlines: Vec<_> = armed
        .iter()
        .filter_map(|effect| match effect {
            OwnerEffect::Arm {
                deadline:
                    deadline @ (OwnerDeadline::TerminalStore(_) | OwnerDeadline::SettleTurn { .. }),
                budget,
            } => Some((deadline.clone(), *budget)),
            _ => None,
        })
        .collect();
    let [(settle, settle_after), (store, store_after)] = deadlines.as_slice() else {
        panic!("expected both clocks to be armed: {deadlines:?}");
    };
    assert!(matches!(settle, OwnerDeadline::SettleTurn { settlement, .. } if *settlement == key));
    assert!(matches!(store, OwnerDeadline::TerminalStore(_)));
    assert_eq!(
        (*settle_after, *store_after),
        (Duration::from_secs(10), Duration::from_secs(10))
    );

    // The owner observed the provider reply at zero. Hold that observation
    // clock at one instant while advancing every ordering of the due events.
    let due_ms = i64::try_from(settle_after.as_millis()).unwrap();
    let store = OwnerEvent::Deadline(store.clone());
    let settle = OwnerEvent::Deadline(settle.clone());
    let fenced = OwnerEvent::TurnAdmissionSettled {
        run: run.generation,
        settlement: key,
        evidence: TurnAdmissionEvidence::Fenced(TurnRecordDisposition::Uncertain),
    };
    let mut expected_end = None;
    for events in [
        vec![store.clone(), settle.clone()],
        vec![settle.clone(), store.clone()],
        vec![store.clone(), fenced.clone(), settle.clone()],
        vec![store.clone(), settle, fenced],
    ] {
        let mut candidate = state.clone();
        let mut publications = Vec::new();
        for event in events {
            let advances_turn = event != store;
            match candidate.decide_at(
                OwnerMailboxEvent {
                    owner: candidate.snapshot().owner,
                    event,
                },
                due_ms,
            ) {
                Ok(commit) => {
                    candidate = commit.next;
                    if !advances_turn && publications.is_empty() {
                        assert_eq!(candidate.snapshot().turn, SessionTurn::Settling(key));
                    }
                    publications.extend(commit.effects.effects.into_iter().filter(|effect| matches!(effect,
                        OwnerEffect::Publish(PublicationBatch::Commit { facts, .. })
                            if facts.iter().any(|fact| matches!(fact, PublicationFact::Live(SessionPublication::TurnEnded { .. })))
                    )));
                    if let Some(settlement) = commit.effects.continue_after_turn_settlement {
                        let continued = candidate
                            .decide_at(
                                OwnerMailboxEvent {
                                    owner: candidate.snapshot().owner,
                                    event: OwnerEvent::ContinueAfterTurnSettlement(settlement),
                                },
                                due_ms,
                            )
                            .expect("continue at the same tick");
                        assert!(continued.effects.effects.is_empty());
                        candidate = continued.next;
                    }
                }
                Err(SessionOwnerRejection::IgnoredLate) if advances_turn => {}
                unexpected => panic!("unexpected due event decision: {unexpected:?}"),
            }
        }
        assert_eq!(publications.len(), 1);
        assert!(witnessed_end(&publications[0], prompt.turn));
        assert_eq!(candidate.snapshot().turn_start_fences, [key]);
        let next = input(run.generation, 3);
        let start = SessionOwnerCommand::Prompt {
            input: next.clone(),
            admission: PromptAdmission::Direct,
        };
        assert!(
            matches!(decide(&candidate, OwnerEvent::Command(start.clone())),
            Err(SessionOwnerRejection::Refused(SessionOwnerRefusal::PreviousTurnSettlementUnresolved(fence))) if fence == key)
        );
        if let Some((state, observed)) = &expected_end {
            assert_eq!(&candidate, state);
            assert_eq!(&publications, observed);
        } else {
            expected_end = Some((candidate.clone(), publications));
        }
        let late = apply_pending(
            &mut candidate,
            OwnerEvent::TurnAdmissionSettled {
                run: run.generation,
                settlement: key,
                evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Uncertain),
            },
        );
        assert!(!late.iter().any(|effect| witnessed_end(effect, prompt.turn)));
        assert!(candidate.snapshot().turn_start_fences.is_empty());
        assert!(
            command(&mut candidate, start)
                .contains(&OwnerEffect::Store(SessionStoreRequest::MarkDispatch(next)))
        );
    }
}

#[test]
fn a_refused_close_continuation_cannot_undo_the_deadline_end_or_fence() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    apply_pending(&mut state, provider_finished(wire));
    let key = settlement(&prompt);
    command(
        &mut state,
        SessionOwnerCommand::CloseWithCapability(CloseRequest {
            run: RunAttempt {
                generation: run.generation,
                attempt: CloseAttempt(uuid(3)),
            },
            provider_supports_close: false,
        }),
    );
    // Inject a pending store mark with no matching cell slot. The Router-only
    // close reaches retired-store cleanup, which rejects the missing entry.
    state.prompts.insert(
        99,
        PendingPrompt {
            input: input(run.generation, 4),
            admission: PromptAdmission::Queue,
            wait: InputWait::DispatchStore,
            choose_steer_if_a_turn_starts: false,
            queued: true,
            parked_mark: None,
        },
    );
    let deadline = OwnerEvent::Deadline(OwnerDeadline::SettleTurn {
        run: run.generation,
        settlement: key,
    });
    let after = assert_exit_survives_refused_continuation(&state, deadline, key);
    assert_eq!(
        after.snapshot().prompt_start_disposition(uuid(5), true),
        PromptStartDisposition::Fenced(key)
    );
}

#[test]
fn a_refused_queue_continuation_cannot_undo_clear_or_publish_the_end_twice() {
    use state_machine_core::StateMachine as _;

    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let prompt = input(run.generation, 2);
    let wire = send_prompt(&mut state, &prompt);
    let queued = input(run.generation, 3);
    command(
        &mut state,
        SessionOwnerCommand::Prompt {
            input: queued.clone(),
            admission: PromptAdmission::Queue,
        },
    );
    apply_pending(&mut state, provider_finished(wire));
    let key = settlement(&prompt);
    // Force the queue-machine refusal with a head already claimed at the
    // cell. The normal owner invariant forbids this while Settling.
    let sequence = *state.prompts.first_key_value().unwrap().0;
    (state.cell, _) = crate::session_cell_machine::SessionCellMachine::transition(
        &state.cell,
        crate::session_cell_machine::SessionCellEvent::Claim { sequence },
    )
    .unwrap();
    let clear = OwnerEvent::TurnAdmissionSettled {
        run: run.generation,
        settlement: key,
        evidence: TurnAdmissionEvidence::Clear(TurnRecordDisposition::Saved),
    };
    let mut after = assert_exit_survives_refused_continuation(&state, clear, key);
    assert_eq!(
        after.snapshot().prompt_start_disposition(uuid(4), true),
        PromptStartDisposition::Proceed
    );
    // A normal cancellation removes the fixture's invalid head. A later
    // direct start must use the original live generation, without reopening.
    command(
        &mut after,
        SessionOwnerCommand::CancelQueued {
            generation: run.generation,
            input: queued.input,
        },
    );
    let next = input(run.generation, 4);
    assert!(
        command(
            &mut after,
            SessionOwnerCommand::Prompt {
                input: next.clone(),
                admission: PromptAdmission::Direct
            }
        )
        .contains(&OwnerEffect::Store(SessionStoreRequest::MarkDispatch(next)))
    );
}

fn assert_exit_survives_refused_continuation(
    state: &SessionOwnerState,
    event: OwnerEvent,
    settlement: TurnSettlement,
) -> SessionOwnerState {
    let exit =
        decide(state, event).expect("settlement exit commits before the fallible continuation");
    assert_eq!(exit.next.turn(), SessionTurn::Idle);
    assert_eq!(
        exit.effects.continue_after_turn_settlement,
        Some(settlement)
    );
    assert_eq!(
        exit.effects
            .effects
            .iter()
            .filter(|effect| witnessed_end(effect, settlement.turn))
            .count(),
        1
    );

    assert!(
        decide(
            &exit.next,
            OwnerEvent::ContinueAfterTurnSettlement(settlement)
        )
        .is_err()
    );
    exit.next
}

#[test]
fn a_started_successor_publishes_previous_end_then_its_start_without_waiting_for_the_write() {
    let mut state = fresh();
    let run = restore_live(&mut state, 1);
    let previous = input(run.generation, 2);
    let wire = send_prompt(&mut state, &previous);
    let steer = OwnerInput {
        turn: previous.turn,
        ..input(run.generation, 3)
    };
    let _mark = command(
        &mut state,
        SessionOwnerCommand::Steer {
            input: steer.clone(),
            supported: true,
        },
    );
    let offered = apply(
        &mut state,
        OwnerEvent::Store(SessionStoreResult::SteerDispatch {
            input: steer,
            outcome: EffectOutcome::Applied,
        }),
    );
    let authorized = authorization(&offered);
    apply(
        &mut state,
        OwnerEvent::Dispatch(DispatchResult::Taken(authorized.clone())),
    );
    apply_pending(&mut state, provider_finished(wire));
    assert_eq!(
        state.snapshot().turn,
        SessionTurn::Settling(settlement(&previous))
    );
    let successor = TurnId(uuid(4));
    let effects = apply_pending(
        &mut state,
        OwnerEvent::Dispatch(DispatchResult::SteerStartedNewTurn {
            authorization: authorized,
            turn: successor,
        }),
    );
    let ended_at = effects
        .iter()
        .position(|effect| witnessed_end(effect, previous.turn))
        .unwrap();
    let started_at = effects
        .iter()
        .position(|effect| {
            matches!(
                effect, OwnerEffect::Publish(PublicationBatch::Commit { facts, .. })
                    if facts.iter().any(|fact| matches!(fact, PublicationFact::Live(
                        SessionPublication::TurnStarted { turn, .. }
                    ) if *turn == successor))
            )
        })
        .unwrap();
    assert!(ended_at < started_at);
    assert_eq!(
        state.snapshot().turn,
        SessionTurn::Running { turn: successor }
    );
}
