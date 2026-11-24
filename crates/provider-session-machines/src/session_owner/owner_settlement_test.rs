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
fn the_exact_fenced_operation_can_replay_and_its_late_receipt_clears_the_fence() {
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
