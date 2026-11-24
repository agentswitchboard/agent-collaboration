use super::{OwnerRejection, OwnerResult, OwnerTransition, publication_effects::LOCAL_BUDGET};
use crate::SessionGeneration;
use crate::queued_entry_machine::SentOutcome;
use crate::session_cell_machine::SessionCellEvent;
use crate::session_owner::{
    LastLiveTurn, OwnerDeadline, OwnerEffect, OwnerInput, SessionOwnerEffect, SessionOwnerRefusal,
    SessionOwnerState, SessionPublication, TurnAdmissionEvidence, TurnMembership,
};
use crate::session_turn_machine::{SessionTurn, TurnId, TurnSettlement};
use session_event_model::TurnOutcome;
use std::time::Duration;

const TURN_SETTLE_BUDGET: Duration = LOCAL_BUDGET;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::session_owner) enum PendingTurnAdmission {
    Awaiting {
        run: SessionGeneration,
        settlement: TurnSettlement,
    },
    Fenced {
        run: SessionGeneration,
        settlement: TurnSettlement,
    },
}

impl PendingTurnAdmission {
    fn run(self) -> SessionGeneration {
        match self {
            Self::Awaiting { run, .. } | Self::Fenced { run, .. } => run,
        }
    }

    pub(super) fn start_fence(self) -> Option<TurnSettlement> {
        match self {
            Self::Awaiting { .. } => None,
            Self::Fenced { settlement, .. } => Some(settlement),
        }
    }

    fn matches(self, run: SessionGeneration, settlement: TurnSettlement) -> bool {
        match self {
            Self::Awaiting {
                run: current_run,
                settlement: current,
            }
            | Self::Fenced {
                run: current_run,
                settlement: current,
            } => run == current_run && settlement == current,
        }
    }
}

impl SessionOwnerState {
    pub(super) fn settling_turn_is(
        &self,
        run: SessionGeneration,
        settlement: TurnSettlement,
    ) -> bool {
        self.turn() == SessionTurn::Settling(settlement)
            && self
                .ports
                .turn_admission
                .get(&settlement.operation)
                .is_some_and(|pending| pending.matches(run, settlement))
    }
}

impl OwnerTransition {
    pub(super) fn check_direct_start(&self, operation: uuid::Uuid) -> OwnerResult {
        if self.next.ports.turn_admission.contains_key(&operation) {
            return Err(SessionOwnerRefusal::InputAlreadyAdmitted.into());
        }
        if let Some(fence) = self
            .next
            .ports
            .turn_admission
            .values()
            .find_map(|pending| pending.start_fence())
        {
            return Err(SessionOwnerRefusal::PreviousTurnSettlementUnresolved(fence).into());
        }
        if matches!(self.next.turn(), SessionTurn::Settling(_))
            || self.next.unstarted_notice().is_some()
        {
            return Err(SessionOwnerRefusal::PromptStartHeld.into());
        }
        Ok(())
    }

    pub(super) fn finish_turn(
        &mut self,
        input: &OwnerInput,
        outcome: SentOutcome,
        observed: Option<TurnOutcome>,
    ) -> OwnerResult {
        let run = input.generation;
        self.retire_turn_tools(run, input.turn);
        let event = if let Some(observed) = observed {
            let settlement = TurnSettlement {
                turn: input.turn,
                operation: input.operation,
            };
            self.next.ports.settling_witness = Some(LastLiveTurn {
                turn: input.turn,
                input: input.input.clone(),
                outcome: observed,
                observed_at_ms: self.observed_at_ms,
            });
            self.next.ports.turn_admission.insert(
                settlement.operation,
                PendingTurnAdmission::Awaiting { run, settlement },
            );
            self.effects
                .push(OwnerEffect::CallbackMembership(TurnMembership::End {
                    run,
                    turn: input.turn,
                }));
            self.arm(
                OwnerDeadline::SettleTurn { run, settlement },
                TURN_SETTLE_BUDGET,
            );
            SessionCellEvent::ProviderEnded {
                settlement,
                outcome,
            }
        } else {
            SessionCellEvent::TurnEnded {
                turn: input.turn,
                outcome,
            }
        };
        let effects = self.cell_move(event)?;
        self.apply_entry_effects(effects)?;
        if let SessionTurn::Settling(settlement) = self.next.turn() {
            self.effects
                .push(OwnerEffect::ObserveTurnAdmission { run, settlement });
        }
        self.clear_cancel_confirmation(input);
        Ok(())
    }

    pub(super) fn turn_admission_settled(
        &mut self,
        run: SessionGeneration,
        settlement: TurnSettlement,
        evidence: TurnAdmissionEvidence,
    ) -> OwnerResult {
        let pending = self
            .next
            .ports
            .turn_admission
            .get(&settlement.operation)
            .copied()
            .filter(|pending| pending.matches(run, settlement))
            .ok_or(OwnerRejection::IgnoredLate)?;
        let record = match evidence {
            TurnAdmissionEvidence::Clear(record) => {
                self.next.ports.turn_admission.remove(&settlement.operation);
                record
            }
            TurnAdmissionEvidence::Fenced(record) => {
                let fenced = PendingTurnAdmission::Fenced { run, settlement };
                if pending == fenced && self.next.turn() != SessionTurn::Settling(settlement) {
                    return Err(OwnerRejection::IgnoredLate);
                }
                self.next
                    .ports
                    .turn_admission
                    .insert(settlement.operation, fenced);
                record
            }
        };
        if self.next.turn() == SessionTurn::Settling(settlement) {
            self.end_settlement(
                run,
                settlement,
                SessionCellEvent::SettlementCleared { settlement, record },
            )?;
        }
        self.continue_close()?;
        self.drive_prompt_line()
    }

    pub(super) fn turn_settle_deadline(
        &mut self,
        run: SessionGeneration,
        settlement: TurnSettlement,
    ) -> OwnerResult {
        if !self.next.settling_turn_is(run, settlement) {
            return Err(OwnerRejection::IgnoredLate);
        }
        self.next.ports.turn_admission.insert(
            settlement.operation,
            PendingTurnAdmission::Fenced { run, settlement },
        );
        self.end_settlement(
            run,
            settlement,
            SessionCellEvent::SettleDeadlineReached(settlement),
        )?;
        self.continue_close()?;
        self.drive_prompt_line()
    }

    pub(super) fn retire_settlement(&mut self, settlement: TurnSettlement) -> OwnerResult {
        let run = self
            .next
            .ports
            .turn_admission
            .get(&settlement.operation)
            .copied()
            .ok_or(OwnerRejection::IgnoredLate)?
            .run();
        self.end_settlement(
            run,
            settlement,
            SessionCellEvent::SettlementRetired(settlement),
        )
    }

    pub(super) fn start_successor_after_settlement(
        &mut self,
        run: SessionGeneration,
        previous: TurnSettlement,
        turn: TurnId,
    ) -> OwnerResult {
        self.end_settlement(
            run,
            previous,
            SessionCellEvent::ProviderStartedSuccessor { previous, turn },
        )
    }

    fn end_settlement(
        &mut self,
        run: SessionGeneration,
        settlement: TurnSettlement,
        event: SessionCellEvent,
    ) -> OwnerResult {
        let witness = self
            .next
            .ports
            .settling_witness
            .take()
            .filter(|last| last.turn == settlement.turn)
            .ok_or(OwnerRejection::IgnoredLate)?;
        let effects = self.cell_move(event)?;
        self.release(SessionOwnerEffect::Publish(SessionPublication::TurnEnded {
            generation: run,
            turn: settlement.turn,
            outcome: witness.outcome.clone(),
            idle: self.is_live(run),
        }));
        self.next.ports.last_live_turn = Some(witness);
        self.apply_entry_effects(effects)
    }
}
