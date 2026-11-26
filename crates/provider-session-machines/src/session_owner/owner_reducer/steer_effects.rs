//! Admit, mark, and settle a steer while retaining the caller's intent.

use super::{OwnerRejection, OwnerResult, OwnerTransition, remote_effect};
use crate::session_cell_machine::{SessionCellEffect, SessionCellEvent};
use crate::session_owner::{
    AuthorizedWork, DeliveryPlacementKind, EffectInProgress, EffectOutcome, InputWait, OwnerEffect,
    OwnerInput, PendingSteer, RemoteEffect, RevokeReason, SessionDeadline, SessionOwnerEffect,
    SessionOwnerRefusal, SessionStoreRequest, SteerOrigin, WireOutcome,
};
use crate::session_turn_machine::{SessionTurn, SessionTurnEffect};

impl OwnerTransition {
    pub(super) fn unmark_steer(&mut self, input: &OwnerInput) {
        self.release(SessionOwnerEffect::Store(
            SessionStoreRequest::UndoDispatch(input.clone()),
        ));
        self.wait_for_steer(input.clone(), InputWait::UndoStore);
    }

    pub(super) fn unmark_steer_unsent(&mut self, input: &OwnerInput, reason: RevokeReason) {
        self.next.ports.unsent_prompts.insert(input.clone(), reason);
        self.unmark_steer(input);
    }

    pub(super) fn admit_existing_steer(
        &mut self,
        input: OwnerInput,
        supported: bool,
        origin: SteerOrigin,
    ) -> OwnerResult {
        self.require_live(input.generation)?;
        match (origin, self.next.turn()) {
            (_, SessionTurn::Running { turn }) if turn == input.turn => {}
            (SteerOrigin::Named, SessionTurn::Idle) => {
                return Err(SessionOwnerRefusal::NoActiveTurn.into());
            }
            (SteerOrigin::Named, SessionTurn::Running { turn }) => {
                return Err(SessionOwnerRefusal::NamedSteerTurnChanged { actual: turn }.into());
            }
            _ => return Err(SessionOwnerRefusal::GenerationChanged.into()),
        }
        self.require_new_input(&input.input)?;
        if self.next.cell.settings_held() {
            return Err(SessionOwnerRefusal::SettingsUnresolved.into());
        }
        if self.next.steer.is_some() {
            return Err(SessionOwnerRefusal::EffectInProgress(EffectInProgress::Steer).into());
        }
        let effects = self.cell_move(SessionCellEvent::SteerRequested {
            agent_steers: supported,
        })?;
        if !effects.contains(&SessionCellEffect::Turn(SessionTurnEffect::InjectSteer {
            turn: input.turn,
        })) {
            return Err(SessionOwnerRefusal::GenerationChanged.into());
        }
        self.next.ports.admitted_inputs.insert(input.input.clone());
        self.next.steer = Some(PendingSteer {
            input: input.clone(),
            wait: InputWait::Admission,
            origin,
        });
        if !self.await_delivery_evidence(&input, DeliveryPlacementKind::Steer) {
            self.start_steer_mark(input);
        }
        Ok(())
    }

    pub(super) fn start_steer_mark(&mut self, input: OwnerInput) {
        self.release(SessionOwnerEffect::Store(
            SessionStoreRequest::MarkSteerDispatch(input.clone()),
        ));
        self.wait_for_steer(input, InputWait::DispatchStore);
    }

    pub(super) fn steer_marked(
        &mut self,
        input: &OwnerInput,
        outcome: EffectOutcome,
    ) -> OwnerResult {
        let origin = self
            .next
            .steer
            .as_ref()
            .filter(|pending| &pending.input == input && pending.wait == InputWait::DispatchStore)
            .map(|pending| pending.origin)
            .ok_or(OwnerRejection::IgnoredLate)?;
        if outcome != EffectOutcome::Applied {
            // The terminal offer follows any accepted mark in the lane.
            self.next.steer = None;
            self.reply_input(input.clone(), RemoteEffect::None);
        } else if self.is_live(input.generation)
            && self.next.turn() == (SessionTurn::Running { turn: input.turn })
        {
            self.next.steer = Some(PendingSteer {
                input: input.clone(),
                wait: InputWait::Enqueue,
                origin,
            });
            self.issue_authorization(AuthorizedWork::Steer {
                input: input.clone(),
                origin,
            })?;
        } else if origin == SteerOrigin::Named
            && let Some(reason) = self.named_steer_target_loss(input)
        {
            self.unmark_steer_unsent(input, reason);
        } else {
            self.unmark_steer(input);
        }
        Ok(())
    }

    pub(super) fn finish_steer(
        &mut self,
        input: &OwnerInput,
        outcome: WireOutcome<()>,
    ) -> OwnerResult {
        if outcome == WireOutcome::Refused {
            return self.refuse_steer(input, RevokeReason::ProviderSteerRefused);
        }
        if !self.steer_is_pending(input, InputWait::Wire) {
            return Err(OwnerRejection::IgnoredLate);
        }
        if outcome == WireOutcome::Confirmed(())
            && self.is_live(input.generation)
            && matches!(self.next.turn(), SessionTurn::Running { turn } | SessionTurn::Cancelling { turn } if turn == input.turn)
        {
            self.next.ports.recent_steer = Some(input.clone());
            self.offer_commit(vec![super::super::PublicationFact::Stream {
                run: input.generation,
                event: session_event_model::SessionEvent::InputAccepted {
                    input_id: input.input.clone(),
                    turn_id: input.turn.0.to_string(),
                },
            }]);
            self.effects
                .push(OwnerEffect::SteerSubmitted(input.clone()));
        }
        self.next.steer = None;
        self.reply_input(input.clone(), remote_effect(&outcome));
        Ok(())
    }

    pub(super) fn refuse_steer(&mut self, input: &OwnerInput, reason: RevokeReason) -> OwnerResult {
        if !self.steer_is_pending(input, InputWait::Wire) {
            return Err(OwnerRejection::IgnoredLate);
        }
        self.next.steer = None;
        self.next.ports.unsent_prompts.insert(input.clone(), reason);
        self.reply_input(input.clone(), RemoteEffect::None);
        Ok(())
    }

    pub(super) fn steer_is_pending(&self, input: &OwnerInput, wait: InputWait) -> bool {
        self.next
            .steer
            .as_ref()
            .is_some_and(|pending| &pending.input == input && pending.wait == wait)
    }

    fn wait_for_steer(&mut self, input: OwnerInput, wait: InputWait) {
        if let Some(pending) = self
            .next
            .steer
            .as_mut()
            .filter(|pending| pending.input == input)
        {
            pending.wait = wait;
        }
        self.release(SessionOwnerEffect::ArmDeadline(SessionDeadline::Steer {
            input,
            wait,
        }));
    }

    pub(super) fn steer_deadline(&mut self, input: &OwnerInput, wait: InputWait) -> OwnerResult {
        if !self.steer_is_pending(input, wait) {
            return Err(OwnerRejection::IgnoredLate);
        }
        match wait {
            InputWait::Admission | InputWait::Enqueue => Err(OwnerRejection::IgnoredLate),
            InputWait::DispatchStore => self.steer_marked(input, EffectOutcome::Unknown),
            InputWait::UndoStore => self.dispatch_unmarked(input, EffectOutcome::Unknown),
            InputWait::Wire => self.finish_steer(input, WireOutcome::Unknown),
        }
    }
}
