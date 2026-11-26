//! Work offers and evidence for the scripted harness.

use super::{ScriptObservation, ScriptedBinding, lock, record};
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{HandOverRefusal, HarnessContent, RunRetirement, SlotEvidence, WorkReply},
    session_owner::{
        CancelDrainReport, CancelDrainStart, CancelDrainWait, DispatchAuthorization,
        DispatchResult, HarnessBinding, PromptCompletion, ReplyAdmission, ReservedWorkReply,
        SettingOffer, SteerIdlePolicy, TakeBackRefusal, WireOutcome, WorkOfferRefusal,
    },
    session_setting_machine::SettingValueId,
    session_turn_machine::TurnId,
};
use session_event_model::StopReason;

impl HarnessBinding for ScriptedBinding {
    fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        _turn: TurnId,
        _content: HarnessContent<'_>,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        let mut state = lock(&self.state);
        if authorization.incarnation != self.address.incarnation
            || state.active != Some(authorization.run)
        {
            return Err(WorkOfferRefusal::RouteUnavailable);
        }
        if state.offered.is_some() {
            return Err(WorkOfferRefusal::OtherAuthorizationOffered);
        }
        state.record_slot(&authorization, SlotEvidence::Offered);
        state.offered = Some((authorization, reply));
        Ok(())
    }

    fn offer_steer(
        &self,
        _authorization: DispatchAuthorization,
        _content: HarnessContent<'_>,
        _idle: SteerIdlePolicy,
        _reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        Err(WorkOfferRefusal::RouteUnavailable)
    }

    fn offer_setting(
        &self,
        _authorization: DispatchAuthorization,
        _kind: ProviderSettingKind,
        _value: SettingValueId,
        _reply: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal> {
        Err(WorkOfferRefusal::SettingSetterUnavailable)
    }

    fn hand_over(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: &mut dyn FnMut() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        let mut state = lock(&self.state);
        let Some((authorization, reply)) = state.offered.take_if(|offer| &offer.0 == authorization)
        else {
            return Err(HandOverRefusal::NoSuchOffer);
        };
        if !still_requested() {
            state.record_slot(&authorization, SlotEvidence::Revoked);
            return Ok(DispatchResult::DefinitelyUnsent(authorization));
        }
        state.record_slot(&authorization, SlotEvidence::Taken);
        record(
            &self.journal,
            ScriptObservation::PromptTaken(authorization.clone()),
        );
        reply.send(WorkReply::Dispatch(DispatchResult::PromptFinished {
            authorization: authorization.clone(),
            outcome: WireOutcome::Confirmed(PromptCompletion {
                stop_reason: StopReason::EndTurn,
                local_cause: None,
                response: "scripted answer".into(),
            }),
        }));
        Ok(DispatchResult::Taken(authorization))
    }

    fn take_back(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TakeBackRefusal> {
        let mut state = lock(&self.state);
        if state
            .dispatches
            .iter()
            .any(|(recorded, slot)| recorded == authorization && *slot == SlotEvidence::Taken)
        {
            return Ok(DispatchResult::AlreadyTaken(authorization.clone()));
        }
        let (authorization, _) = state
            .offered
            .take_if(|offer| &offer.0 == authorization)
            .ok_or(TakeBackRefusal::NoRecord)?;
        state.record_slot(&authorization, SlotEvidence::Revoked);
        Ok(DispatchResult::Revoked(authorization))
    }

    fn begin_cancel(
        &mut self,
        _authorization: DispatchAuthorization,
        _reserve: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        _retired: RunRetirement,
    ) -> CancelDrainStart {
        CancelDrainStart::AlreadyDrained
    }

    fn send_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        _wait: CancelDrainWait,
    ) -> DispatchResult {
        DispatchResult::CancelTaken {
            authorization,
            drain: CancelDrainReport::Drained,
        }
    }
}
