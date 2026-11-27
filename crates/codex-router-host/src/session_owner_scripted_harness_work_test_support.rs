//! Work offers and evidence for the scripted harness.

use super::{ScriptObservation, ScriptedBinding, ScriptedWork, lock, record};
use provider_session_machines::{
    ProviderSettingKind,
    harness_lifecycle::{HandOverRefusal, HarnessContent, RunRetirement, SlotEvidence, WorkReply},
    session_owner::{
        AuthorizationTarget, CancelDrainReport, CancelDrainStart, CancelDrainWait,
        DispatchAuthorization, DispatchResult, HarnessBinding, PromptCompletion, ReplyAdmission,
        ReservedWorkReply, SettingOffer, SteerIdlePolicy, TakeBackRefusal, WireOutcome,
        WorkOfferRefusal,
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
        self.record_work_offer(authorization, reply)
    }

    fn offer_steer(
        &self,
        authorization: DispatchAuthorization,
        _content: HarnessContent<'_>,
        _idle: SteerIdlePolicy,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        if self.controls.work != ScriptedWork::RefuseObservedLease {
            return Err(WorkOfferRefusal::RouteUnavailable);
        }
        self.record_work_offer(authorization, reply)
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
        if self.controls.work == ScriptedWork::UnavailableRecovery {
            record(
                &self.journal,
                ScriptObservation::PromptHeld(authorization.clone()),
            );
            state.offered = Some((authorization, reply));
            return Err(HandOverRefusal::WriterStopped);
        }
        state.record_slot(&authorization, SlotEvidence::Taken);
        if matches!(
            authorization.target,
            AuthorizationTarget::ExistingTurn { .. }
        ) {
            let turn = TurnId(uuid::Uuid::now_v7());
            record(
                &self.journal,
                ScriptObservation::SteerTaken(authorization.clone(), turn),
            );
            reply.send(WorkReply::Dispatch(DispatchResult::SteerStartedNewTurn {
                authorization: authorization.clone(),
                turn,
            }));
            return Ok(DispatchResult::Taken(authorization));
        }
        record(
            &self.journal,
            ScriptObservation::PromptTaken(authorization.clone()),
        );
        if self.controls.work == ScriptedWork::Complete {
            finish_prompt(authorization.clone(), reply);
        } else {
            state.prompt_reply = Some((authorization.clone(), reply));
            if let (Some(policy), Some((turn, context))) =
                (&self.controls.policy, state.approval.take())
            {
                state.pending_request = Some(super::loss::PendingRequest::start(
                    &self.tasks,
                    policy.clone(),
                    turn,
                    context,
                ));
            }
        }
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

impl ScriptedBinding {
    fn record_work_offer(
        &self,
        authorization: DispatchAuthorization,
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
        if self.controls.work != ScriptedWork::Complete
            && authorization.target == AuthorizationTarget::NewTurn
        {
            record(
                &self.journal,
                ScriptObservation::PromptOffered(authorization.clone()),
            );
        }
        state.offered = Some((authorization, reply));
        Ok(())
    }
}

pub(super) fn finish_prompt(authorization: DispatchAuthorization, reply: ReservedWorkReply) {
    reply.send(WorkReply::Dispatch(DispatchResult::PromptFinished {
        authorization,
        outcome: WireOutcome::Confirmed(PromptCompletion {
            stop_reason: StopReason::EndTurn,
            local_cause: None,
            response: "scripted answer".into(),
        }),
    }));
}
