//! The ACP implementation of one session owner's harness work calls.

use crate::{
    InteractionPort, OwnerApprovalBindings, OwnerCancelDrainExecutor, owner_wire::OwnerAcpWire,
    provider_prompt_content::acp_blocks_from_prompt_content,
};
use std::sync::Arc;
use switchboard_harness_contracts::{
    CancelDrainStart, CancelDrainWait, DispatchAuthorization, DispatchResult, HandOverRefusal,
    HarnessBinding, HarnessContent, ProviderSettingKind, ReplyAdmission, ReservedWorkReply,
    RunRetirement, SettingOffer, SettingValueId, SteerIdlePolicy, TakeBackRefusal, TurnId,
    WorkOfferRefusal, WorkReply,
};
use tokio_util::task::TaskTracker;

pub struct OwnerAcpBinding<Port: InteractionPort> {
    wire: Arc<OwnerAcpWire>,
    provider_session: String,
    cancel_drains: OwnerCancelDrainExecutor<Port>,
    tasks: TaskTracker,
}

impl<Port: InteractionPort> OwnerAcpBinding<Port> {
    #[must_use]
    pub fn new(
        wire: Arc<OwnerAcpWire>,
        provider_session: String,
        approvals: OwnerApprovalBindings<Port>,
        tasks: TaskTracker,
    ) -> Self {
        Self {
            wire,
            provider_session,
            cancel_drains: OwnerCancelDrainExecutor::new(approvals),
            tasks,
        }
    }
}

impl<Port: InteractionPort> HarnessBinding for OwnerAcpBinding<Port> {
    fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        turn: TurnId,
        content: HarnessContent<'_>,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        self.wire.offer_work_prompt(
            authorization,
            turn,
            self.provider_session.clone(),
            acp_blocks_from_prompt_content(content.blocks().to_vec()),
            matches!(content, HarnessContent::Compaction(_)),
            reply,
        )
    }

    fn offer_steer(
        &self,
        authorization: DispatchAuthorization,
        content: HarnessContent<'_>,
        idle: SteerIdlePolicy,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        self.wire.offer_work_steer(
            authorization,
            self.provider_session.clone(),
            acp_blocks_from_prompt_content(content.blocks().to_vec()),
            idle,
            reply,
        )
    }

    fn offer_setting(
        &self,
        authorization: DispatchAuthorization,
        kind: ProviderSettingKind,
        wanted: SettingValueId,
        reply: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal> {
        self.wire.offer_work_setting(
            authorization,
            self.provider_session.clone(),
            kind,
            wanted.0,
            reply,
        )
    }

    fn hand_over(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: &mut dyn FnMut() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        self.wire.hand_over_work(authorization, still_requested)
    }

    fn take_back(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TakeBackRefusal> {
        self.cancel_drains.discard(authorization);
        self.wire
            .revoke(authorization)
            .map_err(|_| TakeBackRefusal::NoRecord)
    }

    fn begin_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        retired: RunRetirement,
    ) -> CancelDrainStart {
        self.cancel_drains.begin_work(
            &self.provider_session,
            authorization,
            reserve_reply,
            &self.tasks,
            retired,
        )
    }

    fn send_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        wait: CancelDrainWait,
    ) -> DispatchResult {
        self.cancel_drains.take(
            &self.wire,
            self.provider_session.clone(),
            authorization,
            wait == CancelDrainWait::Expired,
        )
    }
}
