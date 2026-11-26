//! Pass Host and provider interaction facts through the session owner mailbox.

use crate::{
    acp_interaction_port::RefusedTurnCancellation,
    external_provider_approval_context::{ExternalProviderApprovalContext, OwnerApprovalScope},
    session_owner_door::SessionOwnerDoor,
    session_owner_lifecycle_reports::inline_admission,
};
use acp_client_runtime::InteractionFuture;
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        CallbackAdmission, HarnessCallbackIngress, HarnessSessionId, SessionHarnessPayloads,
    },
    session_lifecycle_machine::SessionLifecycle,
    session_owner::OwnerEvent,
    session_turn_machine::SessionTurn,
};
use session_event_model::{SessionEvent, SkillView};
use std::sync::{Arc, OnceLock, Weak};

#[derive(Default)]
pub(crate) struct SessionOwnerIngress {
    door: OnceLock<Weak<SessionOwnerDoor>>,
}

impl SessionOwnerIngress {
    pub(crate) fn attach(&self, door: &Arc<SessionOwnerDoor>) {
        let _attached = self.door.set(Arc::downgrade(door));
    }

    fn owner(&self, session: &str) -> Option<Arc<crate::session_owner_door::SessionOwnerAddress>> {
        self.door
            .get()?
            .upgrade()?
            .existing_provider_session(session)
    }
}

impl RefusedTurnCancellation for SessionOwnerIngress {
    fn cancel_refused_turn(
        &self,
        session: String,
        scope: OwnerApprovalScope,
    ) -> InteractionFuture<'_, ()> {
        Box::pin(async move {
            if let Some(owner) = self.owner(&session) {
                let _settlement = owner.cancel_turn(scope.run, scope.turn).await;
            }
        })
    }
}

impl HarnessCallbackIngress<SessionHarnessPayloads<ExternalProviderApprovalContext>>
    for SessionOwnerIngress
{
    fn report_for_turn(
        &self,
        session: &HarnessSessionId,
        generation: SessionGeneration,
        turn: provider_session_machines::session_turn_machine::TurnId,
        event: SessionEvent,
    ) -> CallbackAdmission {
        let Some(owner) = self.owner(&session.0) else {
            return CallbackAdmission::Ended;
        };
        let snapshot = owner.snapshots().borrow().clone();
        if !matches!(snapshot.lifecycle,
            SessionLifecycle::Live { generation: live } | SessionLifecycle::Closing { generation: live, .. } if live == generation
        ) || !matches!(snapshot.turn, SessionTurn::Running { turn: active } | SessionTurn::Cancelling { turn: active } if active == turn)
        {
            return CallbackAdmission::Ended;
        }
        inline_admission(owner.report_event(OwnerEvent::Stream {
            run: generation,
            expected_turn: Some(turn),
            event,
        }))
    }

    fn report_skill_view(&self, session: &HarnessSessionId, view: SkillView) -> CallbackAdmission {
        let Some(owner) = self.owner(&session.0) else {
            return CallbackAdmission::Ended;
        };
        let run = self.current_generation(session);
        inline_admission(owner.report_event(OwnerEvent::SkillView { run, view }))
    }

    fn current_generation(&self, session: &HarnessSessionId) -> SessionGeneration {
        let Some(owner) = self.owner(&session.0) else {
            return SessionGeneration::UNTRACKED;
        };
        match owner.snapshots().borrow().lifecycle {
            SessionLifecycle::Live { generation }
            | SessionLifecycle::Closing { generation, .. } => generation,
            _ => SessionGeneration::UNTRACKED,
        }
    }

    fn report_for_generation(
        &self,
        session: &HarnessSessionId,
        generation: SessionGeneration,
        event: SessionEvent,
    ) -> CallbackAdmission {
        let Some(owner) = self.owner(&session.0) else {
            return CallbackAdmission::Ended;
        };
        let snapshot = owner.snapshots().borrow().clone();
        if !matches!(snapshot.lifecycle,
            SessionLifecycle::Live { generation: live } | SessionLifecycle::Closing { generation: live, .. } if live == generation
        ) {
            return CallbackAdmission::Ended;
        }
        let turn = match snapshot.turn {
            SessionTurn::Idle => None,
            SessionTurn::Running { turn } | SessionTurn::Cancelling { turn } => Some(turn),
        };
        inline_admission(owner.report_event(OwnerEvent::Stream {
            run: generation,
            expected_turn: turn,
            event,
        }))
    }
}
