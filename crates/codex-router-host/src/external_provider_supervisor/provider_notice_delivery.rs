//! Resolves and submits claimed notices through the existing ACP delivery owner.

use super::{
    ConversationOperationFailureKind, ExternalProviderSupervisor, ProviderOperationEffect,
    provider_prompt_contents::ProviderPromptContentsRequest,
};
use crate::{
    session_owner_calls::InputAcceptance,
    session_owner_notice::{
        NoticePlacement, NoticeRun, NoticeWriterTake, notice_waits_for_session,
    },
};
use collaboration_protocol::{OperationId, SessionRef};
use provider_session_machines::{
    session_lifecycle_machine::SessionLifecycle,
    session_owner::{RevokeReason, SessionSnapshot},
    session_turn_machine::SessionTurn,
};
use std::sync::Arc;
use switchboard_message_custody::{
    handover::{NoticePreTakeRefusal, NoticeUncertainty, NoticeWriteOutcome},
    handover_notice::{
        ClaimedNotice, MachineId, ManagedNoticePort, ManagedNoticeRun, NoticeDeferred,
        NoticeFuture, SessionRef as MailSessionRef,
    },
};

/// Resolves one exact managed ACP owner for a recipient already admitted by custody.
pub struct ManagedAcpNoticeAdapter {
    supervisor: Arc<ExternalProviderSupervisor>,
    machine: MachineId,
}

impl ManagedAcpNoticeAdapter {
    pub fn new(supervisor: Arc<ExternalProviderSupervisor>, machine: MachineId) -> Self {
        Self {
            supervisor,
            machine,
        }
    }
}

impl ManagedNoticePort for ManagedAcpNoticeAdapter {
    fn ready<'a>(
        &'a self,
        target: &'a MailSessionRef,
    ) -> NoticeFuture<'a, Result<Box<dyn ManagedNoticeRun>, NoticeDeferred>> {
        Box::pin(async move {
            if target.machine_id != self.machine {
                return Err(NoticeDeferred::UnprovedRecipient);
            }
            let id =
                collaboration_protocol::SessionId::try_from(target.session_id.as_str().to_owned())
                    .map_err(|_| NoticeDeferred::UnprovedRecipient)?;
            let mut owners =
                self.supervisor
                    .inner
                    .bindings
                    .iter()
                    .filter_map(|(endpoint, binding)| {
                        let target = SessionRef {
                            endpoint: endpoint.clone(),
                            session_id: id.clone(),
                        };
                        let owner = binding.owners.existing(&target)?;
                        let snapshots = owner.snapshots();
                        let snapshot = snapshots.borrow().clone();
                        Some((target, snapshots, snapshot))
                    });
            let (local_target, snapshots, snapshot) =
                owners.next().ok_or(NoticeDeferred::NotOpen)?;
            if owners.next().is_some() {
                return Err(NoticeDeferred::UnprovedRecipient);
            }
            let SessionLifecycle::Live { generation } = snapshot.lifecycle else {
                return Err(NoticeDeferred::NotOpen);
            };
            if notice_waits_for_session(&snapshot) {
                return Err(NoticeDeferred::WaitingForSession);
            }
            let placement = match snapshot.turn {
                SessionTurn::Idle => NoticePlacement::Direct,
                SessionTurn::Running { turn } => NoticePlacement::Steer(turn),
                SessionTurn::Cancelling { .. } => return Err(NoticeDeferred::WaitingForSession),
            };
            Ok(Box::new(OwnedManagedNotice {
                supervisor: self.supervisor.clone(),
                target: target.clone(),
                local_target,
                run: NoticeRun {
                    owner: snapshot.owner,
                    generation,
                    placement,
                },
                clock: self.supervisor.inner.clock.clone(),
                snapshots,
            }) as Box<dyn ManagedNoticeRun>)
        })
    }
}

struct OwnedManagedNotice {
    supervisor: Arc<ExternalProviderSupervisor>,
    target: MailSessionRef,
    local_target: SessionRef,
    run: NoticeRun,
    clock: Arc<dyn router_clock::RuntimeClock>,
    snapshots: tokio::sync::watch::Receiver<SessionSnapshot>,
}

impl ManagedNoticeRun for OwnedManagedNotice {
    fn offer(self: Box<Self>, notice: ClaimedNotice) -> NoticeFuture<'static, NoticeWriteOutcome> {
        Box::pin(async move {
            use NoticePreTakeRefusal::{CannotSteer, Expired, RejectedBeforeTake, RunChanged};
            if notice.target() != &self.target {
                return NoticeWriteOutcome::DefinitelyNotWritten(RunChanged);
            }
            if self.clock.monotonic_now() >= notice.not_after() {
                return NoticeWriteOutcome::DefinitelyNotWritten(Expired);
            }
            let record = match self
                .supervisor
                .inner
                .store
                .lock()
                .await
                .session_record(&self.local_target)
                .await
            {
                Ok(Some(record)) => record,
                Ok(None) | Err(_) => {
                    return NoticeWriteOutcome::DefinitelyNotWritten(RejectedBeforeTake);
                }
            };
            let Ok(content) = session_event_model::PromptContent::text(notice.text()) else {
                return NoticeWriteOutcome::DefinitelyNotWritten(RejectedBeforeTake);
            };
            let (Ok(operation_id), Ok(input_id)) = (
                OperationId::try_from(String::from(notice.attempt())),
                session_event_model::InputId::new(String::from(notice.attempt())),
            ) else {
                return NoticeWriteOutcome::DefinitelyNotWritten(RejectedBeforeTake);
            };
            let request = ProviderPromptContentsRequest {
                operation_id,
                input_id,
                target: self.local_target,
                requested_by: record.created_by,
                approver: record.approver,
                input: acp_client_runtime::ProviderTurnInput::Prompt(
                    session_event_model::ProviderSubmission::plain(vec![content]),
                ),
                busy: super::PromptBusyPolicy::RefuseBusy,
            };
            let take = NoticeWriterTake::new(notice, self.clock, self.snapshots);
            let accepted = self
                .supervisor
                .submit_owned_notice(request, self.run, take.clone())
                .await;
            if let Some(taken_at) = take.taken_at() {
                if matches!(self.run.placement, NoticePlacement::Direct) {
                    return NoticeWriteOutcome::Written { taken_at };
                }
                return match accepted {
                    Ok(InputAcceptance::Steered { .. } | InputAcceptance::ProviderStarted) => {
                        NoticeWriteOutcome::Written { taken_at }
                    }
                    Ok(InputAcceptance::Unsent {
                        reason: Some(RevokeReason::ProviderSteerNeedsPrompt),
                    }) => NoticeWriteOutcome::SteerNotInjected,
                    Ok(InputAcceptance::Unsent {
                        reason: Some(RevokeReason::ProviderSteerRefused),
                    }) => NoticeWriteOutcome::SteerRefused,
                    _ => NoticeWriteOutcome::NotKnown(NoticeUncertainty::WriterOutcomeLost),
                };
            }
            match accepted {
                Ok(InputAcceptance::Unsent { .. }) => {
                    NoticeWriteOutcome::DefinitelyNotWritten(RejectedBeforeTake)
                }
                Err(error) if error.effect == ProviderOperationEffect::None => {
                    let reason = if error.kind == ConversationOperationFailureKind::CannotSteer {
                        CannotSteer
                    } else {
                        RejectedBeforeTake
                    };
                    NoticeWriteOutcome::DefinitelyNotWritten(reason)
                }
                Ok(
                    InputAcceptance::Started
                    | InputAcceptance::Unknown
                    | InputAcceptance::QueuePlanChanged
                    | InputAcceptance::Queued
                    | InputAcceptance::ProviderStarted
                    | InputAcceptance::Steered { .. },
                )
                | Err(_) => NoticeWriteOutcome::NotKnown(NoticeUncertainty::WriterOutcomeLost),
            }
        })
    }
}
