//! Owns delivery submission through owner acknowledgement and settlement.

use super::{
    ConversationOperationFailureKind, ConversationOperationFailureStage,
    ExternalProviderSupervisor, PreparedOperation, ProviderOperationCompletion,
    ProviderOperationEffect, ProviderOperationKind, failure,
    provider_admission_policy::{AdmissionClass, AdmissionMode},
    provider_owner_operations::owner_failure,
    provider_owner_prompt::{SessionQueuePlan, WarmRun, explicit_steer_refusal, retained_prompt},
    provider_prompt_contents::ProviderPromptContentsRequest,
    runtime_failure,
};
use crate::{
    session_owner_calls::InputAcceptance,
    session_owner_delivery_evidence::{
        OwnerDeliveryEvidence, ProviderDeliveryEvidence, placement_evidence,
    },
    session_owner_execution::NoticeWriterTake,
};
use collaboration_contracts::AttemptEvidenceSink;
use collaboration_protocol::{EndpointRef, MessageDelivery, OperationId, ProviderKind, SessionRef};
use provider_session_machines::{
    session_lifecycle_machine::SessionLifecycle,
    session_owner::{OwnerInput, PromptAdmission, SessionOwnerReply},
    session_turn_machine::{SessionTurn, TurnId},
};
use std::sync::Arc;
use switchboard_message_custody::{
    handover::{NoticePreTakeRefusal, NoticeUncertainty, NoticeWriteOutcome},
    handover_notice::{
        ClaimedNotice, IdleClaudeRun, MachineId, ManagedClaudeNoticePort, NoticeDeferred,
        NoticeFuture, SessionRef as MailSessionRef,
    },
};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum DeliveryAdmission {
    #[cfg(test)]
    Direct,
    Requested(MessageDelivery),
    SteerExisting(TurnId),
    QueueOnRun(SessionQueuePlan),
    Warm(WarmRun),
    Notice(WarmRun),
}

/// Resolves only the configured managed Claude endpoint and its current owner.
pub struct ManagedClaudeNoticeAdapter {
    supervisor: Arc<ExternalProviderSupervisor>,
    machine: MachineId,
    endpoint: EndpointRef,
}

impl ManagedClaudeNoticeAdapter {
    pub fn new(
        supervisor: Arc<ExternalProviderSupervisor>,
        machine: MachineId,
        endpoint: EndpointRef,
    ) -> Self {
        Self {
            supervisor,
            machine,
            endpoint,
        }
    }
}

impl ManagedClaudeNoticePort for ManagedClaudeNoticeAdapter {
    fn ready<'a>(
        &'a self,
        target: &'a MailSessionRef,
    ) -> NoticeFuture<'a, Result<Box<dyn IdleClaudeRun>, NoticeDeferred>> {
        Box::pin(async move {
            if target.machine_id != self.machine {
                return Err(NoticeDeferred::UnprovedRecipient);
            }
            let binding = self
                .supervisor
                .inner
                .bindings
                .get(&self.endpoint)
                .ok_or(NoticeDeferred::UnsupportedProgram)?;
            if binding.identity.runtime.provider != ProviderKind::ClaudeCode {
                return Err(NoticeDeferred::UnsupportedProgram);
            }
            let local_target = SessionRef {
                endpoint: self.endpoint.clone(),
                session_id: collaboration_protocol::SessionId::try_from(
                    target.session_id.as_str().to_owned(),
                )
                .map_err(|_| NoticeDeferred::UnprovedRecipient)?,
            };
            let snapshot = self
                .supervisor
                .owner_session_snapshot(&local_target)
                .ok_or(NoticeDeferred::NotOpen)?;
            let SessionLifecycle::Live { generation } = snapshot.lifecycle else {
                return Err(NoticeDeferred::NotOpen);
            };
            if snapshot.turn != SessionTurn::Idle {
                return Err(NoticeDeferred::Busy);
            }
            Ok(Box::new(IdleManagedClaude {
                supervisor: self.supervisor.clone(),
                target: target.clone(),
                local_target,
                run: WarmRun {
                    owner: snapshot.owner,
                    generation,
                },
                clock: binding.runtime.owner_clock(),
            }) as Box<dyn IdleClaudeRun>)
        })
    }
}

struct IdleManagedClaude {
    supervisor: Arc<ExternalProviderSupervisor>,
    target: MailSessionRef,
    local_target: SessionRef,
    run: WarmRun,
    clock: Arc<dyn router_clock::RuntimeClock>,
}

impl IdleClaudeRun for IdleManagedClaude {
    fn offer(self: Box<Self>, notice: ClaimedNotice) -> NoticeFuture<'static, NoticeWriteOutcome> {
        Box::pin(async move {
            use NoticePreTakeRefusal::{Busy, Expired, RejectedBeforeTake, RunChanged};
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
            let request = ProviderPromptContentsRequest {
                operation_id: OperationId::generate(),
                input_id: session_event_model::InputId::generate(),
                target: self.local_target,
                requested_by: record.created_by,
                approver: record.approver,
                input: acp_client_runtime::ProviderTurnInput::Prompt(
                    session_event_model::ProviderSubmission::plain(vec![content]),
                ),
                busy: super::PromptBusyPolicy::RefuseBusy,
            };
            let take = NoticeWriterTake::new(notice, self.clock);
            let accepted = self
                .supervisor
                .submit_owner_delivery_with_notice(
                    request,
                    DeliveryAdmission::Notice(self.run),
                    None,
                    Some(take.clone()),
                )
                .await;
            if let Some(taken_at) = take.taken_at() {
                return NoticeWriteOutcome::Written { taken_at };
            }
            match accepted {
                Ok(InputAcceptance::Unsent { .. }) => {
                    NoticeWriteOutcome::DefinitelyNotWritten(RejectedBeforeTake)
                }
                Err(error) if error.effect == ProviderOperationEffect::None => {
                    let reason = if error.kind == ConversationOperationFailureKind::Busy {
                        Busy
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

impl ExternalProviderSupervisor {
    #[expect(
        clippy::result_large_err,
        reason = "Retains a typed conversation failure for delivery."
    )]
    pub(crate) async fn submit_owned_delivery(
        &self,
        request: ProviderPromptContentsRequest,
        mode: MessageDelivery,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        self.submit_owner_delivery_request(request, DeliveryAdmission::Requested(mode), None)
            .await
    }

    #[expect(clippy::result_large_err, reason = "Retains the typed steer refusal.")]
    pub(crate) async fn submit_existing_turn_steer(
        &self,
        request: ProviderPromptContentsRequest,
        turn: TurnId,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        self.submit_owner_delivery_request(request, DeliveryAdmission::SteerExisting(turn), None)
            .await
    }

    #[expect(
        clippy::result_large_err,
        reason = "Retains a typed conversation failure for delivery."
    )]
    pub(crate) async fn submit_queued_on_run(
        &self,
        request: ProviderPromptContentsRequest,
        run: SessionQueuePlan,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        self.submit_owner_delivery_request(request, DeliveryAdmission::QueueOnRun(run), None)
            .await
    }

    #[expect(
        clippy::result_large_err,
        reason = "Retains a typed conversation failure for delivery."
    )]
    #[cfg(test)]
    pub(super) async fn submit_owned_direct_delivery(
        &self,
        request: ProviderPromptContentsRequest,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        self.submit_owner_delivery_request(request, DeliveryAdmission::Direct, None)
            .await
    }

    pub(crate) async fn submit_warm_owner_prompt(
        &self,
        request: ProviderPromptContentsRequest,
        run: WarmRun,
    ) -> Result<crate::session_owner_calls::InputAcceptance, Box<super::ConversationOperationFailure>>
    {
        self.submit_owner_delivery_request(request, DeliveryAdmission::Warm(run), None)
            .await
            .map_err(Box::new)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Retains the existing typed delivery refusal."
    )]
    pub(crate) async fn submit_evidenced_delivery(
        &self,
        request: ProviderPromptContentsRequest,
        mode: MessageDelivery,
        queued_run: Option<SessionQueuePlan>,
        sink: &dyn AttemptEvidenceSink,
        effect: ProviderDeliveryEvidence,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        let admission = queued_run.map_or(
            DeliveryAdmission::Requested(mode),
            DeliveryAdmission::QueueOnRun,
        );
        self.submit_owner_delivery_request(request, admission, Some((sink, effect)))
            .await
    }

    #[expect(
        clippy::result_large_err,
        reason = "Retains a typed conversation failure for delivery."
    )]
    async fn submit_owner_delivery_request(
        &self,
        request: ProviderPromptContentsRequest,
        admission: DeliveryAdmission,
        evidence: Option<(&dyn AttemptEvidenceSink, ProviderDeliveryEvidence)>,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        self.submit_owner_delivery_with_notice(request, admission, evidence, None)
            .await
    }

    #[expect(
        clippy::result_large_err,
        reason = "Retains a typed conversation failure for delivery."
    )]
    async fn submit_owner_delivery_with_notice(
        &self,
        request: ProviderPromptContentsRequest,
        admission: DeliveryAdmission,
        evidence: Option<(&dyn AttemptEvidenceSink, ProviderDeliveryEvidence)>,
        notice: Option<NoticeWriterTake>,
    ) -> Result<crate::session_owner_calls::InputAcceptance, super::ConversationOperationFailure>
    {
        use crate::session_owner_calls::InputAcceptance;
        use provider_session_machines::session_owner::SessionOwnerCommand;

        let deadline = self.admission_deadline();
        let target = request.target.clone();
        let operation = request.operation_id.clone();
        let (binding, runtime) =
            self.runtime_binding(&target.endpoint, &operation, Some(target.clone()))?;
        let owner = if matches!(admission, DeliveryAdmission::Notice(_)) {
            let Some(owner) = self
                .owner_door(&target.endpoint)
                .and_then(|door| door.existing(&target))
            else {
                return Ok(InputAcceptance::Unsent {
                    reason: Some(provider_session_machines::session_owner::RevokeReason::NotLoaded),
                });
            };
            owner
        } else {
            self.session_owner_for(&target)
                .map_err(|error| owner_failure(operation.clone(), target.clone(), error))?
        };
        let snapshot = owner.snapshots().borrow().clone();
        let (generation, cold_restore) = match (admission, snapshot.lifecycle) {
            (_, SessionLifecycle::Live { generation })
            | (DeliveryAdmission::QueueOnRun(_), SessionLifecycle::Loading { generation, .. }) => {
                (generation, None)
            }
            (
                DeliveryAdmission::QueueOnRun(SessionQueuePlan::Restore { run, attempt }),
                SessionLifecycle::NotLoaded { last },
            ) if last.number().checked_add(1) == Some(run.generation.number()) => {
                (run.generation, Some(attempt))
            }
            (
                DeliveryAdmission::QueueOnRun(expected),
                SessionLifecycle::Closing { generation, .. },
            ) if expected.run().incarnation == snapshot.owner.incarnation
                && expected.run().generation == generation =>
            {
                return Ok(InputAcceptance::Unsent {
                    reason: Some(provider_session_machines::session_owner::RevokeReason::NotLoaded),
                });
            }
            (DeliveryAdmission::QueueOnRun(_), _) => return Ok(InputAcceptance::QueuePlanChanged),
            _ => {
                return Ok(InputAcceptance::Unsent {
                    reason: Some(provider_session_machines::session_owner::RevokeReason::NotLoaded),
                });
            }
        };
        if let DeliveryAdmission::QueueOnRun(expected) = admission
            && (expected.run().incarnation != snapshot.owner.incarnation
                || expected.run().generation != generation)
        {
            return Ok(InputAcceptance::QueuePlanChanged);
        }
        if let DeliveryAdmission::Warm(run) | DeliveryAdmission::Notice(run) = admission
            && (snapshot.owner != run.owner || generation != run.generation)
        {
            return Ok(InputAcceptance::Unsent { reason: None });
        }
        let preview = match &request.input {
            acp_client_runtime::ProviderTurnInput::Prompt(prompt) => {
                crate::provider_queue_operation_registry::queued_content_preview(&prompt.content)
            }
            acp_client_runtime::ProviderTurnInput::Compact => String::new(),
        };
        let id = Uuid::parse_str(operation.as_str()).map_err(|_| {
            failure(
                ConversationOperationFailureKind::InvalidRequest,
                ConversationOperationFailureStage::Admission,
                ProviderOperationEffect::None,
                "invalid operation identity",
                operation.clone(),
                Some(target.clone()),
            )
        })?;
        let provider_id = String::from(target.session_id.clone());
        let content = self
            .within_admission(
                deadline,
                &operation,
                Some(&target),
                runtime.prepare_owner_prompt(&provider_id, request.input),
            )
            .await?
            .map_err(|error| runtime_failure(operation.clone(), Some(target.clone()), error))?;
        let can_queue = matches!(
            admission,
            DeliveryAdmission::Requested(MessageDelivery::Auto | MessageDelivery::Queue)
                | DeliveryAdmission::QueueOnRun(_)
        );
        let class = match admission {
            DeliveryAdmission::Requested(MessageDelivery::Queue)
            | DeliveryAdmission::QueueOnRun(_)
            | DeliveryAdmission::SteerExisting(_) => AdmissionClass::LinePlacement,
            DeliveryAdmission::Requested(MessageDelivery::Auto | MessageDelivery::Steer)
                if matches!(snapshot.turn, SessionTurn::Running { .. }) =>
            {
                AdmissionClass::LinePlacement
            }
            DeliveryAdmission::Requested(MessageDelivery::Auto) if !snapshot.queue.is_empty() => {
                AdmissionClass::LinePlacement
            }
            _ => AdmissionClass::Independent,
        };
        let mode = AdmissionMode::deferred(class);
        let live = if can_queue {
            let Some(live) = self
                .reserve_queued_delivery(deadline, &operation, &target, &binding, mode)
                .await?
            else {
                return Ok(InputAcceptance::Unknown);
            };
            live
        } else {
            let PreparedOperation::Admitted { live, .. } = self
                .admit_operation(
                    deadline,
                    operation.clone(),
                    ProviderOperationKind::ConversationPrompt,
                    binding.clone(),
                    Some(&target),
                    mode,
                )
                .await?
            else {
                return Ok(InputAcceptance::Unknown);
            };
            live
        };
        let steering_supported = snapshot
            .projection
            .capabilities
            .as_ref()
            .is_some_and(|report| report.steer);
        let turn = match (admission, snapshot.turn) {
            (DeliveryAdmission::SteerExisting(turn), _) => turn,
            (
                DeliveryAdmission::Requested(MessageDelivery::Steer),
                SessionTurn::Running { turn },
            ) => turn,
            _ => TurnId(Uuid::now_v7()),
        };
        let input_id = request.input_id.clone();
        let input = OwnerInput {
            generation,
            operation: id,
            input: request.input_id,
            turn,
        };
        let command = match admission {
            DeliveryAdmission::Requested(MessageDelivery::Auto) => SessionOwnerCommand::Prompt {
                input: input.clone(),
                admission: PromptAdmission::DeliveryAuto { steering_supported },
            },
            DeliveryAdmission::Requested(MessageDelivery::Queue)
            | DeliveryAdmission::QueueOnRun(_) => SessionOwnerCommand::Prompt {
                input: input.clone(),
                admission: PromptAdmission::Queue,
            },
            DeliveryAdmission::Requested(MessageDelivery::Steer) => SessionOwnerCommand::Steer {
                input: input.clone(),
                supported: steering_supported,
            },
            DeliveryAdmission::SteerExisting(_) => SessionOwnerCommand::SteerExisting {
                input: input.clone(),
                supported: steering_supported,
            },
            #[cfg(test)]
            DeliveryAdmission::Direct => SessionOwnerCommand::Prompt {
                input: input.clone(),
                admission: PromptAdmission::Direct,
            },
            DeliveryAdmission::Warm(_) => SessionOwnerCommand::WarmPrompt(input.clone()),
            DeliveryAdmission::Notice(_) => SessionOwnerCommand::NoticePrompt(input.clone()),
        };
        let (writer, intake) = if let Some((sink, effect)) = &evidence {
            let (offer, intake) = tokio::sync::oneshot::channel();
            let writer = OwnerDeliveryEvidence::new(
                offer,
                sink.retain_queued_dispatch(),
                effect.clone(),
                runtime.owner_clock(),
            );
            (Some(writer), Some(intake))
        } else {
            (None, None)
        };
        let backend = self.clone();
        let (acknowledged, owner_acknowledgement) = tokio::sync::oneshot::channel();
        // The Host owns settlement before the caller can suspend on placement.
        // Only the borrowed evidence reply stays with the caller.
        let acceptance = self.retain_operation(operation.clone(), Some(target.clone()), async move {
            let receipt = if can_queue
                && owner
                    .defer_queued_admission(switchboard_session_store::ProviderOperationAdmission {
                        operation_id: operation.clone(),
                        operation_kind: ProviderOperationKind::ConversationPrompt,
                        binding:
                            collaboration_protocol::ConversationBindingIdentity::ExternalProvider {
                                binding: binding.clone(),
                            },
                        admitted_at_ms: backend.inner.now_ms(),
                    })
                    .is_err()
            {
                Err(crate::session_owner_door::OwnerDoorError::Unavailable)
            } else {
                if let Some(notice) = notice {
                    owner.remember_notice(input.input.clone(), notice);
                }
                owner.remember_approval(
                    input.input.clone(),
                    crate::ExternalProviderApprovalContext {
                        owner_scope: None,
                        requester: request.requested_by,
                        approver: request.approver,
                        target: target.clone(),
                        operation_id: operation.clone(),
                        binding_generation: binding.generation.clone(),
                        binding_retirement: runtime.retirement(),
                    },
                );
                owner
                    .start_delivery_with_evidence(input, content, command, writer, cold_restore)
                    .await
            };
            match receipt {
                Ok(receipt) => {
                    let completion_backend = backend.clone();
                    let (report_acceptance, received) = tokio::sync::oneshot::channel();
                    backend.spawn_operation(operation.clone(), live, async move {
                        let acceptance = receipt.accepted.await.unwrap_or(InputAcceptance::Unknown);
                        if acceptance == InputAcceptance::Queued {
                            completion_backend.queued_operation_registry().record_queued_with_preview(
                                operation.clone(),
                                target.clone(),
                                binding,
                                input_id,
                                preview,
                            );
                        }
                        let _caller_gone = report_acceptance.send(acceptance);
                        match receipt.terminal.await {
                            Ok(settled) => {
                                if let SessionOwnerReply::InputUnsent { reason, .. } = &settled.reply {
                                    let detail = match reason.queue_not_sent_reason() {
                                        Some(provider_session_machines::queued_entry_machine::NotSentReason::CancelledFromQueue) => "queueCancelled".into(),
                                        Some(reason) => reason.human_phrase().into(),
                                        None => reason.display_detail(),
                                    };
                                    completion_backend
                                        .queued_operation_registry()
                                        .mark_not_submitted(&operation, detail.as_ref());
                                } else {
                                    completion_backend.queued_operation_registry().clear(&operation);
                                }
                                retained_prompt(&runtime, operation, target, settled)
                            }
                            Err(_) => ProviderOperationCompletion::Failure(failure(
                                ConversationOperationFailureKind::OutcomeUnknown,
                                ConversationOperationFailureStage::Dispatch,
                                ProviderOperationEffect::Unknown,
                                "provider dispatch outcome is unknown",
                                operation,
                                Some(target),
                            )),
                        }
                    });
                    let _caller_gone = acknowledged.send(());
                    Ok(received.await.unwrap_or(InputAcceptance::Unknown))
                }
                Err(error) => {
                    if can_queue {
                        owner.release_queued_admission(&operation);
                    }
                    use provider_session_machines::session_owner::{
                        SessionOwnerRefusal, SessionOwnerRejection,
                    };
                    if matches!(admission, DeliveryAdmission::QueueOnRun(_))
                        && matches!(
                            error,
                            crate::session_owner_door::OwnerDoorError::Refused {
                                rejection: SessionOwnerRejection::Refused(
                                    SessionOwnerRefusal::GenerationChanged
                                ),
                                ..
                            }
                        )
                    {
                        backend.drop_reservation(&operation, &live, false);
                        return Ok(InputAcceptance::QueuePlanChanged);
                    }
                    let refusal = owner_failure(operation.clone(), target.clone(), error);
                    let refusal = if refusal.kind == ConversationOperationFailureKind::CannotSteer
                        && matches!(
                            admission,
                            DeliveryAdmission::SteerExisting(_)
                                | DeliveryAdmission::Requested(MessageDelivery::Steer)
                        ) {
                        explicit_steer_refusal(refusal, ConversationOperationFailureKind::CannotSteer)
                    } else {
                        refusal
                    };
                    if refusal.effect == ProviderOperationEffect::None {
                        backend.queued_operation_registry()
                            .mark_not_submitted(&operation, String::from(refusal.message.clone()));
                    }
                    let final_failure = refusal.clone();
                    backend.spawn_operation(operation, live, async move {
                        if can_queue {
                            ProviderOperationCompletion::Owned {
                                result: Err(final_failure),
                                target,
                                saved: true,
                            }
                        } else {
                            ProviderOperationCompletion::Failure(final_failure)
                        }
                    });
                    Err(refusal)
                }
            }
        });
        if let (Some((sink, effect)), Some(intake)) = (evidence, intake)
            && owner_acknowledgement.await.is_ok()
            && let Ok(offer) = intake.await
        {
            use provider_session_machines::session_owner::EffectOutcome;
            let mut recorded = offer.recorded;
            let outcome = tokio::select! {
                biased;
                () = recorded.closed() => EffectOutcome::Unknown,
                result = sink.record(placement_evidence(effect, offer.placement.kind)) => {
                    if result.is_ok() { EffectOutcome::Applied } else { EffectOutcome::Failed }
                }
            };
            let _owner_stopped = recorded.send(outcome);
        }
        acceptance.await
    }
}
