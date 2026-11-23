//! Applies or replays a signed-in person's decision on the organization transaction.

use approval_request_protocol::{
    AdmittedDestination, CentralTime, DecidedBy, Ending, GiverDecision, InvalidApprovalEnding,
};
use central_engine_model::supported_time;
use central_storage::StoreFailure;
use fleet_request_log::{
    RetainedRequest, decode_organization_request, lookup_organization_request,
    retain_organization_request, strict_cutoff,
};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use fleet_signin::{
    ApprovalPersonAdmission, VerifiedApprovalDecideRetry,
    approval_person_wire::{
        ApprovalDecideReply, ApprovalDecideRequest, ApprovalInvalidReason, ApprovalPersonOperation,
    },
};
use workspace_ids::RequestId;

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput,
    person_commands::reply_address,
};
use crate::{
    AdmittedDecider, ApprovalDecideInput, ApprovalDecisionReply, ApprovalEvent,
    ApprovalPersonStoreTransaction, ApprovalReceipt, ApprovalReplyAddress, ApprovalState,
    EndedApproval, PendingApprovalReply,
    approval_decision_log::retained_answer_matches,
    approval_store_reads::{ApprovalReadRecord, read_people_approval_request},
    approval_store_transaction::ApprovalStoreTransaction,
};

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalPersonStoreTransaction,
{
    pub(super) async fn replay_person_decision(
        &mut self,
        retry: &VerifiedApprovalDecideRetry<'_>,
        retained_at: CentralTime,
    ) -> Result<Option<PendingApprovalReply<ApprovalDecideReply>>, ApprovalApplyFailure> {
        let request = retry.request();
        if self.transaction.organization_id() != &request.organization {
            return Ok(None);
        }
        let Some(stored) = self
            .read_person_decision(&request.operation_id, retained_at)
            .await?
        else {
            return Ok(None);
        };
        let actor = self.transaction.find_person_actor(retry.person()).await?;
        if actor.as_ref() != Some(&stored.caller) || retry.intent_digest() != stored.content_digest
        {
            return Ok(None);
        }
        let reply =
            decode_retained_decision(self.transaction, &stored, request, retry.person()).await?;
        let address = ApprovalReplyAddress::Person {
            organization: request.organization.clone(),
            person: retry.person().clone(),
        };
        Ok(Some(self.stage_reply(
            address,
            ApprovalTransitionOutput::with_reply(reply),
        )))
    }

    pub(super) async fn decide_person_request(
        &mut self,
        admission: &ApprovalPersonAdmission,
        request: ApprovalDecideRequest,
        retained_at: CentralTime,
    ) -> Result<PendingApprovalReply<ApprovalDecideReply>, ApprovalApplyFailure> {
        if admission.organization() != self.transaction.organization_id()
            || admission.organization() != &request.organization
        {
            return Err(corrupt("approval_store.person_organization").into());
        }
        let digest = ApprovalPersonOperation::Decide(request.clone())
            .intent()
            .digest()
            .map_err(|_| corrupt("runtime_request_log.approval_intent"))?;
        let address = reply_address(admission);
        if let Some(stored) = self
            .read_person_decision(&request.operation_id, retained_at)
            .await?
        {
            let actor = self
                .transaction
                .find_person_actor(admission.person())
                .await?;
            let reply = if actor.as_ref() == Some(&stored.caller) && digest == stored.content_digest
            {
                decode_retained_decision(self.transaction, &stored, &request, admission.person())
                    .await?
            } else {
                ApprovalDecideReply::OperationConflict {}
            };
            return Ok(self.stage_reply(address, ApprovalTransitionOutput::with_reply(reply)));
        }

        let at = self.sample_applying_time()?;
        let applied = self.apply_person_decision(admission, &request, at).await?;
        match &applied.reply {
            ApprovalDecideReply::SavedDecision { .. }
            | ApprovalDecideReply::AlreadyDecided { .. }
            | ApprovalDecideReply::TooLate {}
            | ApprovalDecideReply::Invalid { .. }
            | ApprovalDecideReply::NotVisible {} => {
                let caller = self
                    .transaction
                    .resolve_person_actor(admission, at, self.row_ids)
                    .await?;
                for write in retain_organization_request(
                    &request.operation_id,
                    &caller,
                    &digest,
                    &applied.reply,
                    &retained_at.to_rfc3339(),
                    &strict_cutoff(retained_at).to_rfc3339(),
                )? {
                    self.execute_guarded_write(write).await?;
                }
            }
            ApprovalDecideReply::OperationConflict {} => {
                return Err(corrupt("approval_store.fresh_decide_conflict").into());
            }
        }
        Ok(self.stage_reply(address, applied))
    }

    async fn read_person_decision(
        &mut self,
        operation: &RequestId,
        retained_at: CentralTime,
    ) -> Result<Option<RetainedRequest>, ApprovalApplyFailure> {
        if !supported_time(retained_at) {
            return Err(corrupt("runtime_request_log.applying_time").into());
        }
        let stored = self
            .transaction
            .read_approval_statement(lookup_organization_request(
                operation,
                &strict_cutoff(retained_at).to_rfc3339(),
            ))
            .await?;
        Ok(decode_organization_request(&stored)?)
    }

    async fn apply_person_decision(
        &mut self,
        admission: &ApprovalPersonAdmission,
        request: &ApprovalDecideRequest,
        at: CentralTime,
    ) -> Result<ApprovalTransitionOutput<ApprovalDecideReply>, ApprovalApplyFailure> {
        let record = read_people_approval_request(
            self.transaction,
            admission.organization(),
            &request.request_id,
        )
        .await?;
        let Some(record) = record else {
            return Ok(ApprovalTransitionOutput::with_reply(
                ApprovalDecideReply::NotVisible {},
            ));
        };
        match &record.state {
            ApprovalState::Ended { closed, .. } => {
                Ok(ApprovalTransitionOutput::with_reply(ended_reply(closed)))
            }
            ApprovalState::Waiting { saved } if at >= saved.window.deadline() => {
                let expired = self
                    .apply_request_event(
                        Some(&record.row_id),
                        &record.scope,
                        &record.state,
                        ApprovalEvent::Alarm {
                            deadline: saved.window.deadline(),
                        },
                        at,
                    )
                    .await?;
                Ok(expired.replace_reply(ApprovalDecideReply::TooLate {}))
            }
            ApprovalState::Waiting { .. } => {
                self.apply_waiting_person_decision(admission, request, &record, at)
                    .await
            }
            ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => Ok(
                ApprovalTransitionOutput::with_reply(ApprovalDecideReply::NotVisible {}),
            ),
        }
    }

    async fn apply_waiting_person_decision(
        &mut self,
        admission: &ApprovalPersonAdmission,
        request: &ApprovalDecideRequest,
        record: &ApprovalReadRecord,
        at: CentralTime,
    ) -> Result<ApprovalTransitionOutput<ApprovalDecideReply>, ApprovalApplyFailure> {
        let saved = record
            .state
            .retained()
            .ok_or_else(|| corrupt("approval_store.waiting"))?;
        let decider = AdmittedDecider::Person {
            person_id: admission.person().clone(),
        };
        let recipient = decider.actor().identity();
        let destination = AdmittedDestination::for_request(
            &saved.raise,
            &recipient,
            std::slice::from_ref(&recipient),
        )
        .map_err(|_| corrupt("approval_store.person_destination"))?;
        if let Err(reason) = destination.decide(request.answer.clone()) {
            return Ok(ApprovalTransitionOutput::with_reply(invalid_reply(reason)));
        }

        let applied = self
            .apply_request_event(
                Some(&record.row_id),
                &record.scope,
                &record.state,
                ApprovalEvent::Decide {
                    key: saved.key.clone(),
                    actor: decider,
                    request: ApprovalDecideInput {
                        request_id: request.request_id.clone(),
                        operation_id: request.operation_id.clone(),
                        value: request.answer.clone(),
                    },
                },
                at,
            )
            .await?;
        let reply = match &applied.reply {
            Some(ApprovalReceipt::Decide(reply)) => reply,
            None
            | Some(
                ApprovalReceipt::Raise(_)
                | ApprovalReceipt::Read(_)
                | ApprovalReceipt::PassOn(_)
                | ApprovalReceipt::Stop(_),
            ) => return Err(corrupt("approval_store.person_decide_receipt").into()),
        };
        let reply = reply
            .for_recipient(&saved.key, &recipient)
            .map_err(|_| corrupt("approval_store.person_decide_recipient"))?;
        // The request is visible and waiting, its deadline has not passed,
        // and the answer has passed validation.
        let decision = match reply {
            ApprovalDecisionReply::SavedDecision { answer, by } => GiverDecision {
                answer: answer.clone(),
                by: by.clone(),
            },
            ApprovalDecisionReply::Standing { .. } => {
                return Err(corrupt("approval_store.person_decide_standing").into());
            }
            ApprovalDecisionReply::NotVisible {} => {
                return Err(corrupt("approval_store.person_decide_visibility").into());
            }
            ApprovalDecisionReply::Invalid {} => {
                return Err(corrupt("approval_store.person_decide_admitted_answer").into());
            }
            ApprovalDecisionReply::TooLate {} => {
                return Err(corrupt("approval_store.person_decide_deadline").into());
            }
            ApprovalDecisionReply::OperationConflict {} => {
                return Err(corrupt("approval_store.person_decide_reducer_conflict").into());
            }
        };
        Ok(applied.replace_reply(ApprovalDecideReply::SavedDecision { decision }))
    }
}

fn invalid_reply(reason: InvalidApprovalEnding) -> ApprovalDecideReply {
    match reason {
        InvalidApprovalEnding::Answer(reason) => ApprovalDecideReply::Invalid {
            reason: ApprovalInvalidReason::InvalidAnswer { reason },
        },
        InvalidApprovalEnding::Digest(_)
        | InvalidApprovalEnding::RequesterIsReader
        | InvalidApprovalEnding::BindingMismatch
        | InvalidApprovalEnding::IneligibleDecider
        | InvalidApprovalEnding::DestinationNotAdmitted
        | InvalidApprovalEnding::RetainedDecisionMismatch
        | InvalidApprovalEnding::Chronology => ApprovalDecideReply::Invalid {
            reason: ApprovalInvalidReason::InvalidApprovalEnding { reason },
        },
    }
}

fn ended_reply(closed: &EndedApproval) -> ApprovalDecideReply {
    let by = match closed.ending.clone().into_ending() {
        Ending::Decided { by, .. } => by,
        Ending::UnprovenCallDecided { person_id, .. } => DecidedBy::Person { person_id },
        Ending::Cancelled { .. } | Ending::TimedOut {} => return ApprovalDecideReply::TooLate {},
    };
    ApprovalDecideReply::AlreadyDecided {
        by,
        at: closed.ended_at,
    }
}

async fn decode_retained_decision(
    transaction: &mut impl ApprovalStoreTransaction,
    stored: &RetainedRequest,
    request: &ApprovalDecideRequest,
    person: &workspace_ids::PersonId,
) -> Result<ApprovalDecideReply, StoreFailure> {
    let reply = json(&stored.answer, "runtime_request_log.answer")?;
    match &reply {
        ApprovalDecideReply::SavedDecision { decision } => {
            if decision.answer != request.answer
                || !matches!(&decision.by, DecidedBy::Person { person_id } if person_id == person)
            {
                return Err(invalid_retained_decision());
            }
            let record = retained_person_approval(transaction, request).await?;
            let ApprovalState::Ended { closed, .. } = &record.state else {
                return Err(invalid_retained_decision());
            };
            if closed.decided_operation_id.as_ref() != Some(&request.operation_id)
                || !retained_answer_matches(&record.state, &decision.answer, &decision.by)
            {
                return Err(invalid_retained_decision());
            }
        }
        ApprovalDecideReply::AlreadyDecided { .. } | ApprovalDecideReply::TooLate {} => {
            let record = retained_person_approval(transaction, request).await?;
            let ApprovalState::Ended { closed, .. } = &record.state else {
                return Err(invalid_retained_decision());
            };
            if ended_reply(closed) != reply {
                return Err(invalid_retained_decision());
            }
        }
        // Historical refusals are replayed even if later operations change the request.
        ApprovalDecideReply::Invalid { .. } | ApprovalDecideReply::NotVisible {} => {}
        ApprovalDecideReply::OperationConflict {} => {
            return Err(invalid_retained_decision());
        }
    }
    Ok(reply)
}

async fn retained_person_approval(
    transaction: &mut impl ApprovalStoreTransaction,
    request: &ApprovalDecideRequest,
) -> Result<ApprovalReadRecord, StoreFailure> {
    read_people_approval_request(transaction, &request.organization, &request.request_id)
        .await?
        .ok_or_else(invalid_retained_decision)
}

fn invalid_retained_decision() -> StoreFailure {
    corrupt("runtime_request_log.answer")
}
