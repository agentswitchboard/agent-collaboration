//! Decide's twenty-four-hour answer uses the organization request log.
use approval_request_protocol::{
    AdmittedDestination, Answer, CentralTime, DecidedBy, Ending, EndingView,
};
use central_engine_model::supported_time;
use central_storage::{GuardedStatement, ReadOutcome, RowExpectation, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_request_log::{
    decode_organization_request, lookup_organization_request, request_log_digest,
    retain_organization_request, strict_cutoff,
};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use serde::{Deserialize, Serialize};
use workspace_ids::{ActorIdentityId, ApprovalRequestId, RequestId};

use crate::{ApprovalDecideInput, ApprovalDecisionReply, ApprovalRequestRowId, ApprovalState};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DecideApprovalIntent<'request> {
    operation: &'static str,
    organization_id: &'request OrganizationId,
    request_id: &'request ApprovalRequestId,
    value: &'request Answer,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum RetainedDecisionAnswer {
    SavedDecision { answer: Answer, by: DecidedBy },
    Standing { ending: EndingView },
    Invalid {},
    TooLate {},
}

pub(crate) struct ApprovalDecisionLogKey {
    operation_id: RequestId,
    caller: ActorIdentityId,
    digest: String,
    sampled_at: String,
    cutoff: String,
}

impl ApprovalDecisionLogKey {
    pub(crate) fn new(
        organization: &OrganizationId,
        caller: ActorIdentityId,
        request: &ApprovalDecideInput,
        sampled_at: CentralTime,
    ) -> Result<Self, StoreFailure> {
        if !supported_time(sampled_at) {
            return Err(corrupt("runtime_request_log.applying_time"));
        }
        let digest = request_log_digest(&DecideApprovalIntent {
            operation: "decideApproval",
            organization_id: organization,
            request_id: &request.request_id,
            value: &request.value,
        })?;
        Ok(Self {
            operation_id: request.operation_id.clone(),
            caller,
            digest,
            sampled_at: sampled_at.to_rfc3339(),
            cutoff: strict_cutoff(sampled_at).to_rfc3339(),
        })
    }

    pub(crate) fn lookup_statement(&self) -> Statement {
        lookup_organization_request(&self.operation_id, &self.cutoff)
    }

    pub(crate) fn replay(
        &self,
        rows: &ReadOutcome,
        request: &ApprovalDecideInput,
        recipient: &DecidedBy,
        state: &ApprovalState,
    ) -> Result<Option<ApprovalDecisionReply>, StoreFailure> {
        let Some(stored) = decode_organization_request(rows)? else {
            return Ok(None);
        };
        if stored.caller != self.caller || stored.content_digest != self.digest {
            return Ok(Some(ApprovalDecisionReply::OperationConflict {}));
        }
        let retained: RetainedDecisionAnswer = json(&stored.answer, "runtime_request_log.answer")?;
        let result = match retained {
            RetainedDecisionAnswer::SavedDecision { answer, by } => {
                if &by != recipient
                    || answer != request.value
                    || !retained_answer_matches(state, &answer, &by)
                {
                    return Err(invalid_retained_answer());
                }
                ApprovalDecisionReply::SavedDecision { answer, by }
            }
            RetainedDecisionAnswer::Standing { ending } => {
                let ApprovalState::Ended { saved, closed } = state else {
                    return Err(invalid_retained_answer());
                };
                let admitted = AdmittedDestination::for_request(
                    &saved.raise,
                    recipient,
                    std::slice::from_ref(recipient),
                )
                .map_err(|_| invalid_retained_answer())?;
                let observed = EndingView::for_destination(&admitted, &closed.ending)
                    .and_then(|view| view.for_recipient(recipient))
                    .map_err(|_| invalid_retained_answer())?;
                if observed != ending
                    || matches!(closed.ending.clone().into_ending(), Ending::TimedOut {})
                {
                    return Err(invalid_retained_answer());
                }
                ApprovalDecisionReply::Standing { ending }
            }
            RetainedDecisionAnswer::Invalid {} => ApprovalDecisionReply::Invalid {},
            RetainedDecisionAnswer::TooLate {} => {
                if !matches!(state, ApprovalState::Ended { closed, .. }
                        if matches!(closed.ending.clone().into_ending(), Ending::TimedOut {}))
                {
                    return Err(invalid_retained_answer());
                }
                ApprovalDecisionReply::TooLate {}
            }
        };
        Ok(Some(result))
    }

    pub(crate) fn retain(
        &self,
        row_id: &ApprovalRequestRowId,
        reply: &ApprovalDecisionReply,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        let answer = match reply {
            ApprovalDecisionReply::SavedDecision { answer, by } => {
                RetainedDecisionAnswer::SavedDecision {
                    answer: answer.clone(),
                    by: by.clone(),
                }
            }
            ApprovalDecisionReply::Standing { ending } => RetainedDecisionAnswer::Standing {
                ending: ending.clone(),
            },
            ApprovalDecisionReply::Invalid {} => RetainedDecisionAnswer::Invalid {},
            ApprovalDecisionReply::TooLate {} => RetainedDecisionAnswer::TooLate {},
            ApprovalDecisionReply::NotVisible {} | ApprovalDecisionReply::OperationConflict {} => {
                return Err(invalid_retained_answer());
            }
        };
        retain_approval_operation(
            row_id,
            &self.operation_id,
            &self.caller,
            &self.digest,
            &answer,
            &self.sampled_at,
            &self.cutoff,
        )
    }
}

pub(crate) fn retain_approval_operation(
    row_id: &ApprovalRequestRowId,
    operation: &RequestId,
    caller: &ActorIdentityId,
    digest: &str,
    answer: &impl Serialize,
    at: &str,
    cutoff: &str,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let mut writes = retain_organization_request(operation, caller, digest, answer, at, cutoff)?;
    writes.push(GuardedStatement {
        statement: fleet_engine::checked_engine_statement!(
            query_file,
            [],
            "src/queries/tag_approval_operation_result.sql",
            operation = operation.as_str().to_owned(),
            row_id = row_id.as_str().to_owned(),
        ),
        expect: RowExpectation::Exactly(1),
    });
    Ok(writes)
}

pub(crate) fn retained_answer_matches(
    state: &ApprovalState,
    expected_answer: &Answer,
    recipient: &DecidedBy,
) -> bool {
    let ApprovalState::Ended { closed, .. } = state else {
        return false;
    };
    match closed.ending.clone().into_ending() {
        Ending::Decided { answer, by } => {
            &by == recipient && &Answer::from(answer) == expected_answer
        }
        Ending::UnprovenCallDecided {
            decision,
            person_id,
        } => {
            recipient == &(DecidedBy::Person { person_id })
                && expected_answer == &(Answer::UnprovenCall { decision })
        }
        Ending::Cancelled { .. } | Ending::TimedOut {} => false,
    }
}

fn invalid_retained_answer() -> StoreFailure {
    corrupt("runtime_request_log.answer")
}
