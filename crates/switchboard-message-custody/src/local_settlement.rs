//! Store effects owed for members whose lane evidence this worker still owns.

use central_link_protocol::ReplyPayload;
use router_clock::ClockInstant;
use std::collections::BTreeMap;
use switchboard_machine_core_store::{
    EndedWrite, OutboxCancellation, OutboxQueuedWrite, OutboxRequestKey, OutboxRequestStatus,
    OutboxStoreError, OutboxWriteOutcome, OutboxWriteTarget, StoredOutboxWrite,
    UnansweredOutboxClaim,
};
use workspace_ids::RequestId;
use workspace_model::{Caller, WorkspaceOperation};

use crate::{
    custody_submission::accepted_reply,
    flush_report::{BatchDispatchError, FlushReport, FlushedWriteAnswer},
    outbox_settlement_map::outcome_from_link_reply,
    outbox_work::OutboxWork,
};

/// The dispatcher retains either the checked reply or the actual departure evidence for one member.
enum MemberEvidence {
    Answered(Box<CheckedMemberAnswer>),
    NotTransmitted,
    Unanswered,
}

struct CheckedMemberAnswer {
    wire_reply: ReplyPayload,
    outcome: OutboxWriteOutcome,
    cutoff: AnswerAtCutoff,
}

/// Custody compares the driver's acceptance instant with the original write deadline.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum AnswerAtCutoff {
    BeforeCutoff,
    FinishCutoff,
    StatusOnly,
}

impl AnswerAtCutoff {
    pub(crate) fn new(accepted_at: ClockInstant, deadline: ClockInstant) -> Self {
        if accepted_at < deadline {
            Self::BeforeCutoff
        } else {
            Self::FinishCutoff
        }
    }
}

struct MemberSettlement {
    key: RequestId,
    caller: Caller,
    payload: OutboxQueuedWrite,
    evidence: MemberEvidence,
}

/// The store's batch or orphan projection supplies the original write and its owner for settlement.
pub(crate) struct SettlementWrite {
    request: RequestId,
    caller: Caller,
    payload: OutboxQueuedWrite,
}

impl From<StoredOutboxWrite> for SettlementWrite {
    fn from(write: StoredOutboxWrite) -> Self {
        Self {
            request: write.request,
            caller: write.caller,
            payload: write.payload,
        }
    }
}

impl From<UnansweredOutboxClaim> for SettlementWrite {
    fn from(claim: UnansweredOutboxClaim) -> Self {
        Self {
            request: claim.key.request_id,
            caller: claim.caller,
            payload: claim.payload,
        }
    }
}

/// Pending store transitions owned by the command task until commit or exact read-back.
#[derive(Default)]
pub(crate) struct LocalSettlements {
    pending: BTreeMap<RequestId, MemberSettlement>,
}

impl LocalSettlements {
    pub(crate) fn protected_ids(&self) -> impl Iterator<Item = &RequestId> {
        self.pending.keys()
    }

    pub(crate) async fn answer(
        &mut self,
        work: &mut OutboxWork,
        write: SettlementWrite,
        reply: ReplyPayload,
        cutoff: AnswerAtCutoff,
        report: &mut FlushReport,
    ) {
        let evidence = match outcome_from_link_reply(&write.payload, reply.clone()) {
            Ok(outcome) => MemberEvidence::Answered(Box::new(CheckedMemberAnswer {
                wire_reply: reply,
                outcome,
                cutoff,
            })),
            Err(_) => {
                report
                    .issues
                    .push(BatchDispatchError::UnsettledReply(write.request.clone()));
                if cutoff == AnswerAtCutoff::StatusOnly {
                    return;
                }
                MemberEvidence::Unanswered
            }
        };
        self.persist(
            work,
            MemberSettlement {
                key: write.request,
                caller: write.caller,
                payload: write.payload,
                evidence,
            },
            report,
        )
        .await;
    }

    pub(crate) async fn recover_cutoff(
        work: &mut OutboxWork,
        write: &StoredOutboxWrite,
    ) -> Result<Option<EndedWrite>, OutboxStoreError> {
        let key = OutboxRequestKey { request_id: write.request.clone(), scope: write.payload.request_scope() };
        match work.outbox_storage().scoped_request_status(&write.caller, &key).await? {
            OutboxRequestStatus::Ended { reason: OutboxCancellation::NotConfirmedWithin15Minutes } => {
                let target = queued_write_target(&write.payload).ok_or_else(|| OutboxStoreError::UnexpectedState {
                    request: write.request.clone(),
                    expected: "a cutoff with its original write target",
                })?;
                Ok(Some(EndedWrite {
                    caller: write.caller.clone(),
                    request_id: write.request.clone(),
                    target,
                    reason: OutboxCancellation::NotConfirmedWithin15Minutes,
                }))
            }
            OutboxRequestStatus::Answered(_) | OutboxRequestStatus::Refused(_) => Ok(None),
            _ => Err(OutboxStoreError::UnexpectedState {
                request: write.request.clone(),
                expected: "an ended or answered batch member",
            }),
        }
    }

    pub(crate) async fn return_member(
        &mut self,
        work: &mut OutboxWork,
        write: switchboard_machine_core_store::StoredOutboxWrite,
        departure: MemberReturn,
        report: &mut FlushReport,
    ) {
        self.persist(
            work,
            MemberSettlement {
                key: write.request,
                caller: write.caller,
                payload: write.payload,
                evidence: departure.into_evidence(),
            },
            report,
        )
        .await;
    }

    pub(crate) async fn retry(&mut self, work: &mut OutboxWork, report: &mut FlushReport) {
        for pending in std::mem::take(&mut self.pending).into_values() {
            self.persist(work, pending, report).await;
        }
    }

    pub(crate) async fn return_recovered_claim(
        &mut self,
        work: &mut OutboxWork,
        claim: UnansweredOutboxClaim,
        departure: MemberReturn,
        report: &mut FlushReport,
    ) {
        self.persist(
            work,
            MemberSettlement {
                key: claim.key.request_id,
                caller: claim.caller,
                payload: claim.payload,
                evidence: departure.into_evidence(),
            },
            report,
        )
        .await;
    }

    async fn persist(
        &mut self,
        work: &mut OutboxWork,
        mut settled: MemberSettlement,
        report: &mut FlushReport,
    ) {
        let result = match settled.apply(work, report).await {
            Ok(result) => Some(result),
            Err(error) => {
                let permanent = matches!(
                    error,
                    OutboxStoreError::ContradictoryFinalOutcome { .. }
                        | OutboxStoreError::LateOutcomeForNotSent { .. }
                        | OutboxStoreError::InvalidOutcome { .. }
                );
                work.report_point_read_error(&error);
                report.issues.push(BatchDispatchError::StoreMember {
                    request: settled.key.clone(),
                    error,
                });
                if permanent {
                    return;
                }
                settled.read_back(work).await
            }
        };
        match result {
            Some(ended) => {
                report.ended.extend(ended);
                if let MemberEvidence::Answered(answer) = settled.evidence
                    && answer.cutoff == AnswerAtCutoff::BeforeCutoff
                {
                    report.answered.push(FlushedWriteAnswer {
                        request: settled.key,
                        caller: settled.caller,
                        reply: answer.wire_reply,
                    });
                }
            }
            None => {
                self.pending.insert(settled.key.clone(), settled);
            }
        }
    }
}

/// The lane or dispatcher supplies which return is justified for this member.
pub(crate) enum MemberReturn {
    NotTransmitted,
    MayHaveTransmitted,
}

impl MemberReturn {
    fn into_evidence(self) -> MemberEvidence {
        match self {
            Self::NotTransmitted => MemberEvidence::NotTransmitted,
            Self::MayHaveTransmitted => MemberEvidence::Unanswered,
        }
    }
}

impl MemberSettlement {
    async fn apply(
        &mut self,
        work: &mut OutboxWork,
        report: &mut FlushReport,
    ) -> Result<Vec<EndedWrite>, OutboxStoreError> {
        let now_ms = work.local_now_ms();
        match &mut self.evidence {
            MemberEvidence::Answered(answer) => {
                if answer.cutoff == AnswerAtCutoff::BeforeCutoff {
                    let recorded = work
                        .outbox_storage()
                        .record_outcome(&self.key, &answer.outcome, now_ms)
                        .await;
                    match recorded {
                        Ok(()) => return Ok(Vec::new()),
                        Err(error @ OutboxStoreError::UnexpectedState { .. }) => {
                            let key = OutboxRequestKey {
                                request_id: self.key.clone(),
                                scope: self.payload.request_scope(),
                            };
                            let status = work
                                .outbox_storage()
                                .scoped_request_status(&self.caller, &key)
                                .await?;
                            if status
                                != (OutboxRequestStatus::Ended {
                                    reason: OutboxCancellation::NotConfirmedWithin15Minutes,
                                })
                            {
                                return Err(error);
                            }
                            answer.cutoff = AnswerAtCutoff::StatusOnly;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if answer.cutoff == AnswerAtCutoff::FinishCutoff {
                    report
                        .ended
                        .extend(work.end_selected_claim(&self.key).await?);
                    answer.cutoff = AnswerAtCutoff::StatusOnly;
                }
                work.outbox_storage()
                    .record_late_outcome(&self.key, &answer.outcome)
                    .await?;
            }
            MemberEvidence::NotTransmitted => {
                work.outbox_storage()
                    .return_not_transmitted(&self.key, now_ms)
                    .await?;
            }
            MemberEvidence::Unanswered => {
                return work
                    .return_unanswered(std::slice::from_ref(&self.key))
                    .await;
            }
        }
        Ok(Vec::new())
    }

    async fn read_back(&self, work: &mut OutboxWork) -> Option<Vec<EndedWrite>> {
        let key = OutboxRequestKey {
            request_id: self.key.clone(),
            scope: self.payload.request_scope(),
        };
        let status = match work
            .outbox_storage()
            .scoped_request_status(&self.caller, &key)
            .await
        {
            Ok(status) => status,
            Err(error) => {
                work.report_point_read_error(&error);
                return None;
            }
        };
        match (&self.evidence, status) {
            (MemberEvidence::Answered(answer), OutboxRequestStatus::Answered(accepted)) => {
                (outcome_from_link_reply(&self.payload, accepted_reply(accepted))
                    .ok()
                    .as_ref()
                    == Some(&answer.outcome))
                .then(Vec::new)
            }
            (MemberEvidence::Answered(answer), OutboxRequestStatus::Refused(reason)) => {
                (crate::outbox_settlement_map::outcome_from_stored_refusal(&self.payload, reason)
                    .ok()
                    .as_ref()
                    == Some(&answer.outcome))
                .then(Vec::new)
            }
            (
                MemberEvidence::NotTransmitted | MemberEvidence::Unanswered,
                OutboxRequestStatus::InOutbox { .. }
                | OutboxRequestStatus::Answered(_)
                | OutboxRequestStatus::Refused(_),
            ) => Some(Vec::new()),
            (MemberEvidence::Unanswered, OutboxRequestStatus::Ended { reason }) => {
                Some(vec![EndedWrite {
                    caller: self.caller.clone(),
                    request_id: self.key.clone(),
                    target: queued_write_target(&self.payload)?,
                    reason,
                }])
            }
            (MemberEvidence::NotTransmitted, OutboxRequestStatus::Ended { .. }) => Some(Vec::new()),
            _ => None,
        }
    }
}

fn queued_write_target(payload: &OutboxQueuedWrite) -> Option<OutboxWriteTarget> {
    match payload {
        OutboxQueuedWrite::Workspace(request) => {
            let WorkspaceOperation::ThreadPost { thread_id, .. } = &request.operation else {
                return None;
            };
            Some(OutboxWriteTarget::Thread {
                workspace_id: request.workspace_id.clone(),
                thread_id: thread_id.clone(),
            })
        }
        OutboxQueuedWrite::SendMessage { message, .. } => Some(OutboxWriteTarget::Session {
            to: message.to.clone(),
        }),
    }
}
