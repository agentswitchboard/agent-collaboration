//! Reconcile durable claims that survived their worker, keeping their original cutoff.

use central_link_client::{
    AdmittedConnection, CorrelatedStatusReply, LINK_LANE_SETTLEMENT_LIMIT, LinkLaneDisposition,
    RequestStatusCommand,
};
use central_link_protocol::{
    FreshCorrelationId, ReplyPayload, RequestRefusal, RequestScope, RequestStatusAnswer,
    RequestStatusQuery,
};
use router_clock::ClockInstant;
use std::collections::BTreeSet;
use switchboard_machine_core_store::{OutboxScope, UnansweredOutboxClaim};
use workspace_ids::RequestId;
use workspace_model::{WorkspaceReply, WorkspaceResult, central_mail::CentralMailResult};

use crate::{
    flush_report::{BatchDispatchError, FlushReport},
    local_settlement::{AnswerAtCutoff, MemberReturn},
    outbox_dispatch::OutboxDispatcher,
    outbox_settlement_map::outcome_from_link_reply,
    outbox_work::{OutboxWork, OutboxWorkError},
};

/// The store supplies a genuine orphan and custody prepares the exact owner's request-status read.
pub(crate) struct OrphanProbe {
    pub(crate) claim: UnansweredOutboxClaim,
    pub(crate) command: RequestStatusCommand,
}

/// Custody scans the store for locally due endings and one request that can be checked on the lane.
pub(crate) struct OrphanTurn {
    pub(crate) report: FlushReport,
    pub(crate) next_deadline: Option<ClockInstant>,
    pub(crate) probe: Option<OrphanProbe>,
}

pub(crate) async fn prepare_orphan_turn(
    work: &mut OutboxWork,
    dispatch: &mut OutboxDispatcher,
    startup_claims: &mut Option<BTreeSet<RequestId>>,
    ready: Option<AdmittedConnection>,
    observed_at: ClockInstant,
    deferred: &BTreeSet<RequestId>,
) -> Result<OrphanTurn, OutboxWorkError> {
    let claims = work.orphan_claims().await?;
    let scanned = claims
        .iter()
        .map(|claim| claim.key.request_id.clone())
        .collect::<BTreeSet<_>>();
    // Before dispatching any write, take the durable inventory that pre-dates
    // this worker. A later unowned claim can only be a local claim whose
    // commit acknowledgment failed before the worker received any batch.
    let startup = startup_claims.get_or_insert_with(|| scanned.clone());
    startup.retain(|request| scanned.contains(request));
    let protected = dispatch.protected_request_ids();
    let mut report = FlushReport::default();
    let mut genuine_claims = Vec::new();
    for claim in claims {
        if protected.contains(&claim.key.request_id) {
            continue;
        }
        if startup.contains(&claim.key.request_id) {
            genuine_claims.push(claim);
        } else {
            dispatch
                .return_recovered_claim(work, claim, MemberReturn::NotTransmitted, &mut report)
                .await;
        }
    }
    let now_ms = work.local_now_ms();
    let (overdue, live): (Vec<_>, Vec<_>) = genuine_claims
        .into_iter()
        .partition(|claim| claim.deadline_ms <= now_ms);
    if !overdue.is_empty() {
        let protected = dispatch.protected_request_ids();
        match work.end_due_orphans(&protected).await {
            Ok(ended) => report.ended.extend(ended),
            Err(error) if report.issues.is_empty() => return Err(error),
            Err(error) => {
                report.issues.push(BatchDispatchError::Work(error));
                let next_deadline = Some(work.local_wake_at(now_ms.saturating_add(1_000)));
                return Ok(OrphanTurn {
                    report,
                    next_deadline,
                    probe: None,
                });
            }
        }
    }
    let next_deadline = live
        .iter()
        .map(|claim| work.local_wake_at(claim.deadline_ms))
        .min();
    let probe = ready.and_then(|admitted| {
        live.into_iter()
            .find(|claim| !deferred.contains(&claim.key.request_id))
            .map(|claim| {
                let query = RequestStatusQuery {
                    request_id: claim.key.request_id.clone(),
                    scope: match &claim.key.scope {
                        OutboxScope::Organization => RequestScope::Organization,
                        OutboxScope::Workspace(workspace) => {
                            RequestScope::Workspace(workspace.clone())
                        }
                    },
                };
                let command = RequestStatusCommand {
                    connection: admitted.connection,
                    send_before: observed_at
                        .after(LINK_LANE_SETTLEMENT_LIMIT)
                        .min(work.local_wake_at(claim.deadline_ms)),
                    correlation_id: FreshCorrelationId::mint_at(now_ms).wire_id().clone(),
                    caller: claim.caller.clone(),
                    query,
                };
                OrphanProbe { claim, command }
            })
    });
    Ok(OrphanTurn {
        report,
        next_deadline,
        probe,
    })
}

pub(crate) async fn settle_orphan_probe(
    work: &mut OutboxWork,
    dispatch: &mut OutboxDispatcher,
    probe: OrphanProbe,
    disposition: LinkLaneDisposition<CorrelatedStatusReply>,
    cutoff_reported: bool,
) -> FlushReport {
    let mut report = FlushReport::default();
    let LinkLaneDisposition::Answered { reply, accepted_at } = disposition else {
        return report;
    };
    if reply.correlation_id != probe.command.correlation_id {
        return report;
    }
    let deadline = work.local_wake_at(probe.claim.deadline_ms);
    let reply = match *reply.result {
        RequestStatusAnswer::NotFound => {
            if !cutoff_reported
                && accepted_at < deadline
                && work.has_settlement_time(probe.claim.deadline_ms)
            {
                dispatch
                    .return_recovered_claim(
                        work,
                        probe.claim,
                        MemberReturn::MayHaveTransmitted,
                        &mut report,
                    )
                    .await;
            }
            return report;
        }
        RequestStatusAnswer::Answered(reply) => reply,
        RequestStatusAnswer::Refused(RequestRefusal::Workspace(reason)) => {
            ReplyPayload::Workspace(WorkspaceReply {
                result: WorkspaceResult::Refused(reason),
                actors: Vec::new(),
            })
        }
        RequestStatusAnswer::Refused(RequestRefusal::Mail(reason)) => {
            ReplyPayload::Mail(CentralMailResult::Refused(reason))
        }
    };
    if outcome_from_link_reply(&probe.claim.payload, reply.clone()).is_ok() {
        let timing = if cutoff_reported {
            AnswerAtCutoff::StatusOnly
        } else {
            AnswerAtCutoff::new(accepted_at, deadline)
        };
        dispatch
            .settle_orphan_answer(work, probe.claim, reply, timing, &mut report)
            .await;
    } else {
        report.issues.push(BatchDispatchError::UnsettledReply(
            probe.claim.key.request_id,
        ));
    }
    report
}
