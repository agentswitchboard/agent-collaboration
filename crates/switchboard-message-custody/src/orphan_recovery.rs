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
    dispatch: &OutboxDispatcher,
    ready: Option<AdmittedConnection>,
    observed_at: ClockInstant,
    deferred: &BTreeSet<RequestId>,
) -> Result<OrphanTurn, OutboxWorkError> {
    let protected = dispatch.protected_request_ids();
    let now_ms = work.local_now_ms();
    let (overdue, live): (Vec<_>, Vec<_>) = work
        .orphan_claims()
        .await?
        .into_iter()
        .filter(|claim| !protected.contains(&claim.key.request_id))
        .partition(|claim| claim.deadline_ms <= now_ms);
    let mut report = FlushReport::default();
    if !overdue.is_empty() {
        report.ended = work.end_due_orphans(&protected).await?;
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
) -> FlushReport {
    let mut report = FlushReport::default();
    if work.local_now_ms() >= probe.claim.deadline_ms {
        return report;
    }
    let LinkLaneDisposition::Answered { reply } = disposition else {
        return report;
    };
    if reply.correlation_id != probe.command.correlation_id {
        return report;
    }
    let reply = match *reply.result {
        RequestStatusAnswer::NotFound => {
            if work.has_settlement_time(probe.claim.deadline_ms) {
                dispatch.return_orphan(work, probe.claim, &mut report).await;
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
        dispatch
            .settle_orphan_answer(work, probe.claim, reply, &mut report)
            .await;
    } else {
        report.issues.push(BatchDispatchError::UnsettledReply(
            probe.claim.key.request_id,
        ));
    }
    report
}
