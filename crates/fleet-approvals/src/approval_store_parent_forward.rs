//! Advance only the current destination and retain its correlated pass-on result.
use approval_request_protocol::{
    ApprovalDestination, CentralTime, Ending, PassOnApprovalReply, PassOnApprovalRequest,
};
use fleet_actor_identity::AuthenticatedSession;
use fleet_request_log::{
    decode_organization_request, lookup_organization_request, request_log_digest, strict_cutoff,
};
use fleet_runtime_ports::{stored_values::json, workspace_decode_error::corrupt};
use serde::Serialize;
use workspace_ids::ApprovalRequestId;

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput,
    destination_commands::approval_session_address,
};
use crate::{
    ApprovalActor, ApprovalDestinations, ApprovalEvent, ApprovalRouteDestination, ApprovalSession,
    ApprovalSessionFacts, ApprovalState, PendingApprovalReply,
    approval_store_reads::{ApprovalReadRecord, read_parent_approval_request},
    approval_store_transaction::ApprovalStoreTransaction,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PassOnIntent<'request> {
    operation: &'static str,
    organization_id: &'request fleet_identity_protocol::OrganizationId,
    request_id: &'request ApprovalRequestId,
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn pass_on_as_session(
        &mut self,
        admitted: &AuthenticatedSession,
        request: PassOnApprovalRequest,
    ) -> Result<PendingApprovalReply<PassOnApprovalReply>, ApprovalApplyFailure> {
        let (organization, session, address) = approval_session_address(admitted);
        let record = read_parent_approval_request(
            self.transaction,
            organization,
            &request.request_id,
            &session,
        )
        .await?;
        let Some(record) = record else {
            return Ok(self.stage_reply(
                address,
                ApprovalTransitionOutput::with_reply(PassOnApprovalReply::NotVisible {}),
            ));
        };
        let actor = ApprovalActor::Session(session.clone());
        let caller = self
            .transaction
            .approval_decision_caller(organization, &actor)
            .await?;
        let at = self.sample_applying_time()?;
        if crate::approval_retention::hidden_at(&record.state, at)? {
            return Ok(self.stage_reply(
                address,
                ApprovalTransitionOutput::with_reply(PassOnApprovalReply::NotVisible {}),
            ));
        }
        let cutoff = strict_cutoff(at).to_rfc3339();
        let digest = request_log_digest(&PassOnIntent {
            operation: "passOnApproval",
            organization_id: organization,
            request_id: &request.request_id,
        })?;
        let stored = self
            .transaction
            .read_approval_statement(lookup_organization_request(&request.operation_id, &cutoff))
            .await?;
        if let Some(stored) = decode_organization_request(&stored)? {
            let answer = if stored.caller != caller || stored.content_digest != digest {
                PassOnApprovalReply::OperationConflict {}
            } else {
                let retained = json(&stored.answer, "runtime_request_log.pass_on_answer")?;
                if !retained_pass_on_matches(&retained, &record.state, &session) {
                    return Err(corrupt("runtime_request_log.pass_on_answer").into());
                }
                retained
            };
            let effects = self.expire_if_waiting(&record, at).await?;
            return Ok(self.stage_reply(
                address,
                ApprovalTransitionOutput {
                    reply: answer,
                    ending_offers: effects,
                },
            ));
        }
        let Some(saved) = record.state.retained() else {
            return Err(corrupt("approval_requests.pass_on_request").into());
        };
        let applied = match &record.state {
            ApprovalState::Waiting { .. } if at >= saved.window.deadline() => {
                ApprovalTransitionOutput {
                    reply: PassOnApprovalReply::TooLate {},
                    ending_offers: self.expire_if_waiting(&record, at).await?,
                }
            }
            ApprovalState::Waiting { .. }
                if saved.destinations.current_session() == Some(&session) =>
            {
                let next = self
                    .sessions
                    .next_approval_parent(self.transaction, &session)
                    .await?;
                let route = next.map_or(
                    ApprovalRouteDestination::People,
                    ApprovalRouteDestination::Session,
                );
                let reply = PassOnApprovalReply::PassedOn {
                    to: route_address(&route),
                };
                let result = self
                    .apply_request_event(
                        Some(&record.row_id),
                        &record.scope,
                        &record.state,
                        ApprovalEvent::PassOn {
                            key: saved.key.clone(),
                            live_parent: session,
                            to: route,
                        },
                        at,
                    )
                    .await?;
                result.replace_reply(reply)
            }
            ApprovalState::Waiting { .. } => {
                return Ok(self.stage_reply(
                    address,
                    ApprovalTransitionOutput::with_reply(PassOnApprovalReply::NotVisible {}),
                ));
            }
            ApprovalState::Ended { closed, .. } => {
                ApprovalTransitionOutput::with_reply(match closed.ending.clone().into_ending() {
                    Ending::Decided { by, .. } => PassOnApprovalReply::AlreadyDecided {
                        by,
                        at: closed.ended_at,
                    },
                    Ending::Cancelled { .. } | Ending::TimedOut {} => {
                        PassOnApprovalReply::TooLate {}
                    }
                    Ending::UnprovenCallDecided { .. } => {
                        return Err(corrupt("approval_requests.pass_on_ending").into());
                    }
                })
            }
            ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => {
                return Err(corrupt("approval_requests.pass_on_request").into());
            }
        };
        for write in crate::approval_decision_log::retain_approval_operation(
            &record.row_id,
            &request.operation_id,
            &caller,
            &digest,
            &applied.reply,
            &at.to_rfc3339(),
            &cutoff,
        )? {
            self.execute_guarded_write(write).await?;
        }
        Ok(self.stage_reply(address, applied))
    }

    async fn expire_if_waiting(
        &mut self,
        record: &ApprovalReadRecord,
        at: CentralTime,
    ) -> Result<Vec<crate::CommittedApprovalOffer>, ApprovalApplyFailure> {
        if let ApprovalState::Waiting { saved } = &record.state
            && at >= saved.window.deadline()
        {
            let applied = self
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
            return Ok(applied.ending_offers);
        }
        Ok(Vec::new())
    }
}

fn route_address(route: &ApprovalRouteDestination) -> ApprovalDestination {
    match route {
        ApprovalRouteDestination::Session(session) => ApprovalDestination::Session {
            session_id: session.session_id.clone(),
        },
        ApprovalRouteDestination::People => ApprovalDestination::People {},
    }
}

fn retained_pass_on_matches(
    reply: &PassOnApprovalReply,
    state: &ApprovalState,
    caller: &ApprovalSession,
) -> bool {
    match reply {
        PassOnApprovalReply::PassedOn { to } => {
            let Some(saved) = state.retained() else {
                return false;
            };
            let Some(index) = saved
                .destinations
                .previous()
                .iter()
                .position(|parent| parent == caller)
            else {
                return false;
            };
            match (to, saved.destinations.sessions().nth(index + 1)) {
                (ApprovalDestination::Session { session_id }, Some(next)) => {
                    &next.session_id == session_id
                }
                (ApprovalDestination::People {}, None) => {
                    matches!(saved.destinations, ApprovalDestinations::People { .. })
                }
                (ApprovalDestination::Session { .. }, None)
                | (ApprovalDestination::People {}, Some(_)) => false,
            }
        }
        PassOnApprovalReply::AlreadyDecided { by, at } => {
            matches!(state, ApprovalState::Ended { closed, .. } if &closed.ended_at == at
                && matches!(closed.ending.clone().into_ending(), Ending::Decided { by: observed, .. } if &observed == by))
        }
        PassOnApprovalReply::TooLate {} => matches!(state, ApprovalState::Ended { closed, .. }
                if matches!(closed.ending.clone().into_ending(), Ending::Cancelled { .. } | Ending::TimedOut {})),
        PassOnApprovalReply::NotVisible {} | PassOnApprovalReply::OperationConflict {} => false,
    }
}
