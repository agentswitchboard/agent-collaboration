use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Raise, RaiseReply, RunRefusal, StopReply, StopRequest,
};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::ApprovalRequestId;

use super::{ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput};
use crate::{
    ApprovalEvent, ApprovalReceipt, ApprovalRefusal, ApprovalReplyAddress, ApprovalRequestKey,
    ApprovalRouting, ApprovalSession, ApprovalSessionFacts, ApprovalState, CommittedApprovalOffer,
    PendingApprovalReply, ProviderSessionAvailability, RaiseAdmission,
    approval_lifecycle_admission::stop_binding,
    approval_store_reads::{
        ScopedHolderRequest, count_outstanding_approvals, read_holder_request,
        read_owed_approval_endings,
    },
    approval_store_scope::{ApprovalHolderAdmission, ApprovalHolderScope},
    approval_store_transaction::ApprovalStoreTransaction,
};

#[derive(Debug)]
pub enum ApprovalHolderCommand {
    Raise(Box<Raise>),
    Stop(StopRequest),
    DeliveryReply {
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    },
    Reoffer {
        request_id: ApprovalRequestId,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum ApprovalHolderReply {
    Raised(RaiseReply),
    Stopped(StopReply),
    NoReply,
}

impl ApprovalHolderCommand {
    fn approval_request_id(&self) -> &ApprovalRequestId {
        match self {
            Self::Raise(raise) => raise.request_id(),
            Self::Stop(stop) => stop_binding(stop).0,
            Self::DeliveryReply { request, .. } => request.request_id(),
            Self::Reoffer { request_id } => request_id,
        }
    }

    fn holder_binding_matches(&self, scope: &ApprovalHolderScope) -> bool {
        match self {
            Self::Raise(raise) => raise.router_run() == &scope.run.router_run,
            Self::Stop(stop) => stop_binding(stop).1 == &scope.run.router_run,
            Self::DeliveryReply { .. } | Self::Reoffer { .. } => true,
        }
    }

    fn authority_refused(&self, refusal: RunRefusal) -> ApprovalHolderReply {
        match self {
            Self::Raise(_) => ApprovalHolderReply::Raised(RaiseReply::AuthorityRefused { refusal }),
            Self::Stop(_) => ApprovalHolderReply::Stopped(StopReply::AuthorityRefused { refusal }),
            Self::DeliveryReply { .. } | Self::Reoffer { .. } => ApprovalHolderReply::NoReply,
        }
    }

    fn request_belongs_to_other_holder(&self) -> ApprovalHolderReply {
        match self {
            Self::Raise(_) => ApprovalHolderReply::Raised(RaiseReply::ContentsDiffer {}),
            Self::Stop(_) => ApprovalHolderReply::Stopped(StopReply::NotVisible {}),
            Self::DeliveryReply { .. } | Self::Reoffer { .. } => ApprovalHolderReply::NoReply,
        }
    }
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    /// Apply a request from an authenticated holder. The Raise body carries no route.
    pub async fn apply_holder_command(
        &mut self,
        holder: &ApprovalHolderAdmission,
        command: ApprovalHolderCommand,
    ) -> Result<PendingApprovalReply<ApprovalHolderReply>, ApprovalApplyFailure> {
        let applied = self.apply_holder(holder, command).await?;
        Ok(self.stage_reply(
            ApprovalReplyAddress::Holder(holder.holder_scope().clone()),
            applied,
        ))
    }

    async fn apply_holder(
        &mut self,
        holder: &ApprovalHolderAdmission,
        command: ApprovalHolderCommand,
    ) -> Result<ApprovalTransitionOutput<ApprovalHolderReply>, ApprovalApplyFailure> {
        let scope = holder.holder_scope();
        if let Err(refusal) = self
            .check_holder_fence(scope, command.holder_binding_matches(scope))
            .await?
        {
            return Ok(ApprovalTransitionOutput::with_reply(
                command.authority_refused(refusal),
            ));
        }

        let request_id = command.approval_request_id();
        let key = ApprovalRequestKey {
            run: scope.run.clone(),
            request_id: request_id.clone(),
        };
        let record = match read_holder_request(self.transaction, scope, request_id).await? {
            ScopedHolderRequest::OtherHolder => {
                return Ok(ApprovalTransitionOutput::with_reply(
                    command.request_belongs_to_other_holder(),
                ));
            }
            ScopedHolderRequest::Unseen => None,
            ScopedHolderRequest::Retained(record) => Some(record),
        };
        let prior = record
            .as_ref()
            .map_or(&ApprovalState::Unraised, |saved| &saved.state);
        let event = match command {
            ApprovalHolderCommand::Raise(raise) => {
                let (routing, admission) = if record.is_none() {
                    self.resolve_first_raise(scope, &raise).await?
                } else {
                    // The reducer replays the frozen row with its original route and admission.
                    (
                        ApprovalRouting::Owner,
                        RaiseAdmission {
                            provider_session: ProviderSessionAvailability::SessionFree,
                            outstanding: 0,
                        },
                    )
                };
                ApprovalEvent::Raise {
                    key,
                    raise,
                    routing,
                    admission,
                }
            }
            ApprovalHolderCommand::Stop(request) => ApprovalEvent::Stop { key, request },
            ApprovalHolderCommand::DeliveryReply { request, reply } => {
                ApprovalEvent::DeliveryReply {
                    key,
                    request,
                    reply,
                }
            }
            ApprovalHolderCommand::Reoffer { .. } => ApprovalEvent::Reoffer,
        };
        let at = self.sample_applying_time()?;
        let row_id = record.as_ref().map(|saved| &saved.row_id);
        let applied = self
            .apply_request_event(row_id, scope, prior, event, at)
            .await?;
        let reply = match &applied.reply {
            Some(ApprovalReceipt::Raise(reply)) => ApprovalHolderReply::Raised(reply.clone()),
            Some(ApprovalReceipt::Stop(reply)) => ApprovalHolderReply::Stopped(reply.clone()),
            None => ApprovalHolderReply::NoReply,
            Some(
                ApprovalReceipt::Decide(_) | ApprovalReceipt::Read(_) | ApprovalReceipt::PassOn(_),
            ) => {
                return Err(corrupt("approval_store.holder_reply").into());
            }
        };
        Ok(applied.replace_reply(reply))
    }

    /// Recover all current Owed endings for the exact admitted run.
    pub async fn recover_owed_approval_endings(
        &mut self,
        holder: &ApprovalHolderAdmission,
    ) -> Result<PendingApprovalReply<Result<(), RunRefusal>>, ApprovalApplyFailure> {
        let applied = self.read_owed_endings(holder).await?;
        Ok(self.stage_reply(
            ApprovalReplyAddress::Holder(holder.holder_scope().clone()),
            applied,
        ))
    }

    async fn read_owed_endings(
        &mut self,
        holder: &ApprovalHolderAdmission,
    ) -> Result<ApprovalTransitionOutput<Result<(), RunRefusal>>, ApprovalApplyFailure> {
        let scope = holder.holder_scope();
        if let Err(refusal) = self.check_holder_fence(scope, true).await? {
            return Ok(ApprovalTransitionOutput::with_reply(Err(refusal)));
        }
        let records = read_owed_approval_endings(self.transaction, scope).await?;
        let mut pending = ApprovalTransitionOutput::with_reply(Ok(()));
        for record in records {
            let ApprovalState::Ended { closed, .. } = record.state else {
                return Err(corrupt("approval_requests.owed_selection").into());
            };
            if record.scope != *scope
                || closed.delivery != (approval_request_protocol::Delivery::Owed {})
            {
                return Err(corrupt("approval_requests.owed_selection").into());
            }
            pending.ending_offers.push(CommittedApprovalOffer::new(
                record.scope,
                Box::new(closed.ending),
            ));
        }
        Ok(pending)
    }

    async fn resolve_first_raise(
        &mut self,
        scope: &ApprovalHolderScope,
        raise: &Raise,
    ) -> Result<(ApprovalRouting, RaiseAdmission), ApprovalApplyFailure> {
        let (routing, provider_session) = match raise.requesting_session() {
            None => (
                ApprovalRouting::Owner,
                ProviderSessionAvailability::SessionFree,
            ),
            Some(session_id) => {
                let requester = ApprovalSession {
                    enrollment: scope.run.enrollment.clone(),
                    session_id: session_id.clone(),
                };
                let route = self
                    .sessions
                    .session_initial_route(self.transaction, &requester)
                    .await?;
                let Some(route) = route else {
                    return Ok(unavailable_requester());
                };
                if !self
                    .sessions
                    .session_is_live(self.transaction, &requester)
                    .await?
                {
                    return Ok(unavailable_requester());
                }
                if let ApprovalRouting::Delegate { admitted_parent } = &route
                    && (admitted_parent.session_id == requester.session_id
                        || !self
                            .sessions
                            .session_is_live(self.transaction, admitted_parent)
                            .await?)
                {
                    return Err(ApprovalApplyFailure::Lifecycle(
                        ApprovalRefusal::InvalidInitialRoute,
                    ));
                }
                (route, ProviderSessionAvailability::Live)
            }
        };
        let outstanding = count_outstanding_approvals(self.transaction, scope).await?;
        Ok((
            routing,
            RaiseAdmission {
                provider_session,
                outstanding,
            },
        ))
    }
}

fn unavailable_requester() -> (ApprovalRouting, RaiseAdmission) {
    (
        ApprovalRouting::Owner,
        RaiseAdmission {
            provider_session: ProviderSessionAvailability::NotAvailable,
            outstanding: 0,
        },
    )
}
