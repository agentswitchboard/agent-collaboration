use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, ErasedApprovalMarker, Raise, RaiseReply, RunRefusal,
    StopReply, StopRequest,
};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::ApprovalRequestId;

use super::{ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput};
use crate::{
    ApprovalEvent, ApprovalReceipt, ApprovalRefusal, ApprovalReplyAddress, ApprovalRequestKey,
    ApprovalRouting, ApprovalSession, ApprovalSessionFacts, ApprovalState, PendingApprovalReply,
    ProviderSessionAvailability, RaiseAdmission,
    approval_lifecycle_admission::stop_binding,
    approval_retention::read_erased_approval,
    approval_store_reads::{
        ScopedHolderRequest, count_outstanding_approvals, read_holder_request,
        read_recoverable_approval_rows,
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

    fn erased_request(&self, marker: ErasedApprovalMarker) -> ApprovalHolderReply {
        match self {
            Self::Raise(_) => ApprovalHolderReply::Raised(RaiseReply::AlreadyEnded { marker }),
            Self::Stop(_) => ApprovalHolderReply::Stopped(StopReply::Stands {
                ending: marker.ending,
            }),
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

    /// Stage only a Raise reply. A mismatched receipt rejects the applying loan.
    pub async fn raise_from_holder(
        &mut self,
        holder: &ApprovalHolderAdmission,
        raise: Box<Raise>,
    ) -> Result<PendingApprovalReply<RaiseReply>, ApprovalApplyFailure> {
        let transition = self
            .apply_holder(holder, ApprovalHolderCommand::Raise(raise))
            .await?;
        let ApprovalHolderReply::Raised(ref reply) = transition.reply else {
            return Err(corrupt("approval_store.holder_raise_reply").into());
        };
        let reply = reply.clone();
        Ok(self.stage_reply(
            ApprovalReplyAddress::Holder(holder.holder_scope().clone()),
            transition.replace_reply(reply),
        ))
    }

    /// Stage only a Stop reply. A mismatched receipt rejects the applying loan.
    pub async fn stop_from_holder(
        &mut self,
        holder: &ApprovalHolderAdmission,
        stop: StopRequest,
    ) -> Result<PendingApprovalReply<StopReply>, ApprovalApplyFailure> {
        let transition = self
            .apply_holder(holder, ApprovalHolderCommand::Stop(stop))
            .await?;
        let ApprovalHolderReply::Stopped(ref reply) = transition.reply else {
            return Err(corrupt("approval_store.holder_stop_reply").into());
        };
        let reply = reply.clone();
        Ok(self.stage_reply(
            ApprovalReplyAddress::Holder(holder.holder_scope().clone()),
            transition.replace_reply(reply),
        ))
    }

    /// Stage only successful application of a correlated holder delivery answer.
    pub async fn record_holder_delivery(
        &mut self,
        holder: &ApprovalHolderAdmission,
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    ) -> Result<PendingApprovalReply<()>, ApprovalApplyFailure> {
        let transition = self
            .apply_holder(
                holder,
                ApprovalHolderCommand::DeliveryReply { request, reply },
            )
            .await?;
        match transition.reply {
            ApprovalHolderReply::NoReply => Ok(self.stage_reply(
                ApprovalReplyAddress::Holder(holder.holder_scope().clone()),
                transition.replace_reply(()),
            )),
            ApprovalHolderReply::Raised(_) | ApprovalHolderReply::Stopped(_) => {
                Err(corrupt("approval_store.delivery_reply").into())
            }
        }
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

        let at = self.sample_applying_time()?;
        let request_id = command.approval_request_id();
        let key = ApprovalRequestKey {
            run: scope.run.clone(),
            request_id: request_id.clone(),
        };
        let record = match read_holder_request(self.transaction, scope, request_id, at).await? {
            ScopedHolderRequest::OtherHolder => {
                return Ok(ApprovalTransitionOutput::with_reply(
                    command.request_belongs_to_other_holder(),
                ));
            }
            ScopedHolderRequest::Unseen => {
                match read_erased_approval(self.transaction, scope, request_id).await? {
                    crate::approval_retention::ErasedApprovalLookup::Missing => None,
                    crate::approval_retention::ErasedApprovalLookup::OtherHolder => {
                        return Ok(ApprovalTransitionOutput::with_reply(
                            command.request_belongs_to_other_holder(),
                        ));
                    }
                    crate::approval_retention::ErasedApprovalLookup::Owned(marker) => {
                        return Ok(ApprovalTransitionOutput::with_reply(
                            command.erased_request(marker),
                        ));
                    }
                }
            }
            ScopedHolderRequest::Retained(record) => Some(record),
            ScopedHolderRequest::PastCutoff(cutoff) => {
                let reply = match &command {
                    ApprovalHolderCommand::Raise(_) | ApprovalHolderCommand::Stop(_) => {
                        command.erased_request(cutoff.marker(self.transaction).await?)
                    }
                    ApprovalHolderCommand::DeliveryReply { .. }
                    | ApprovalHolderCommand::Reoffer { .. } => ApprovalHolderReply::NoReply,
                };
                return Ok(ApprovalTransitionOutput::with_reply(reply));
            }
        };
        let prior = record
            .as_ref()
            .map_or(&ApprovalState::Unraised, |saved| &saved.state);
        let event = match command {
            ApprovalHolderCommand::Raise(raise) => {
                let (routing, admission) = if record.is_none() {
                    self.resolve_first_raise(scope, &raise, at).await?
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

    /// A staged live effect may outlive the durable debt it represented.
    pub async fn holder_ending_is_owed(
        &mut self,
        holder: &ApprovalHolderScope,
        request_id: &approval_request_protocol::ApprovalRequestId,
    ) -> Result<bool, central_storage::StoreFailure> {
        let at = self.sample_applying_time()?;
        let record = read_holder_request(self.transaction, holder, request_id, at).await?;
        let ScopedHolderRequest::Retained(record) = record else {
            return Ok(false);
        };
        match &record.state {
            ApprovalState::Ended { closed, .. } => {
                Ok(closed.delivery == (approval_request_protocol::Delivery::Owed {}))
            }
            ApprovalState::Unraised
            | ApprovalState::StoppedBeforeRaise { .. }
            | ApprovalState::Waiting { .. } => Ok(false),
        }
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
        let at = self.sample_applying_time()?;
        let records = read_recoverable_approval_rows(self.transaction, scope, at).await?;
        let mut pending = ApprovalTransitionOutput::with_reply(Ok(()));
        for record in records {
            if record.scope != *scope {
                return Err(corrupt("approval_requests.recover_holder").into());
            }
            let due_or_owed = match &record.state {
                ApprovalState::Waiting { saved } => at >= saved.window.deadline(),
                ApprovalState::Ended { closed, .. } => {
                    closed.delivery == (approval_request_protocol::Delivery::Owed {})
                }
                ApprovalState::Unraised | ApprovalState::StoppedBeforeRaise { .. } => false,
            };
            if due_or_owed {
                let recovered = self
                    .apply_request_event(
                        Some(&record.row_id),
                        scope,
                        &record.state,
                        ApprovalEvent::Reoffer,
                        at,
                    )
                    .await?;
                pending.ending_offers.extend(recovered.ending_offers);
            }
        }
        Ok(pending)
    }

    async fn resolve_first_raise(
        &mut self,
        scope: &ApprovalHolderScope,
        raise: &Raise,
        at: approval_request_protocol::CentralTime,
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
                    && admitted_parent.session_id == requester.session_id
                {
                    return Err(ApprovalApplyFailure::Lifecycle(
                        ApprovalRefusal::InvalidInitialRoute,
                    ));
                }
                (route, ProviderSessionAvailability::Live)
            }
        };
        let outstanding = count_outstanding_approvals(self.transaction, scope, at).await?;
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
