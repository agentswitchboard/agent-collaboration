use approval_request_protocol::{CentralTime, RegisterRunReply, RunRefusal};
use central_engine_model::InstanceKey;
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::workspace_decode_error::corrupt;

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, PendingApprovalApply, PendingApprovalOffer,
};
use crate::{
    ApprovalEvent, ApprovalSession, ApprovalSessionFacts,
    approval_store_reads::{
        read_approval_due_subject, read_approval_run, read_last_approval_run,
        read_outstanding_approval_rows, read_waiting_approval_session_rows,
    },
    approval_store_scope::{ApprovalEnrollmentScope, ApprovalHolderScope, ApprovalRowId},
    approval_store_transaction::{ApprovalEnrollmentStanding, ApprovalStoreTransaction},
    approval_store_writes::{plan_register_approval_run, plan_retire_approval_run},
};

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn register_approval_run(
        &mut self,
        scope: &ApprovalHolderScope,
    ) -> Result<PendingApprovalApply<RegisterRunReply>, ApprovalApplyFailure> {
        let enrollment = scope.enrollment_scope();
        if let Some(refusal) = self.enrollment_refusal(&enrollment).await? {
            return Ok(PendingApprovalApply::with_reply(register_refusal(refusal)));
        }
        if let Some(existing) = read_approval_run(self.transaction, &scope.run.router_run).await? {
            let reply = if &existing.scope != scope {
                RegisterRunReply::RunNotRegistered {}
            } else if existing.retired_at.is_some() {
                RegisterRunReply::RunRetired {}
            } else {
                RegisterRunReply::Registered {}
            };
            return Ok(PendingApprovalApply::with_reply(reply));
        }

        let prior = read_last_approval_run(self.transaction, &enrollment).await?;
        let order = prior
            .as_ref()
            .map_or(0, |run| run.registration_order)
            .checked_add(1)
            .ok_or_else(|| corrupt("approval_router_runs.registration_order"))?;
        let at = self.sample_applying_time()?;
        if let Some(prior) = prior.filter(|prior| prior.retired_at.is_none()) {
            self.retire_current_run(&prior.scope, ApprovalEvent::RunRetired, at)
                .await?;
        }
        let write = plan_register_approval_run(scope, order, at)?;
        self.execute_guarded_write(write).await?;
        Ok(PendingApprovalApply::with_reply(
            RegisterRunReply::Registered {},
        ))
    }

    pub async fn retire_approval_run(
        &mut self,
        scope: &ApprovalHolderScope,
    ) -> Result<PendingApprovalApply<Result<(), RunRefusal>>, ApprovalApplyFailure> {
        if let Err(refusal) = self.check_holder_fence(scope, true).await? {
            return Ok(PendingApprovalApply::with_reply(Err(refusal)));
        }
        let at = self.sample_applying_time()?;
        self.retire_current_run(scope, ApprovalEvent::RunRetired, at)
            .await?;
        Ok(PendingApprovalApply::with_reply(Ok(())))
    }

    pub async fn end_approval_enrollment(
        &mut self,
        enrollment: &ApprovalEnrollmentScope,
    ) -> Result<(), ApprovalApplyFailure> {
        if self
            .transaction
            .approval_enrollment_standing(enrollment)
            .await?
            != ApprovalEnrollmentStanding::Ended
        {
            return Ok(());
        }
        let prior = read_last_approval_run(self.transaction, enrollment).await?;
        if let Some(prior) = prior.filter(|prior| prior.retired_at.is_none()) {
            let at = self.sample_applying_time()?;
            self.retire_current_run(&prior.scope, ApprovalEvent::EnrollmentEnded, at)
                .await?;
        }
        Ok(())
    }

    pub async fn end_approval_session(
        &mut self,
        organization: &OrganizationId,
        session: &ApprovalSession,
    ) -> Result<Vec<PendingApprovalOffer>, ApprovalApplyFailure> {
        if self
            .sessions
            .session_is_live(self.transaction, organization, session)
            .await?
        {
            return Ok(Vec::new());
        }
        let records =
            read_waiting_approval_session_rows(self.transaction, organization, session).await?;
        let at = self.sample_applying_time()?;
        let mut offers = Vec::new();
        for record in records {
            let applied = self
                .apply_request_event(
                    Some(&record.row_id),
                    &record.scope,
                    &record.state,
                    ApprovalEvent::SessionEnded,
                    at,
                )
                .await?;
            offers.extend(applied.ending_offers);
        }
        Ok(offers)
    }

    pub async fn apply_approval_deadline(
        &mut self,
        subject: &InstanceKey,
        deadline: CentralTime,
    ) -> Result<Vec<PendingApprovalOffer>, ApprovalApplyFailure> {
        let row_id = ApprovalRowId::parse(&subject.0)?;
        let record = read_approval_due_subject(self.transaction, &row_id).await?;
        let Some(record) = record else {
            return Ok(Vec::new());
        };
        let at = self.sample_applying_time()?;
        let applied = self
            .apply_request_event(
                Some(&record.row_id),
                &record.scope,
                &record.state,
                ApprovalEvent::Alarm { deadline },
                at,
            )
            .await?;
        Ok(applied.ending_offers)
    }

    pub(super) async fn check_holder_fence(
        &mut self,
        scope: &ApprovalHolderScope,
        binding_matches: bool,
    ) -> Result<Result<(), RunRefusal>, StoreFailure> {
        if let Some(refusal) = self.enrollment_refusal(&scope.enrollment_scope()).await? {
            return Ok(Err(refusal));
        }
        if !binding_matches {
            return Ok(Err(RunRefusal::RunRetired {}));
        }
        let existing = read_approval_run(self.transaction, &scope.run.router_run).await?;
        let Some(existing) = existing.filter(|existing| &existing.scope == scope) else {
            return Ok(Err(RunRefusal::RunNotRegistered {}));
        };
        if existing.retired_at.is_some() {
            return Ok(Err(RunRefusal::RunRetired {}));
        }
        Ok(Ok(()))
    }

    async fn enrollment_refusal(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> Result<Option<RunRefusal>, StoreFailure> {
        let standing = self.transaction.approval_enrollment_standing(scope).await?;
        Ok(match standing {
            ApprovalEnrollmentStanding::Live => None,
            ApprovalEnrollmentStanding::Ended => Some(RunRefusal::EnrollmentEnded {}),
            ApprovalEnrollmentStanding::Unavailable => Some(RunRefusal::RunNotRegistered {}),
        })
    }

    async fn retire_current_run(
        &mut self,
        scope: &ApprovalHolderScope,
        event: ApprovalEvent,
        at: CentralTime,
    ) -> Result<(), ApprovalApplyFailure> {
        let outstanding = read_outstanding_approval_rows(self.transaction, scope).await?;
        self.execute_guarded_write(plan_retire_approval_run(scope, at))
            .await?;
        for record in outstanding {
            let applied = self
                .apply_request_event(
                    Some(&record.row_id),
                    &record.scope,
                    &record.state,
                    event.clone(),
                    at,
                )
                .await?;
            if !applied.ending_offers.is_empty() || applied.reply.is_some() {
                return Err(corrupt("approval_store.retirement_output").into());
            }
        }
        Ok(())
    }
}

fn register_refusal(refusal: RunRefusal) -> RegisterRunReply {
    match refusal {
        RunRefusal::EnrollmentEnded {} => RegisterRunReply::EnrollmentEnded {},
        RunRefusal::RunNotRegistered {} => RegisterRunReply::RunNotRegistered {},
        RunRefusal::RunRetired {} => RegisterRunReply::RunRetired {},
    }
}
