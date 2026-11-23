use std::num::NonZeroU16;

use approval_request_protocol::{CentralTime, PassOnReply, RequestView};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::{ApprovalRequestId, PersonId};

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalReadPage, ApprovalTransitionOutput,
};
use crate::{
    AdmittedDecider, ApprovalActor, ApprovalDecideInput, ApprovalDecisionReply, ApprovalEvent,
    ApprovalReceipt, ApprovalSession, ApprovalSessionFacts,
    approval_decision_log::ApprovalDecisionLogKey,
    approval_lifecycle_admission::is_destination,
    approval_store_reads::{
        ApprovalReadRecord, read_parent_approval_request, read_people_approval_request,
        read_person_approval_page,
    },
    approval_store_scope::ApprovalRequestRowId,
    approval_store_transaction::ApprovalStoreTransaction,
};

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub(super) async fn decide_approval(
        &mut self,
        organization: &OrganizationId,
        decider: AdmittedDecider,
        request: ApprovalDecideInput,
    ) -> Result<ApprovalTransitionOutput<ApprovalDecisionReply>, ApprovalApplyFailure> {
        let refused = || ApprovalTransitionOutput::with_reply(ApprovalDecisionReply::NotVisible {});
        if organization != self.transaction.organization_id() {
            return Ok(refused());
        }
        let actor = decider.actor();
        if let ApprovalActor::Session(session) = &actor
            && !self
                .sessions
                .session_is_live(self.transaction, organization, session)
                .await?
        {
            return Ok(refused());
        }
        let record = match &actor {
            ApprovalActor::Person(_) => {
                read_people_approval_request(self.transaction, organization, &request.request_id)
                    .await?
            }
            ApprovalActor::Session(session) => {
                read_parent_approval_request(
                    self.transaction,
                    organization,
                    &request.request_id,
                    session,
                )
                .await?
            }
        };
        let Some(record) = record else {
            return Ok(refused());
        };
        let Some(saved) = record
            .state
            .retained()
            .filter(|saved| is_destination(saved, &actor))
        else {
            return Ok(refused());
        };
        let key = saved.key.clone();
        let recipient = actor.identity();
        let at = self.sample_applying_time()?;
        let caller = self
            .transaction
            .approval_decision_caller(organization, &actor)
            .await?;
        let request_log = ApprovalDecisionLogKey::new(organization, caller, &request, at)?;
        let retained = self
            .transaction
            .read_approval_statement(request_log.lookup_statement())
            .await?;
        if let Some(reply) = request_log.replay(&retained, &request, &recipient, &record.state)? {
            if let crate::ApprovalState::Waiting { saved } = &record.state
                && at >= saved.window.deadline()
            {
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
                return Ok(expired.replace_reply(reply));
            }
            return Ok(ApprovalTransitionOutput::with_reply(reply));
        }
        let event = ApprovalEvent::Decide {
            key: key.clone(),
            actor: decider,
            request,
        };
        let applied = self
            .apply_request_event(
                Some(&record.row_id),
                &record.scope,
                &record.state,
                event,
                at,
            )
            .await?;
        let Some(ApprovalReceipt::Decide(reply)) = &applied.reply else {
            return Err(corrupt("approval_store.decide_reply").into());
        };
        let reply = reply
            .for_recipient(&key, &recipient)
            .map_err(|_| corrupt("approval_store.decide_recipient"))?
            .clone();
        for write in request_log.retain(&reply)? {
            self.execute_guarded_write(write).await?;
        }
        Ok(applied.replace_reply(reply))
    }

    pub(super) async fn pass_on_approval(
        &mut self,
        organization: &OrganizationId,
        parent: ApprovalSession,
        request_id: &ApprovalRequestId,
    ) -> Result<ApprovalTransitionOutput<PassOnReply>, ApprovalApplyFailure> {
        let refused = || ApprovalTransitionOutput::with_reply(PassOnReply::NotVisible {});
        if organization != self.transaction.organization_id() {
            return Ok(refused());
        }
        if !self
            .sessions
            .session_is_live(self.transaction, organization, &parent)
            .await?
        {
            return Ok(refused());
        }
        let record =
            read_parent_approval_request(self.transaction, organization, request_id, &parent)
                .await?;
        let Some(record) = record else {
            return Ok(refused());
        };
        let Some(key) = record.state.key() else {
            return Ok(refused());
        };
        let identity = ApprovalActor::Session(parent.clone()).identity();
        let event = ApprovalEvent::PassOn {
            key: key.clone(),
            live_parent: parent,
        };
        let at = self.sample_applying_time()?;
        let applied = self
            .apply_request_event(
                Some(&record.row_id),
                &record.scope,
                &record.state,
                event,
                at,
            )
            .await?;
        let Some(ApprovalReceipt::PassOn(reply)) = &applied.reply else {
            return Err(corrupt("approval_store.pass_on_reply").into());
        };
        let reply = reply
            .for_recipient(key, &identity)
            .map_err(|_| corrupt("approval_store.pass_on_recipient"))?;
        let reply = reply.clone();
        Ok(applied.replace_reply(reply))
    }

    pub(super) async fn read_person_approval(
        &mut self,
        organization: &OrganizationId,
        person_id: &PersonId,
        request_id: &ApprovalRequestId,
    ) -> Result<ApprovalTransitionOutput<Option<RequestView>>, ApprovalApplyFailure> {
        if organization != self.transaction.organization_id() {
            return Ok(ApprovalTransitionOutput::with_reply(None));
        }
        let record =
            read_people_approval_request(self.transaction, organization, request_id).await?;
        let Some(record) = record else {
            return Ok(ApprovalTransitionOutput::with_reply(None));
        };
        let at = self.sample_applying_time()?;
        self.read_selected_approval(&record, person_id, at).await
    }

    pub(super) async fn list_person_approvals(
        &mut self,
        organization: &OrganizationId,
        person_id: &PersonId,
        after: Option<&ApprovalRequestRowId>,
        limit: NonZeroU16,
    ) -> Result<ApprovalTransitionOutput<ApprovalReadPage>, ApprovalApplyFailure> {
        if organization != self.transaction.organization_id() {
            return Ok(ApprovalTransitionOutput::with_reply(ApprovalReadPage {
                requests: Vec::new(),
                next_after: None,
            }));
        }
        let mut records =
            read_person_approval_page(self.transaction, organization, after, limit).await?;
        let page_size = usize::from(limit.get());
        let next_after = if records.len() > page_size {
            records.truncate(page_size);
            records.last().map(|record| record.row_id.clone())
        } else {
            None
        };
        let at = self.sample_applying_time()?;
        let mut requests = Vec::with_capacity(records.len());
        let mut ending_offers = Vec::new();
        for record in &records {
            let applied = self.read_selected_approval(record, person_id, at).await?;
            let request = applied
                .reply
                .ok_or_else(|| corrupt("approval_request_destinations.people_page"))?;
            requests.push(request);
            ending_offers.extend(applied.ending_offers);
        }
        Ok(ApprovalTransitionOutput {
            reply: ApprovalReadPage {
                requests,
                next_after,
            },
            ending_offers,
        })
    }

    async fn read_selected_approval(
        &mut self,
        record: &ApprovalReadRecord,
        person: &PersonId,
        at: CentralTime,
    ) -> Result<ApprovalTransitionOutput<Option<RequestView>>, ApprovalApplyFailure> {
        let Some(key) = record.state.key() else {
            return Ok(ApprovalTransitionOutput::with_reply(None));
        };
        let event = ApprovalEvent::Read {
            key: key.clone(),
            person_id: person.clone(),
        };
        let applied = self
            .apply_request_event(
                Some(&record.row_id),
                &record.scope,
                &record.state,
                event,
                at,
            )
            .await?;
        let Some(ApprovalReceipt::Read(reply)) = &applied.reply else {
            return Err(corrupt("approval_store.read_reply").into());
        };
        let recipient = ApprovalActor::Person(person.clone()).identity();
        let reply = reply
            .for_recipient(key, &recipient)
            .map_err(|_| corrupt("approval_store.read_recipient"))?
            .clone();
        Ok(applied.replace_reply(reply))
    }
}
