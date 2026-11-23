use std::num::NonZeroU16;

use approval_request_protocol::{PassOnReply, RequestView};
use fleet_identity_protocol::OrganizationId;
use workspace_ids::{ApprovalRequestId, PersonId};

use super::{ApprovalApplyFailure, ApprovalApplyingStore, ApprovalReadPage};
use crate::{
    AdmittedDecider, ApprovalActor, ApprovalDecideInput, ApprovalDecisionReply,
    ApprovalReplyAddress, ApprovalRequestRowId, ApprovalSession, ApprovalSessionFacts,
    ApprovalStoreTransaction, PendingApprovalReply,
};

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn fixture_decide_approval(
        &mut self,
        organization: &OrganizationId,
        decider: AdmittedDecider,
        request: ApprovalDecideInput,
    ) -> Result<PendingApprovalReply<ApprovalDecisionReply>, ApprovalApplyFailure> {
        let address = match decider.actor() {
            ApprovalActor::Person(person) => ApprovalReplyAddress::Person {
                organization: organization.clone(),
                person,
            },
            ApprovalActor::Session(session) => ApprovalReplyAddress::Session {
                organization: organization.clone(),
                session,
            },
        };
        let applied = self.decide_approval(organization, decider, request).await?;
        Ok(self.stage_reply(address, applied))
    }

    pub async fn fixture_pass_on_approval(
        &mut self,
        organization: &OrganizationId,
        parent: ApprovalSession,
        request_id: &ApprovalRequestId,
    ) -> Result<PendingApprovalReply<PassOnReply>, ApprovalApplyFailure> {
        let address = ApprovalReplyAddress::Session {
            organization: organization.clone(),
            session: parent.clone(),
        };
        let applied = self
            .pass_on_approval(organization, parent, request_id)
            .await?;
        Ok(self.stage_reply(address, applied))
    }

    pub async fn fixture_read_person_approval(
        &mut self,
        organization: &OrganizationId,
        person_id: &PersonId,
        request_id: &ApprovalRequestId,
    ) -> Result<PendingApprovalReply<Option<RequestView>>, ApprovalApplyFailure> {
        let address = ApprovalReplyAddress::Person {
            organization: organization.clone(),
            person: person_id.clone(),
        };
        let applied = self
            .read_person_approval(organization, person_id, request_id)
            .await?;
        Ok(self.stage_reply(address, applied))
    }

    pub async fn fixture_list_person_approvals(
        &mut self,
        organization: &OrganizationId,
        person_id: &PersonId,
        after: Option<&ApprovalRequestRowId>,
        limit: NonZeroU16,
    ) -> Result<PendingApprovalReply<ApprovalReadPage>, ApprovalApplyFailure> {
        let address = ApprovalReplyAddress::Person {
            organization: organization.clone(),
            person: person_id.clone(),
        };
        let applied = self
            .list_person_approvals(organization, person_id, after, limit)
            .await?;
        Ok(self.stage_reply(address, applied))
    }
}
