//! The store's production person commands take authority from fleet-signin.

use approval_request_protocol::{ApprovalSummaryState, RequestSummary};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_signin::{
    ApprovalPersonAdmission,
    approval_person_commands::{ApprovalListFilter, ApprovalListReply, ApprovalReadReply},
};
use workspace_ids::ApprovalRequestId;

use super::{ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput};
use crate::{
    ApprovalReplyAddress, ApprovalStoreTransaction, PendingApprovalReply,
    approval_store_reads::read_approval_request_page_for_person,
};

const APPROVAL_PAGE_SIZE: u16 = 100;

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
{
    pub(super) async fn read_person_request(
        &mut self,
        admission: &ApprovalPersonAdmission,
        request_id: &ApprovalRequestId,
    ) -> Result<PendingApprovalReply<ApprovalReadReply>, ApprovalApplyFailure> {
        let applied = self
            .read_person_approval(admission.organization(), admission.person(), request_id)
            .await?;
        let reply = match applied.reply {
            Some(ref view) => ApprovalReadReply::Request { view: view.clone() },
            None => ApprovalReadReply::NotVisible {},
        };
        Ok(self.stage_reply(reply_address(admission), applied.replace_reply(reply)))
    }

    pub(super) async fn list_person_requests(
        &mut self,
        admission: &ApprovalPersonAdmission,
        filter: ApprovalListFilter,
        cursor: Option<&ApprovalRequestId>,
    ) -> Result<PendingApprovalReply<ApprovalListReply>, ApprovalApplyFailure> {
        let address = reply_address(admission);
        if admission.organization() != self.transaction.organization_id() {
            return Ok(self.stage_reply(
                address,
                ApprovalTransitionOutput::with_reply(ApprovalListReply::Page {
                    items: Vec::new(),
                    next: None,
                }),
            ));
        }

        let at = self.sample_applying_time()?;
        let mut records = read_approval_request_page_for_person(
            self.transaction,
            admission.organization(),
            cursor,
            filter,
            at,
            APPROVAL_PAGE_SIZE,
        )
        .await?;
        let page_size = usize::from(APPROVAL_PAGE_SIZE);
        let next = if records.len() > page_size {
            records.truncate(page_size);
            records
                .last()
                .and_then(|record| record.state.key())
                .map(|key| key.request_id.clone())
        } else {
            None
        };
        let mut items = Vec::with_capacity(records.len());
        let mut ending_offers = Vec::new();
        for record in &records {
            let applied = self
                .read_selected_approval(
                    record,
                    &crate::ApprovalActor::Person(admission.person().clone()),
                    at,
                )
                .await?;
            let view = applied
                .reply
                .ok_or_else(|| corrupt("approval_request_destinations.people_page"))?;
            let summary = RequestSummary::from(&view);
            let includes_summary = match filter {
                ApprovalListFilter::All => true,
                ApprovalListFilter::Waiting => {
                    matches!(summary.state, ApprovalSummaryState::Waiting {})
                }
            };
            if includes_summary {
                items.push(summary);
            }
            ending_offers.extend(applied.ending_offers);
        }
        Ok(self.stage_reply(
            address,
            ApprovalTransitionOutput {
                reply: ApprovalListReply::Page { items, next },
                ending_offers,
            },
        ))
    }
}

pub(super) fn reply_address(admission: &ApprovalPersonAdmission) -> ApprovalReplyAddress {
    ApprovalReplyAddress::Person {
        organization: admission.organization().clone(),
        person: admission.person().clone(),
    }
}
