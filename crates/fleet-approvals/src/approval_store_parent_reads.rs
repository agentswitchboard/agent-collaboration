//! Private approval projections for an authenticated destination session.
use approval_request_protocol::{
    ApprovalRequestId, ListApprovalsForSessionReply, ReadApprovalForSessionReply, RequestSummary,
};
use fleet_actor_identity::AuthenticatedSession;
use fleet_engine::checked_engine_statement;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};

use crate::{
    ApprovalActor, ApprovalSessionFacts, ApprovalState, PendingApprovalReply,
    approval_store_reads::{read_parent_approval_request, read_session_approval_page},
    approval_store_transaction::ApprovalStoreTransaction,
};

use super::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalTransitionOutput,
    destination_commands::approval_session_address,
};

const SESSION_APPROVAL_PAGE_SIZE: usize = 100;

pub enum SessionReadApply {
    Unchanged(PendingApprovalReply<ReadApprovalForSessionReply>),
    Applied(PendingApprovalReply<ReadApprovalForSessionReply>),
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn read_for_session(
        &mut self,
        admitted: &AuthenticatedSession,
        request_id: &ApprovalRequestId,
    ) -> Result<SessionReadApply, ApprovalApplyFailure> {
        let (organization, session, address) = approval_session_address(admitted);
        let record =
            read_parent_approval_request(self.transaction, organization, request_id, &session)
                .await?;
        let Some(record) = record else {
            return Ok(SessionReadApply::Unchanged(self.stage_reply(
                address,
                ApprovalTransitionOutput::with_reply(ReadApprovalForSessionReply::NotVisible {}),
            )));
        };
        let at = self.sample_applying_time()?;
        let actor = ApprovalActor::Session(session.clone());
        let applied = self.read_selected_approval(&record, &actor, at).await?;
        let mut changed = matches!(&record.state, ApprovalState::Waiting { saved } if at >= saved.window.deadline());
        let reply = if let Some(view) = &applied.reply {
            if matches!(&record.state, ApprovalState::Waiting { saved } if at < saved.window.deadline())
                && record.session_destinations.iter().any(|destination| {
                    destination.session == session && destination.first_read_at.is_none()
                })
            {
                let read = checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/mark_approval_destination_read.sql",
                    read_at = format_utc_millis(at),
                    row_id = record.row_id.as_str().to_owned(),
                    organization = organization.as_str().to_owned(),
                    machine = session.enrollment.machine_id.as_str().to_owned(),
                    enrollment = session.enrollment.enrollment_id.as_str().to_owned(),
                    session = session.session_id.as_str().to_owned(),
                );
                if self.transaction.execute_approval_statement(read).await? != 1 {
                    return Err(ApprovalApplyFailure::GuardChanged);
                }
                changed = true;
            }
            ReadApprovalForSessionReply::Request {
                view: Box::new(view.clone()),
            }
        } else {
            ReadApprovalForSessionReply::NotVisible {}
        };
        let pending = self.stage_reply(address, applied.replace_reply(reply));
        Ok(if changed {
            SessionReadApply::Applied(pending)
        } else {
            SessionReadApply::Unchanged(pending)
        })
    }

    pub async fn list_for_session(
        &mut self,
        admitted: &AuthenticatedSession,
        cursor: Option<&ApprovalRequestId>,
    ) -> Result<PendingApprovalReply<ListApprovalsForSessionReply>, ApprovalApplyFailure> {
        let (organization, session, address) = approval_session_address(admitted);
        let mut records = read_session_approval_page(
            self.transaction,
            organization,
            &session,
            cursor,
            SESSION_APPROVAL_PAGE_SIZE + 1,
        )
        .await?;
        let has_more = records.len() > SESSION_APPROVAL_PAGE_SIZE;
        records.truncate(SESSION_APPROVAL_PAGE_SIZE);
        let at = self.sample_applying_time()?;
        let actor = ApprovalActor::Session(session);
        let mut items = Vec::with_capacity(records.len());
        let mut ending_offers = Vec::new();
        for record in &records {
            let applied = self.read_selected_approval(record, &actor, at).await?;
            let view = applied
                .reply
                .ok_or_else(|| corrupt("approval_request_destinations.session_page"))?;
            items.push(RequestSummary::from(&view));
            ending_offers.extend(applied.ending_offers);
        }
        let next = if has_more {
            items.last().map(|item| item.request_id.clone())
        } else {
            None
        };
        Ok(self.stage_reply(
            address,
            ApprovalTransitionOutput {
                reply: ListApprovalsForSessionReply::Page { items, next },
                ending_offers,
            },
        ))
    }
}
