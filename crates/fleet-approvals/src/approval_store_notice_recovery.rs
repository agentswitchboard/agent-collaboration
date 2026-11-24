//! Recover durable unread session notices for the welcomed machine.
use std::collections::BTreeSet;

use approval_request_protocol::{ApprovalRequestId, CentralTime};
use central_engine_model::InstanceKey;
use central_storage::SafeInteger;
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_identity_protocol::OrganizationId;
use fleet_machine_login::link_authority::ValidatedMachineConnection;
use fleet_runtime_ports::workspace_decode_error::corrupt;

use super::{ApprovalApplyFailure, ApprovalApplyingStore};
use crate::{
    ApprovalSession, ApprovalSessionFacts, ApprovalState, CommittedApprovalNotice,
    approval_store_reads::{read_approval_by_row_id, read_parent_approval_request},
    approval_store_scope::{ApprovalDestinationRowId, ApprovalRequestRowId},
    approval_store_transaction::ApprovalStoreTransaction,
};

const NOTICE_PAGE_SIZE: i64 = 100;

checked_subject_projection!(NoticeRow {
    approval_destination_id: String,
    approval_row_id: String,
});

#[cfg(test)]
type CheckedNoticeRow = NoticeRow<CheckedStorage>;

pub struct ApprovalNoticeCursor(ApprovalDestinationRowId);

pub enum ApprovalNoticeRecovery {
    NoneOwed,
    Page { next: Option<ApprovalNoticeCursor> },
}

impl<Transaction, Sessions> ApprovalApplyingStore<'_, Transaction, Sessions>
where
    Transaction: ApprovalStoreTransaction,
    Sessions: ApprovalSessionFacts<Transaction>,
{
    pub async fn owed_notice_deadline(
        &mut self,
        organization: &OrganizationId,
        session: &ApprovalSession,
        request_id: &ApprovalRequestId,
    ) -> Result<Option<CentralTime>, ApprovalApplyFailure> {
        let at = self.sample_applying_time()?;
        let Some(record) =
            read_parent_approval_request(self.transaction, organization, request_id, session, at)
                .await?
        else {
            return Ok(None);
        };
        let ApprovalState::Waiting { saved } = record.state else {
            return Ok(None);
        };
        let destination = record
            .session_destinations
            .iter()
            .find(|destination| &destination.session == session)
            .ok_or_else(|| corrupt("approval_request_destinations.notice_scope"))?;
        let deadline = saved.window.deadline();
        Ok(
            (destination.first_read_at.is_none() && self.sample_applying_time()? < deadline)
                .then_some(deadline),
        )
    }

    pub async fn recover_machine_notices(
        &mut self,
        machine: &ValidatedMachineConnection,
        after: Option<&ApprovalNoticeCursor>,
    ) -> Result<ApprovalNoticeRecovery, ApprovalApplyFailure> {
        let organization = machine.organization_id();
        if self.transaction.organization_id() != organization {
            return Err(corrupt("approval_request_destinations.notice_scope").into());
        }
        let at = self.sample_applying_time()?;
        let statement = checked_engine_statement!(
            query_file_as,
            [CheckedNoticeRow],
            "src/queries/read_approval_notices.sql",
            organization = organization.as_str().to_owned(),
            machine = machine.machine_id().as_str().to_owned(),
            enrollment = machine.enrollment_id().as_str().to_owned(),
            cursor = after.map(|cursor| cursor.0.as_str().to_owned()),
            limit = SafeInteger::new(NOTICE_PAGE_SIZE)
                .map_err(|_| corrupt("approval_request_destinations.notice_page"))?,
        );
        let rows = self.transaction.read_approval_statement(statement).await?;
        if rows.rows.is_empty() {
            return Ok(ApprovalNoticeRecovery::NoneOwed);
        }
        let mut cursor = None;
        let mut expired = BTreeSet::new();
        for cells in &rows.rows {
            let notice = NoticeRow::read_named(&rows, cells)?;
            let destination =
                ApprovalDestinationRowId::parse(notice.approval_destination_id.stored_text()?)?;
            let row_id = ApprovalRequestRowId::parse(notice.approval_row_id.stored_text()?)?;
            let record = read_approval_by_row_id(self.transaction, &row_id)
                .await?
                .ok_or_else(|| corrupt("approval_request_destinations.notice_request"))?;
            let member = record
                .session_destinations
                .iter()
                .find(|member| member.row_id == destination)
                .filter(|member| {
                    &member.session.enrollment.machine_id == machine.machine_id()
                        && &member.session.enrollment.enrollment_id == machine.enrollment_id()
                })
                .ok_or_else(|| corrupt("approval_request_destinations.notice_scope"))?;
            if let ApprovalState::Waiting { saved } = record.state
                && member.first_read_at.is_none()
            {
                let deadline = saved.window.deadline();
                if at < deadline {
                    self.notice_offers.push(CommittedApprovalNotice::new(
                        organization.clone(),
                        member.session.clone(),
                        saved.key.request_id,
                    ));
                } else if expired.insert(row_id.clone()) {
                    let _ = self
                        .apply_approval_deadline(&InstanceKey(row_id.as_str().to_owned()), deadline)
                        .await?;
                }
            }
            cursor = Some(ApprovalNoticeCursor(destination));
        }
        let page_size = usize::try_from(NOTICE_PAGE_SIZE)
            .map_err(|_| corrupt("approval_request_destinations.notice_page"))?;
        let next = if rows.rows.len() == page_size {
            cursor
        } else {
            None
        };
        Ok(ApprovalNoticeRecovery::Page { next })
    }
}
