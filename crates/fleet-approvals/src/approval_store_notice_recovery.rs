//! Recover durable unread session notices for the welcomed machine.
use std::collections::BTreeSet;

use approval_request_protocol::CentralTime;
use central_engine_model::InstanceKey;
use central_storage::{SafeInteger, SqlValue};
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_machine_login::link_authority::ValidatedMachineConnection;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::{ApprovalRequestId, CentralSessionId};

use super::{ApprovalApplyFailure, ApprovalApplyingStore};
use crate::{
    ApprovalEnrollment, ApprovalSession, ApprovalSessionFacts, CommittedApprovalNotice,
    approval_store_scope::{ApprovalDestinationRowId, ApprovalRequestRowId},
    approval_store_transaction::ApprovalStoreTransaction,
};

const NOTICE_PAGE_SIZE: i64 = 100;

checked_subject_projection!(NoticeRow {
    approval_destination_id: String,
    session_id: Option<String>,
    approval_row_id: String,
    request_id: String,
    deadline_ms: Option<i64>,
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
            let request_id: ApprovalRequestId = notice
                .request_id
                .stored_text()?
                .parse()
                .map_err(|_| corrupt("approval_request_destinations.notice_request"))?;
            let session_id: CentralSessionId = notice
                .session_id
                .stored_text()?
                .parse()
                .map_err(|_| corrupt("approval_request_destinations.notice_session"))?;
            let SqlValue::Integer(deadline) = notice.deadline_ms.raw() else {
                return Err(corrupt("approval_requests.notice_deadline").into());
            };
            let deadline = CentralTime::from_timestamp_millis(deadline.get())
                .ok_or_else(|| corrupt("approval_requests.notice_deadline"))?;
            if at >= deadline {
                if expired.insert(row_id.as_str().to_owned()) {
                    let _ = self
                        .apply_approval_deadline(&InstanceKey(row_id.as_str().to_owned()), deadline)
                        .await?;
                }
            } else {
                self.notice_offers.push(CommittedApprovalNotice::new(
                    organization.clone(),
                    ApprovalSession {
                        enrollment: ApprovalEnrollment {
                            machine_id: machine.machine_id().clone(),
                            enrollment_id: machine.enrollment_id().clone(),
                        },
                        session_id,
                    },
                    request_id,
                ));
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
