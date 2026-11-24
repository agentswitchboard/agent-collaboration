//! Admission and retention are decided before any approval payload is read.
use approval_request_protocol::{ApprovalRequestId, ErasedApprovalMarker};
use central_engine_model::EngineTime;
use central_storage::{ReadOutcome, SqlValue, StoreFailure};
#[cfg(test)]
use fleet_engine::owner_sql::CheckedStorage;
use fleet_engine::{checked_engine_statement, checked_subject_projection};
use fleet_runtime_ports::{SyncTime, stored_values::json, workspace_decode_error::corrupt};

use crate::{
    ApprovalApplyingTime, ApprovalHolderScope, ApprovalRequestRowId, ApprovalStoreTransaction,
    approval_retention::{cutoff_at, expired_ending_bound},
    approval_store_scope::decode_holder_scope,
    approval_stored_tags::parse_stored_time,
};

checked_subject_projection!(StoredApprovalHeader {
    approval_row_id: String,
    organization_id: String,
    machine_id: String,
    enrollment_id: String,
    router_run_id: String,
    request_id: String,
    record_state: String,
    ended_at: Option<String>,
});

checked_subject_projection!(StoredCompactEnding { compact_ending_json: Option<String> });
#[cfg(test)]
type CheckedCompactEnding = StoredCompactEnding<CheckedStorage>;

pub(crate) struct ApprovalLookup {
    row_id: ApprovalRequestRowId,
    scope: ApprovalHolderScope,
    request_id: ApprovalRequestId,
    cutoff: Option<EngineTime>,
}

pub(crate) struct BeforeApprovalCutoff(ApprovalLookup);
/// Unreported delivery has become receipt unknown. Only the compact marker can be read.
pub(crate) struct PastApprovalCutoff(ApprovalLookup);

pub(crate) enum ApprovalVisibility {
    BeforeCutoff(BeforeApprovalCutoff),
    PastCutoff(PastApprovalCutoff),
}

impl ApprovalLookup {
    pub(crate) fn from_row(result: &ReadOutcome, cells: &[SqlValue]) -> Result<Self, StoreFailure> {
        let stored = StoredApprovalHeader::read_named(result, cells)?;
        let scope = decode_scope(&stored)?;
        Self::from_scoped_header(stored, scope)
    }

    pub(crate) fn for_holder(
        result: &ReadOutcome,
        cells: &[SqlValue],
        holder: &ApprovalHolderScope,
    ) -> Result<Option<Self>, StoreFailure> {
        let stored = StoredApprovalHeader::read_named(result, cells)?;
        let scope = decode_scope(&stored)?;
        if &scope != holder {
            return Ok(None);
        }
        Self::from_scoped_header(stored, scope).map(Some)
    }

    fn from_scoped_header(
        stored: StoredApprovalHeader,
        scope: ApprovalHolderScope,
    ) -> Result<Self, StoreFailure> {
        let request_id = stored
            .request_id
            .stored_text()?
            .parse()
            .map_err(|_| corrupt("approval_requests.request_id"))?;
        let row_id = ApprovalRequestRowId::parse(stored.approval_row_id.stored_text()?)?;
        let cutoff = match stored.record_state.stored_text()? {
            "ended" => {
                let ending = parse_stored_time(stored.ended_at.stored_text()?, "ended_at")?;
                Some(cutoff_at(ending)?)
            }
            "waiting" | "stopped_before_raise"
                if matches!(stored.ended_at.raw(), SqlValue::Null) =>
            {
                None
            }
            "waiting" | "stopped_before_raise" => {
                return Err(corrupt("approval_requests.unended_fields"));
            }
            _ => return Err(corrupt("approval_requests.record_state")),
        };
        Ok(Self {
            row_id,
            scope,
            request_id,
            cutoff,
        })
    }

    pub(crate) async fn observe(
        self,
        transaction: &mut impl ApprovalStoreTransaction,
        now: EngineTime,
    ) -> Result<ApprovalVisibility, StoreFailure> {
        if let Some(cutoff) = self.cutoff
            && now >= cutoff
        {
            // Persist the minimum fact required to prevent a rollback from restoring visibility.
            ApprovalApplyingTime::read(
                transaction,
                SyncTime::from_unix_millis(cutoff.timestamp_millis()),
            )
            .await?
            .persist(transaction)
            .await?;
            return Ok(ApprovalVisibility::PastCutoff(PastApprovalCutoff(self)));
        }
        Ok(ApprovalVisibility::BeforeCutoff(BeforeApprovalCutoff(self)))
    }
}

impl BeforeApprovalCutoff {
    pub(crate) fn identity(self) -> (ApprovalRequestRowId, ApprovalHolderScope, ApprovalRequestId) {
        (self.0.row_id, self.0.scope, self.0.request_id)
    }
}

impl PastApprovalCutoff {
    pub(crate) async fn marker(
        self,
        transaction: &mut impl ApprovalStoreTransaction,
    ) -> Result<ErasedApprovalMarker, StoreFailure> {
        let result = transaction
            .read_approval_statement(checked_engine_statement!(
                query_file_as,
                [CheckedCompactEnding],
                "src/queries/read_compact_approval_ending.sql",
                row_id = self.0.row_id.as_str().to_owned(),
            ))
            .await?;
        let [cells] = result.rows.as_slice() else {
            return Err(corrupt("approval_requests.compact_ending_json"));
        };
        let stored = StoredCompactEnding::read_named(&result, cells)?;
        Ok(ErasedApprovalMarker {
            request_id: self.0.request_id,
            router_run: self.0.scope.run.router_run,
            ending: json(
                stored.compact_ending_json.stored_text()?,
                "approval_requests.compact_ending_json",
            )?,
        })
    }
}

fn decode_scope(stored: &StoredApprovalHeader) -> Result<ApprovalHolderScope, StoreFailure> {
    decode_holder_scope(
        stored.organization_id.stored_text()?,
        stored.machine_id.stored_text()?,
        stored.enrollment_id.stored_text()?,
        stored.router_run_id.stored_text()?,
    )
}

pub(crate) async fn observe_owed_cutoff(
    transaction: &mut impl ApprovalStoreTransaction,
    scope: &ApprovalHolderScope,
    at: EngineTime,
) -> Result<(), StoreFailure> {
    let observation = checked_engine_statement!(
        query_file,
        [],
        "src/queries/read_latest_owed_cutoff.sql",
        organization = scope.organization_id.as_str().to_owned(),
        machine = scope.run.enrollment.machine_id.as_str().to_owned(),
        enrollment = scope.run.enrollment.enrollment_id.as_str().to_owned(),
        run = scope.run.router_run.as_str().to_owned(),
        ending_bound = expired_ending_bound(at)?,
    );
    let result = transaction.read_approval_statement(observation).await?;
    match result.rows.as_slice() {
        [] => {}
        [row] => {
            if matches!(
                ApprovalLookup::from_row(&result, row)?
                    .observe(transaction, at)
                    .await?,
                ApprovalVisibility::BeforeCutoff(_)
            ) {
                return Err(corrupt("approval_requests.owed_cutoff"));
            }
        }
        _ => return Err(corrupt("approval_requests.owed_cutoff")),
    }
    Ok(())
}
