//! Caller-sampled decision time, fixed policy durations and audit identities.
use central_storage::StoreFailure;
use fleet_identity_protocol::UnixSeconds;
use fleet_runtime_ports::IdSource;
use fleet_signin::organization_change::{
    ChangeRequestId, MACHINE_ALLOWANCE_CHANGE_KIND, MachineAllowanceChange, MachineAllowanceState,
    OrganizationChangeKind, OrganizationChangeOrigin, OrganizationChangeRecord,
};
use uuid::Uuid;

pub(crate) const IDLE_EXPIRY_SECONDS: i64 = 30 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecisionTime {
    pub now_ms: i64,
}
impl DecisionTime {
    pub(crate) const fn seconds(self) -> UnixSeconds {
        UnixSeconds(self.now_ms.div_euclid(1000))
    }
}

pub(super) struct DecisionIds<'a>(pub &'a dyn IdSource);
impl DecisionIds<'_> {
    pub(super) fn next(&self) -> Result<Uuid, StoreFailure> {
        let id = self.0.new_id();
        if !fleet_runtime_ports::uuid_v7(id) {
            return Err(StoreFailure::Unavailable);
        }
        Uuid::parse_str(&id.to_string()).map_err(|_| StoreFailure::Unavailable)
    }
    pub(super) fn record(
        &self,
        originating: &str,
        kind: OrganizationChangeKind,
        origin: &OrganizationChangeOrigin,
        target: String,
        time: DecisionTime,
    ) -> Result<OrganizationChangeRecord, StoreFailure> {
        let request_id =
            ChangeRequestId::try_from(format!("{originating}:{}:{target}", kind.as_str()))?;
        Ok(OrganizationChangeRecord {
            id: self.next()?,
            request_id,
            kind,
            origin: origin.clone(),
            target_id: target,
            created_at_millis: time
                .now_ms
                .try_into()
                .map_err(|_| StoreFailure::Unavailable)?,
        })
    }

    pub(super) fn record_allowance_change(
        &self,
        originating: &str,
        origin: &OrganizationChangeOrigin,
        machine_id: &workspace_ids::MachineId,
        workspace_id: &message_board::WorkspaceId,
        state: MachineAllowanceState,
        time: DecisionTime,
    ) -> Result<MachineAllowanceChange, StoreFailure> {
        let request_id = ChangeRequestId::try_from(format!(
            "{originating}:{MACHINE_ALLOWANCE_CHANGE_KIND}:{}",
            workspace_id.as_str()
        ))?;
        Ok(MachineAllowanceChange {
            id: self.next()?,
            request_id,
            origin: origin.clone(),
            machine_id: machine_id.clone(),
            workspace_id: workspace_id.as_str().parse().map_err(|_| {
                fleet_runtime_ports::workspace_decode_error::corrupt(
                    "signin_change_records.workspace_id",
                )
            })?,
            state,
            created_at_millis: time
                .now_ms
                .try_into()
                .map_err(|_| StoreFailure::Unavailable)?,
        })
    }
}

#[cfg(test)]
#[path = "decision_support_test.rs"]
mod tests;
