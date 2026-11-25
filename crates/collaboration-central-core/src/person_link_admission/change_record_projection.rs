//! Project the organization's stored change records onto the person link.

use crate::WorkspaceRuntime;
use central_link_protocol::person_link_rpc::{
    AuditChangeKind, AuditChangeOrigin, AuditRecord, MachineAllowanceState,
    OrganizationReadRefusal, ReadOrganizationChangeRecords, ReadOrganizationChangeRecordsResult,
    TargetedAuditRecord,
};
use central_person_link::PersonSocketAttachment;
use central_storage::StoreFailure;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_signin::{
    FleetPermission,
    organization_change::{
        self, MachineAllowanceState as StoredAllowanceState, OrganizationChangeKind,
        OrganizationChangeOrigin, StoredOrganizationChange,
    },
};

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub(super) async fn read_change_records_on_person_link(
        &self,
        connection: &PersonSocketAttachment,
        request: ReadOrganizationChangeRecords,
    ) -> Result<ReadOrganizationChangeRecordsResult, StoreFailure> {
        use ReadOrganizationChangeRecordsResult::{Records, Refused};
        if !connection.permits(FleetPermission::ReadOrganizationAudit) {
            return Ok(Refused {
                reason: OrganizationReadRefusal::NotPermitted {},
            });
        }
        let limit = u16::from(request.limit);
        // Read one extra row to distinguish a full terminal page from one with a successor.
        let mut records = organization_change::read_page(
            self.execution.statement_host(),
            request.after,
            limit + 1,
        )
        .await?;
        let next = if records.len() > usize::from(limit) {
            records.truncate(usize::from(limit));
            records.last().map(|record| match record {
                StoredOrganizationChange::TargetedChange(change) => change.id,
                StoredOrganizationChange::MachineAllowanceChanged(change) => change.id,
            })
        } else {
            None
        };
        Ok(Records {
            records: records.into_iter().map(project_record).collect(),
            next,
        })
    }
}

fn project_record(record: StoredOrganizationChange) -> AuditRecord {
    match record {
        StoredOrganizationChange::TargetedChange(change) => {
            AuditRecord::TargetedChange(TargetedAuditRecord {
                id: change.id,
                request_id: change.request_id.into(),
                kind: match change.kind {
                    OrganizationChangeKind::OrganizationProvisioned => {
                        AuditChangeKind::OrganizationProvisioned
                    }
                    OrganizationChangeKind::StampRenewed => AuditChangeKind::StampRenewed,
                    OrganizationChangeKind::WorkspaceCreated => AuditChangeKind::WorkspaceCreated,
                    OrganizationChangeKind::MachineEnrolled => AuditChangeKind::MachineEnrolled,
                    OrganizationChangeKind::MachineRevoked => AuditChangeKind::MachineRevoked,
                    OrganizationChangeKind::MachineExpired => AuditChangeKind::MachineExpired,
                },
                origin: project_origin(change.origin),
                target_id: change.target_id,
                created_at_millis: change.created_at_millis,
            })
        }
        StoredOrganizationChange::MachineAllowanceChanged(change) => {
            AuditRecord::MachineAllowanceChanged {
                id: change.id,
                request_id: change.request_id.into(),
                origin: project_origin(change.origin),
                machine_id: change.machine_id,
                workspace_id: change.workspace_id,
                state: match change.state {
                    StoredAllowanceState::Active => MachineAllowanceState::Active,
                    StoredAllowanceState::Withdrawn => MachineAllowanceState::Withdrawn,
                },
                created_at_millis: change.created_at_millis,
            }
        }
    }
}

fn project_origin(origin: OrganizationChangeOrigin) -> AuditChangeOrigin {
    match origin {
        OrganizationChangeOrigin::SignInProvisioning {} => AuditChangeOrigin::SignInProvisioning {},
        OrganizationChangeOrigin::Automatic {} => AuditChangeOrigin::Automatic {},
        OrganizationChangeOrigin::Person {
            person_id,
            permission_checked,
        } => AuditChangeOrigin::Person {
            person_id,
            permission_checked,
        },
    }
}
