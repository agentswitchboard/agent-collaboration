use crate::{ClientId, mail_link::MachineLinkCloseCode};
use central_storage::StoreFailure;
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use serde::{Deserialize, Serialize};
use std::future::Future;
use workspace_ids::{MachineId, WorkspaceId};

/// Hibernation provenance is untrusted until the runtime restores it against live rows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MachineConnectionAttachment {
    organization_id: OrganizationId,
    machine_id: MachineId,
    enrollment_id: EnrollmentId,
    client_id: ClientId,
    effective_expires_at: UnixSeconds,
}

impl MachineConnectionAttachment {
    /// Builds untrusted provenance from the token claims.
    pub fn from_token_claims(
        organization_id: OrganizationId,
        machine_id: MachineId,
        enrollment_id: EnrollmentId,
        client_id: ClientId,
        effective_expires_at: UnixSeconds,
    ) -> Self {
        Self {
            organization_id,
            machine_id,
            enrollment_id,
            client_id,
            effective_expires_at,
        }
    }

    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization_id
    }
    #[must_use]
    pub fn machine_id(&self) -> &MachineId {
        &self.machine_id
    }
    #[must_use]
    pub fn enrollment_id(&self) -> &EnrollmentId {
        &self.enrollment_id
    }

    #[must_use]
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }
    #[must_use]
    pub fn effective_expires_at(&self) -> UnixSeconds {
        self.effective_expires_at
    }
}

/// A committed ending identifies only the enrollment whose live sockets must end.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentInvalidation {
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
    pub cause: MachineLinkCloseCode,
}

/// Restores the attachment against live login rows under the turn held by the caller.
pub trait LiveMachineConnection: Send + Sync {
    fn allows_workspace_held(
        &self,
        machine: &MachineId,
        workspace: &WorkspaceId,
    ) -> impl Future<Output = Result<bool, StoreFailure>> + Send;

    fn restore_held(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> impl Future<Output = Result<Result<(), MachineLinkCloseCode>, StoreFailure>> + Send;
}
