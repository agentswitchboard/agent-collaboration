//! Facts decoded from one organization's owning store.
use central_storage::StoreFailure;
use fleet_contracts::ClientId;
use fleet_contracts::{
    AllowedWorkspaces, DeviceLoginRequestId, EndedMachineClient, EnrollmentRevocationReason,
    PersonId,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_signin::organization_change::{MachineAllowanceChange, OrganizationChangeRecord};
use message_board::{MachineLabel, WorkspaceId};
use std::collections::BTreeMap;
use uuid::Uuid;
use workspace_ids::MachineId;

/// A generation is an exact cross-host SQL integer; exhaustion never wraps.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EnrollmentGeneration(u64);
impl TryFrom<u64> for EnrollmentGeneration {
    type Error = StoreFailure;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > 9_007_199_254_740_991 {
            return Err(corrupt("login_machines.enrollment_generation"));
        }
        Ok(Self(value))
    }
}
impl EnrollmentGeneration {
    pub const fn get(self) -> u64 {
        self.0
    }
    pub fn next(self) -> Result<Self, StoreFailure> {
        Self::try_from(
            self.0
                .checked_add(1)
                .ok_or_else(|| corrupt("login_machines.enrollment_generation"))?,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MachineState {
    Enrolled,
    Revoked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MachineRow {
    pub machine_id: MachineId,
    pub label: MachineLabel,
    pub generation: EnrollmentGeneration,
    pub state: MachineState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredEnrollmentState {
    Approved,
    Lapsed,
    Enrolled {
        client_id: ClientId,
        last_grant_at: UnixSeconds,
    },
    Revoked {
        client_id: ClientId,
        reason: EnrollmentRevocationReason,
    },
    Expired {
        client_id: ClientId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEnrollment {
    pub id: EnrollmentId,
    pub machine_id: MachineId,
    pub request_id: DeviceLoginRequestId,
    pub approved_by: PersonId,
    pub label: MachineLabel,
    pub allowed_workspaces: AllowedWorkspaces,
    pub base_generation: EnrollmentGeneration,
    pub approved_at_ms: i64,
    pub code_expires_at: UnixSeconds,
    pub state: StoredEnrollmentState,
    pub ended_at_ms: Option<i64>,
}
impl StoredEnrollment {
    pub fn ended_client(&self) -> Option<EndedMachineClient> {
        let client_id = match &self.state {
            StoredEnrollmentState::Enrolled { client_id, .. }
            | StoredEnrollmentState::Revoked { client_id, .. }
            | StoredEnrollmentState::Expired { client_id } => client_id.clone(),
            StoredEnrollmentState::Approved | StoredEnrollmentState::Lapsed => return None,
        };
        Some(EndedMachineClient {
            enrollment_id: self.id.clone(),
            client_id,
        })
    }
}

/// A checked machine/enrollment pair and the only basis for live authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentEnrollment {
    machine: MachineRow,
    enrollment: StoredEnrollment,
}

impl CurrentEnrollment {
    pub(crate) fn checked(
        machine: Option<&MachineRow>,
        enrollment: Option<StoredEnrollment>,
    ) -> Result<Option<Self>, StoreFailure> {
        validate_current_binding(
            machine.map(AuthorityMachine::from).as_ref(),
            enrollment.as_ref().map(AuthorityApproval::from).as_ref(),
        )?;
        Ok(machine.zip(enrollment).map(|(machine, enrollment)| Self {
            machine: machine.clone(),
            enrollment,
        }))
    }

    pub fn machine(&self) -> &MachineRow {
        &self.machine
    }

    pub fn enrollment(&self) -> &StoredEnrollment {
        &self.enrollment
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthorityMachine {
    pub(crate) machine_id: MachineId,
    pub(crate) generation: EnrollmentGeneration,
    pub(crate) state: MachineState,
}

impl From<&MachineRow> for AuthorityMachine {
    fn from(row: &MachineRow) -> Self {
        Self {
            machine_id: row.machine_id.clone(),
            generation: row.generation,
            state: row.state,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthorityApproval {
    pub(crate) id: EnrollmentId,
    pub(crate) machine_id: MachineId,
    pub(crate) base_generation: EnrollmentGeneration,
    pub(crate) state: StoredEnrollmentState,
    pub(crate) ended_at_ms: Option<i64>,
}

impl From<&StoredEnrollment> for AuthorityApproval {
    fn from(row: &StoredEnrollment) -> Self {
        Self {
            id: row.id.clone(),
            machine_id: row.machine_id.clone(),
            base_generation: row.base_generation,
            state: row.state.clone(),
            ended_at_ms: row.ended_at_ms,
        }
    }
}

fn validate_current_binding(
    machine: Option<&AuthorityMachine>,
    active: Option<&AuthorityApproval>,
) -> Result<(), StoreFailure> {
    match (machine, active) {
        (Some(machine), Some(enrollment))
            if machine.state == MachineState::Enrolled
                && machine.machine_id == enrollment.machine_id
                && matches!(enrollment.state, StoredEnrollmentState::Enrolled { .. })
                && machine.generation == enrollment.base_generation.next()? =>
        {
            Ok(())
        }
        (
            Some(AuthorityMachine {
                state: MachineState::Revoked,
                ..
            })
            | None,
            None,
        ) => Ok(()),
        _ => Err(corrupt("login_machine_approvals.current_enrollment")),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CurrentAuthority(AuthorityApproval);

impl CurrentAuthority {
    pub(crate) fn enrollment(&self) -> &AuthorityApproval {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EnrollmentAuthority {
    machine: Option<AuthorityMachine>,
    enrollment: Option<AuthorityApproval>,
    current: Option<CurrentAuthority>,
}

impl EnrollmentAuthority {
    #[cfg(test)]
    pub(crate) fn checked(
        machine: Option<MachineRow>,
        enrollment: Option<StoredEnrollment>,
        active: Option<StoredEnrollment>,
    ) -> Result<Self, StoreFailure> {
        Self::from_projection(
            machine.as_ref().map(AuthorityMachine::from),
            enrollment.as_ref().map(AuthorityApproval::from),
            active.as_ref().map(AuthorityApproval::from),
        )
    }

    pub(crate) fn from_projection(
        machine: Option<AuthorityMachine>,
        enrollment: Option<AuthorityApproval>,
        active: Option<AuthorityApproval>,
    ) -> Result<Self, StoreFailure> {
        validate_current_binding(machine.as_ref(), active.as_ref())?;
        if let Some(candidate) = &enrollment
            && matches!(candidate.state, StoredEnrollmentState::Enrolled { .. })
            && active.as_ref() != Some(candidate)
        {
            return Err(corrupt("login_machine_approvals.current_enrollment"));
        }
        Ok(Self {
            machine,
            enrollment,
            current: active.map(CurrentAuthority),
        })
    }

    pub(crate) fn machine(&self) -> Option<&AuthorityMachine> {
        self.machine.as_ref()
    }
    pub(crate) fn enrollment(&self) -> Option<&AuthorityApproval> {
        self.enrollment.as_ref()
    }
    pub(crate) fn current(&self) -> Option<&CurrentAuthority> {
        self.current.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoredAllowanceState {
    Active,
    Withdrawn,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredAllowance {
    pub(crate) id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) state: StoredAllowanceState,
}

#[derive(Clone, Debug, Default)]
pub(crate) enum AllowanceInputs {
    #[default]
    Unused,
    Selected(BTreeMap<WorkspaceId, Option<StoredAllowance>>),
    Ending(BTreeMap<WorkspaceId, StoredAllowance>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllowanceEffect {
    Insert,
    Reactivate(Uuid),
    Withdraw(Uuid),
}

#[derive(Clone, Debug, Default)]
pub struct EnrollmentQuery {
    pub machine_id: Option<MachineId>,
    pub request: Option<DeviceLoginRequestId>,
    pub enrollment: Option<EnrollmentId>,
    pub workspaces: Vec<WorkspaceId>,
}

#[derive(Clone, Debug)]
pub struct EnrollmentFacts {
    pub organization_id: OrganizationId,
    pub organization_has_enrollment: bool,
    pub prior_approval: Option<StoredEnrollment>,
    pub enrollment: Option<StoredEnrollment>,
    pub machine: Option<MachineRow>,
    pub current: Option<CurrentEnrollment>,
    pub superseded_client: Option<EndedMachineClient>,
    pub missing_workspaces: Vec<WorkspaceId>,
    pub(crate) allowances: AllowanceInputs,
}

impl EnrollmentFacts {
    pub(crate) fn checked(
        organization_id: OrganizationId,
        machine: Option<MachineRow>,
        enrollment: Option<StoredEnrollment>,
        active: Option<StoredEnrollment>,
    ) -> Result<Self, StoreFailure> {
        let current = CurrentEnrollment::checked(machine.as_ref(), active)?;
        if let Some(candidate) = &enrollment
            && matches!(candidate.state, StoredEnrollmentState::Enrolled { .. })
            && current.as_ref().map(CurrentEnrollment::enrollment) != Some(candidate)
        {
            return Err(corrupt("login_machine_approvals.current_enrollment"));
        }
        Ok(Self {
            organization_id,
            organization_has_enrollment: false,
            prior_approval: None,
            enrollment,
            machine,
            current,
            superseded_client: None,
            missing_workspaces: Vec::new(),
            allowances: AllowanceInputs::Unused,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnrollmentChange {
    InsertEnrollment(StoredEnrollment),
    ReplaceEnrollment(StoredEnrollment),
    PutMachine(MachineRow),
    Record(OrganizationChangeRecord),
    RecordAllowance {
        record: MachineAllowanceChange,
        effect: AllowanceEffect,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Decided<T> {
    pub answer: T,
    pub changes: Vec<EnrollmentChange>,
    pub written_at_ms: Option<i64>,
}
impl<T> Decided<T> {
    pub fn unchanged(answer: T) -> Self {
        Self {
            answer,
            changes: Vec::new(),
            written_at_ms: None,
        }
    }
    pub fn writing(answer: T, changes: Vec<EnrollmentChange>, now_ms: i64) -> Self {
        Self {
            answer,
            changes,
            written_at_ms: Some(now_ms),
        }
    }
}
