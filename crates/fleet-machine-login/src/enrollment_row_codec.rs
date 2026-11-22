//! Decode owning machine/login rows into the pure policy's checked facts.
use crate::{
    enrollment_row_values as values,
    policy::{
        AuthorityApproval, AuthorityMachine, EnrollmentGeneration, MachineRow, MachineState,
        StoredAllowance, StoredAllowanceState, StoredEnrollment, StoredEnrollmentState,
    },
};
use central_storage::{ReadOutcome, SqlValue, StoreFailure};
use fleet_contracts::ClientId;
use fleet_contracts::{AllowedWorkspaces, EnrollmentRevocationReason};
use fleet_identity_protocol::EnrollmentId;
use fleet_runtime_ports::{
    stored_values::{json, string},
    workspace_decode_error::corrupt,
};

fn label(value: &str, field: &str) -> Result<message_board::MachineLabel, StoreFailure> {
    let parsed =
        message_board::MachineLabel::try_from(value.to_owned()).map_err(|_| corrupt(field))?;
    if parsed.as_str() != value {
        return Err(corrupt(field));
    }
    Ok(parsed)
}

macro_rules! projected_row {
    ($name:ident { $($field:ident: $kind:tt),+ $(,)? }) => {
        #[derive(sqlx::FromRow)]
        pub(crate) struct $name {
            $( pub(super) $field: projected_row!(@type $kind), )+
        }

        impl $name {
            pub(crate) fn from_store(rows: &ReadOutcome, row: &[SqlValue]) -> Result<Self, StoreFailure> {
                Ok(Self {
                    $( $field: projected_row!(@read rows, row, $field, $kind)?, )+
                })
            }
        }
    };
    (@type Text) => { String };
    (@type OptionalText) => { Option<String> };
    (@type Integer) => { i64 };
    (@read $rows:ident, $row:ident, $field:ident, Text) => {
        string($rows, $row, stringify!($field)).map(str::to_owned)
    };
    (@read $rows:ident, $row:ident, $field:ident, OptionalText) => {
        values::optional_text($rows, $row, stringify!($field)).map(|value| value.map(str::to_owned))
    };
    (@read $rows:ident, $row:ident, $field:ident, Integer) => {
        values::nonnegative_integer($rows, $row, stringify!($field)).map(|value| value.get())
    };
}

projected_row!(StoredMachineRow {
    machine_id: OptionalText,
    label: Text,
    state: Text,
    enrollment_generation: Integer,
    created_at: Text,
    updated_at: Text,
});

projected_row!(StoredApprovalRow {
    approval_id: OptionalText,
    request_id: Text,
    label: Text,
    allowed_workspaces: Text,
    state: Text,
    end_reason: OptionalText,
    approved_by_person_id: Text,
    permission_checked: Text,
    base_generation: Integer,
    code_expires_at: Text,
    machine_client_id: OptionalText,
    last_grant_at: OptionalText,
    ended_at: OptionalText,
    created_at: Text,
    updated_at: Text,
    machine_id: OptionalText,
});

projected_row!(StoredAuthorityMachineRow {
    machine_id: OptionalText,
    state: Text,
    enrollment_generation: Integer,
});

projected_row!(StoredAuthorityApprovalRow {
    approval_id: OptionalText,
    machine_id: OptionalText,
    base_generation: Integer,
    state: Text,
    end_reason: OptionalText,
    machine_client_id: OptionalText,
    last_grant_at: OptionalText,
    ended_at: OptionalText,
});

projected_row!(StoredAllowanceRow {
    allowance_id: Text,
    workspace_id: Text,
    state: Text,
    change_request_id: Text,
    change_origin: Text,
    created_at: Text,
    updated_at: Text,
});

projected_row!(StoredApprovalHistory {
    has_enrollment: Integer
});
projected_row!(StoredWorkspacePresence {
    workspace_id: OptionalText
});

pub fn machine(rows: &ReadOutcome, row: &[SqlValue]) -> Result<MachineRow, StoreFailure> {
    StoredMachineRow::from_store(rows, row)
        .map_err(|error| {
            named_scalar_error(
                error,
                "enrollment_generation",
                "login_machines.enrollment_generation",
            )
        })?
        .decode()
}

pub fn enrollment(rows: &ReadOutcome, row: &[SqlValue]) -> Result<StoredEnrollment, StoreFailure> {
    StoredApprovalRow::from_store(rows, row)
        .map_err(|error| {
            named_scalar_error(
                error,
                "base_generation",
                "login_machine_approvals.base_generation",
            )
        })?
        .decode()
}

fn required(value: Option<String>, field: &str) -> Result<String, StoreFailure> {
    value.ok_or_else(|| corrupt(field))
}

fn generation(value: i64, field: &str) -> Result<EnrollmentGeneration, StoreFailure> {
    let value = u64::try_from(value).map_err(|_| corrupt(field))?;
    EnrollmentGeneration::try_from(value).map_err(|_| corrupt(field))
}

fn named_scalar_error(error: StoreFailure, column_name: &str, name: &str) -> StoreFailure {
    match &error {
        StoreFailure::Corrupt {
            reason: central_storage::CorruptReason::BadValue { column },
        } if column == column_name => corrupt(name),
        _ => error,
    }
}

fn decode_machine_state(state: &str) -> Result<MachineState, StoreFailure> {
    match state {
        "enrolled" => Ok(MachineState::Enrolled),
        "revoked" => Ok(MachineState::Revoked),
        _ => Err(corrupt("login_machines.state")),
    }
}

impl StoredAuthorityMachineRow {
    pub(crate) fn decode(self) -> Result<AuthorityMachine, StoreFailure> {
        Ok(AuthorityMachine {
            machine_id: required(self.machine_id, "login_machines.machine_id")?
                .parse()
                .map_err(|_| corrupt("login_machines.machine_id"))?,
            generation: generation(
                self.enrollment_generation,
                "login_machines.enrollment_generation",
            )?,
            state: decode_machine_state(&self.state)?,
        })
    }
}

impl StoredAuthorityApprovalRow {
    pub(crate) fn decode(self) -> Result<AuthorityApproval, StoreFailure> {
        let id = decode_approval_id(self.approval_id)?;
        let (state, ended_at_ms) = decode_approval_lifecycle(
            &id,
            &self.state,
            self.end_reason.as_deref(),
            self.machine_client_id.as_deref(),
            self.last_grant_at.as_deref(),
            self.ended_at.as_deref(),
        )?;
        Ok(AuthorityApproval {
            id,
            machine_id: required(self.machine_id, "login_machine_approvals.machine_id")?
                .parse()
                .map_err(|_| corrupt("login_machine_approvals.machine_id"))?,
            base_generation: generation(
                self.base_generation,
                "login_machine_approvals.base_generation",
            )?,
            state,
            ended_at_ms,
        })
    }
}

impl StoredMachineRow {
    pub(crate) fn decode(self) -> Result<MachineRow, StoreFailure> {
        values::millis(&self.created_at, "login_machines.created_at")?;
        values::millis(&self.updated_at, "login_machines.updated_at")?;
        Ok(MachineRow {
            machine_id: required(self.machine_id, "login_machines.machine_id")?
                .parse()
                .map_err(|_| corrupt("login_machines.machine_id"))?,
            label: label(&self.label, "login_machines.label")?,
            generation: generation(
                self.enrollment_generation,
                "login_machines.enrollment_generation",
            )?,
            state: decode_machine_state(&self.state)?,
        })
    }
}

impl StoredApprovalRow {
    pub(crate) fn decode(self) -> Result<StoredEnrollment, StoreFailure> {
        let approved_at_ms =
            values::millis(&self.created_at, "login_machine_approvals.created_at")?;
        values::millis(&self.updated_at, "login_machine_approvals.updated_at")?;
        let id = decode_approval_id(self.approval_id)?;
        let (state, ended_at_ms) = decode_approval_lifecycle(
            &id,
            &self.state,
            self.end_reason.as_deref(),
            self.machine_client_id.as_deref(),
            self.last_grant_at.as_deref(),
            self.ended_at.as_deref(),
        )?;
        if self.permission_checked != "machine:enroll" {
            return Err(corrupt("login_machine_approvals.permission_checked"));
        }
        let allowed_workspaces: AllowedWorkspaces = json(
            &self.allowed_workspaces,
            "login_machine_approvals.allowed_workspaces",
        )?;
        Ok(StoredEnrollment {
            id,
            machine_id: required(self.machine_id, "login_machine_approvals.machine_id")?
                .parse()
                .map_err(|_| corrupt("login_machine_approvals.machine_id"))?,
            request_id: values::identifier(&self.request_id, "login_machine_approvals.request_id")?,
            approved_by: values::identifier(
                &self.approved_by_person_id,
                "login_machine_approvals.approved_by_person_id",
            )?,
            label: label(&self.label, "login_machine_approvals.label")?,
            allowed_workspaces,
            base_generation: generation(
                self.base_generation,
                "login_machine_approvals.base_generation",
            )?,
            approved_at_ms,
            code_expires_at: values::seconds(
                &self.code_expires_at,
                "login_machine_approvals.code_expires_at",
            )?,
            state,
            ended_at_ms,
        })
    }
}

fn decode_approval_id(value: Option<String>) -> Result<EnrollmentId, StoreFailure> {
    let field = "login_machine_approvals.approval_id";
    values::identifier(&required(value, field)?, field)
}

fn decode_approval_lifecycle(
    id: &EnrollmentId,
    stored_state: &str,
    reason: Option<&str>,
    client: Option<&str>,
    last_grant_at: Option<&str>,
    ended_at: Option<&str>,
) -> Result<(StoredEnrollmentState, Option<i64>), StoreFailure> {
    let client: Option<ClientId> = client
        .map(|value| values::identifier(value, "login_machine_approvals.machine_client_id"))
        .transpose()?;
    let last_grant = last_grant_at
        .map(|value| values::seconds(value, "login_machine_approvals.last_grant_at"))
        .transpose()?;
    let ended_at_ms = ended_at
        .map(|value| values::millis(value, "login_machine_approvals.ended_at"))
        .transpose()?;
    if client
        .as_ref()
        .is_some_and(|client_id| client_id.as_str() != id.as_str())
    {
        return Err(corrupt("login_machine_approvals.machine_client_id"));
    }
    let state = match (stored_state, reason, client, last_grant, ended_at_ms) {
        ("approved", None, None, None, None) => StoredEnrollmentState::Approved,
        ("enrolled", None, Some(client_id), Some(last_grant_at), None) => {
            StoredEnrollmentState::Enrolled {
                client_id,
                last_grant_at,
            }
        }
        ("ended", Some("lapsed"), None, None, Some(_)) => StoredEnrollmentState::Lapsed,
        ("ended", Some("owner"), Some(client_id), None, Some(_)) => {
            StoredEnrollmentState::Revoked {
                client_id,
                reason: EnrollmentRevocationReason::Owner {},
            }
        }
        ("ended", Some("superseded"), Some(client_id), None, Some(_)) => {
            StoredEnrollmentState::Revoked {
                client_id,
                reason: EnrollmentRevocationReason::Superseded {},
            }
        }
        ("ended", Some("idle"), Some(client_id), None, Some(_)) => {
            StoredEnrollmentState::Expired { client_id }
        }
        _ => return Err(corrupt("login_machine_approvals.state")),
    };
    Ok((state, ended_at_ms))
}

impl StoredAllowanceRow {
    pub(crate) fn decode(self) -> Result<StoredAllowance, StoreFailure> {
        let id = values::private_id(&self.allowance_id, "login_machine_allowances.allowance_id")?;
        let workspace_id =
            values::identifier(&self.workspace_id, "login_machine_allowances.workspace_id")?;
        let _: fleet_signin::organization_change::ChangeRequestId =
            self.change_request_id.try_into()?;
        if !matches!(self.change_origin.as_str(), "person" | "automatic") {
            return Err(corrupt("login_machine_allowances.change_origin"));
        }
        let created_at = values::millis(&self.created_at, "login_machine_allowances.created_at")?;
        let updated_at = values::millis(&self.updated_at, "login_machine_allowances.updated_at")?;
        if updated_at < created_at {
            return Err(corrupt("login_machine_allowances.updated_at"));
        }
        let state = match self.state.as_str() {
            "active" => StoredAllowanceState::Active,
            "withdrawn" => StoredAllowanceState::Withdrawn,
            _ => return Err(corrupt("login_machine_allowances.state")),
        };
        Ok(StoredAllowance {
            id,
            workspace_id,
            state,
        })
    }
}

impl StoredApprovalHistory {
    pub(crate) fn decode(self) -> Result<bool, StoreFailure> {
        match self.has_enrollment {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("login_machine_approvals.has_enrollment")),
        }
    }
}

impl StoredWorkspacePresence {
    pub(crate) fn decode(self) -> Result<message_board::WorkspaceId, StoreFailure> {
        let field = "directory_workspaces.workspace_id";
        values::identifier(&required(self.workspace_id, field)?, field)
    }
}

pub fn only_row<T>(
    rows: &ReadOutcome,
    decode: impl FnOnce(&ReadOutcome, &[SqlValue]) -> Result<T, StoreFailure>,
    field: &str,
) -> Result<Option<T>, StoreFailure> {
    match rows.rows.as_slice() {
        [] => Ok(None),
        [row] => decode(rows, row).map(Some),
        _ => Err(corrupt(field)),
    }
}

#[cfg(test)]
#[path = "enrollment_row_codec_test.rs"]
mod tests;
