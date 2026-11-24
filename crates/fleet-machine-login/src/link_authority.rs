//! Classify an authenticated attachment against its owning enrollment and deadline.
use crate::enrollment_row_values::identifier;
use crate::policy::{EnrollmentAuthority, StoredEnrollmentState};
use central_storage::StoreFailure;
use fleet_contracts::mail_link::MachineLinkCloseCode;
use fleet_contracts::{
    ClientId, EnrollmentInvalidation, EnrollmentRevocationReason, MachineConnectionAttachment,
    MachineConnectionRefusal,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::MachineId;

/// Core-created connection provenance. It carries no cached workspace permissions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedMachineConnection {
    attachment: MachineConnectionAttachment,
}
impl ValidatedMachineConnection {
    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        self.attachment.organization_id()
    }
    #[must_use]
    pub fn machine_id(&self) -> &MachineId {
        self.attachment.machine_id()
    }
    #[must_use]
    pub fn enrollment_id(&self) -> &EnrollmentId {
        self.attachment.enrollment_id()
    }
    #[must_use]
    pub fn client_id(&self) -> &ClientId {
        self.attachment.client_id()
    }
    #[must_use]
    pub fn effective_expires_at(&self) -> UnixSeconds {
        self.attachment.effective_expires_at()
    }
    #[must_use]
    pub fn attachment(&self) -> MachineConnectionAttachment {
        self.attachment.clone()
    }

    pub fn attachment_ref(&self) -> &MachineConnectionAttachment {
        &self.attachment
    }
}

pub type Admission =
    Result<Result<ValidatedMachineConnection, MachineConnectionRefusal>, StoreFailure>;

pub struct LiveAuthorityRefusal {
    pub reason: MachineConnectionRefusal,
    pub close: MachineLinkCloseCode,
    pub(crate) ending: Option<EnrollmentInvalidation>,
}

pub type LiveAdmission =
    Result<Result<ValidatedMachineConnection, LiveAuthorityRefusal>, StoreFailure>;

pub fn refuse_authority(
    reason: MachineConnectionRefusal,
    close: MachineLinkCloseCode,
) -> LiveAdmission {
    Ok(Err(LiveAuthorityRefusal {
        reason,
        close,
        ending: None,
    }))
}

pub fn validate_shape(attachment: &MachineConnectionAttachment) -> Result<(), StoreFailure> {
    let _: OrganizationId = identifier(
        attachment.organization_id().as_str(),
        "connection.organization_id",
    )?;
    let _: EnrollmentId = identifier(
        attachment.enrollment_id().as_str(),
        "connection.enrollment_id",
    )?;
    let _: ClientId = identifier(attachment.client_id().as_str(), "connection.client_id")?;
    if attachment.effective_expires_at().0 < 0 {
        return Err(corrupt("connection.effective_expires_at"));
    }
    attachment
        .effective_expires_at()
        .0
        .checked_mul(1000)
        .and_then(|millis| central_storage::SafeInteger::new(millis).ok())
        .ok_or_else(|| corrupt("connection.effective_expires_at"))?;
    Ok(())
}

pub(crate) fn classify_link(
    attachment: &MachineConnectionAttachment,
    organization: &OrganizationId,
    authority: &EnrollmentAuthority,
    now_ms: i64,
) -> LiveAdmission {
    use MachineLinkCloseCode::{Expired, Inactive, Revoked};

    if attachment.organization_id() != organization {
        return refuse_authority(
            MachineConnectionRefusal::MachineInAnotherOrganization {},
            Inactive,
        );
    }
    let Some(machine) = authority.machine() else {
        return refuse_authority(MachineConnectionRefusal::UnknownMachine {}, Inactive);
    };
    let Some(enrollment) = authority.enrollment() else {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Inactive);
    };
    if &machine.machine_id != attachment.machine_id()
        || &enrollment.machine_id != attachment.machine_id()
        || &enrollment.id != attachment.enrollment_id()
    {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Inactive);
    }
    if attachment.client_id().as_str() != attachment.enrollment_id().as_str() {
        return refuse_authority(MachineConnectionRefusal::WrongClient {}, Inactive);
    }
    let ending = match &enrollment.state {
        StoredEnrollmentState::Enrolled { client_id, .. }
            if client_id == attachment.client_id() =>
        {
            None
        }
        StoredEnrollmentState::Enrolled { .. } => {
            return refuse_authority(MachineConnectionRefusal::WrongClient {}, Inactive);
        }
        StoredEnrollmentState::Revoked { client_id, reason }
            if client_id == attachment.client_id() =>
        {
            Some(match reason {
                EnrollmentRevocationReason::Owner {} => Revoked,
                EnrollmentRevocationReason::Superseded {} => Inactive,
            })
        }
        StoredEnrollmentState::Expired { client_id } if client_id == attachment.client_id() => {
            Some(Inactive)
        }
        StoredEnrollmentState::Approved
        | StoredEnrollmentState::Lapsed
        | StoredEnrollmentState::Revoked { .. }
        | StoredEnrollmentState::Expired { .. } => {
            return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Inactive);
        }
    };
    if let Some(cause) = ending {
        let reason = match &enrollment.state {
            StoredEnrollmentState::Revoked {
                reason: EnrollmentRevocationReason::Superseded {},
                ..
            } => MachineConnectionRefusal::Superseded {},
            _ => MachineConnectionRefusal::EnrollmentNotActive {},
        };
        return Ok(Err(LiveAuthorityRefusal {
            reason,
            close: cause,
            ending: Some(EnrollmentInvalidation {
                machine_id: machine.machine_id.clone(),
                enrollment_id: enrollment.id.clone(),
                cause,
            }),
        }));
    }
    if authority.current().map(|current| &current.enrollment().id) != Some(&enrollment.id) {
        return Err(corrupt("login_machine_approvals.enrolled"));
    }
    if now_ms.div_euclid(1000) >= attachment.effective_expires_at().0 {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Expired);
    }
    Ok(Ok(ValidatedMachineConnection {
        attachment: attachment.clone(),
    }))
}

/// Recheck in the caller's write transaction after sampling the applying time.
pub async fn recheck_lent_link_authority<T: fleet_lent_transaction::CentralTransaction>(
    tx: &mut T,
    attachment: &MachineConnectionAttachment,
    clock: &dyn fleet_runtime_ports::Clock,
) -> Result<(fleet_runtime_ports::SyncTime, Option<MachineLinkCloseCode>), StoreFailure>
where
    <T::Database as sqlx_core::database::Database>::Arguments:
        sqlx_core::arguments::IntoArguments<T::Database>,
    String: sqlx_core::types::Type<T::Database>
        + for<'q> sqlx_core::encode::Encode<'q, T::Database>
        + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    Option<String>:
        sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    i64: sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    for<'r> &'r str:
        sqlx_core::column::ColumnIndex<<T::Database as sqlx_core::database::Database>::Row>,
{
    recheck_lent_link_authority_after_read(tx, attachment, clock, |_| {}).await
}

pub(crate) async fn recheck_lent_link_authority_after_read<
    T: fleet_lent_transaction::CentralTransaction,
>(
    tx: &mut T,
    attachment: &MachineConnectionAttachment,
    clock: &dyn fleet_runtime_ports::Clock,
    after_read: impl FnOnce(&mut T),
) -> Result<(fleet_runtime_ports::SyncTime, Option<MachineLinkCloseCode>), StoreFailure>
where
    <T::Database as sqlx_core::database::Database>::Arguments:
        sqlx_core::arguments::IntoArguments<T::Database>,
    String: sqlx_core::types::Type<T::Database>
        + for<'q> sqlx_core::encode::Encode<'q, T::Database>
        + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    Option<String>:
        sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    i64: sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    for<'r> &'r str:
        sqlx_core::column::ColumnIndex<<T::Database as sqlx_core::database::Database>::Row>,
{
    validate_shape(attachment)?;
    let authority = crate::enrollment_reads::read_lent_authority(
        tx,
        attachment.machine_id(),
        attachment.enrollment_id(),
    )
    .await?;
    after_read(tx);
    let applying_time = clock.now();
    let cause = classify_link(
        attachment,
        attachment.organization_id(),
        &authority,
        applying_time.unix_millis(),
    )?
    .err()
    .map(|refused| refused.close);
    Ok((applying_time, cause))
}

/// Restores a fixture attachment against real enrollment rows on its lending store.
#[cfg(feature = "test-support")]
pub async fn restore_fixture_link<T: fleet_lent_transaction::CentralTransaction>(
    tx: &mut T,
    attachment: &MachineConnectionAttachment,
    organization: &OrganizationId,
    now_ms: i64,
) -> LiveAdmission
where
    <T::Database as sqlx_core::database::Database>::Arguments:
        sqlx_core::arguments::IntoArguments<T::Database>,
    String: sqlx_core::types::Type<T::Database>
        + for<'q> sqlx_core::encode::Encode<'q, T::Database>
        + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    Option<String>:
        sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    i64: sqlx_core::types::Type<T::Database> + for<'r> sqlx_core::decode::Decode<'r, T::Database>,
    for<'r> &'r str:
        sqlx_core::column::ColumnIndex<<T::Database as sqlx_core::database::Database>::Row>,
{
    validate_shape(attachment)?;
    let authority = crate::enrollment_reads::read_lent_authority(
        tx,
        attachment.machine_id(),
        attachment.enrollment_id(),
    )
    .await?;
    classify_link(attachment, organization, &authority, now_ms)
}

#[cfg(test)]
#[path = "link_authority_test.rs"]
mod tests;
