//! Verified token provenance becomes a connection only after live organization admission.
use central_link_protocol::MachineLinkCloseCode;
use central_storage::StoreFailure;
use fleet_contracts::{
    ClientId, EnrollmentRevocationReason, MachineConnectionAttachment, MachineConnectionRefusal,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId, UnixSeconds};
use fleet_machine_login::enrollment_row_values::identifier;
use fleet_machine_login::policy::{EnrollmentFacts, MachineState, StoredEnrollmentState};
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

    pub(crate) fn attachment_ref(&self) -> &MachineConnectionAttachment {
        &self.attachment
    }
}

pub(crate) type Admission =
    Result<Result<ValidatedMachineConnection, MachineConnectionRefusal>, StoreFailure>;

pub(crate) struct LiveAuthorityRefusal {
    pub reason: MachineConnectionRefusal,
    pub close: MachineLinkCloseCode,
}

pub(crate) type LiveAdmission =
    Result<Result<ValidatedMachineConnection, LiveAuthorityRefusal>, StoreFailure>;

pub(crate) fn refuse_authority(
    reason: MachineConnectionRefusal,
    close: MachineLinkCloseCode,
) -> LiveAdmission {
    Ok(Err(LiveAuthorityRefusal { reason, close }))
}

pub(crate) fn validate_shape(attachment: &MachineConnectionAttachment) -> Result<(), StoreFailure> {
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

pub(crate) fn validate_live(
    attachment: &MachineConnectionAttachment,
    organization: &OrganizationId,
    facts: &EnrollmentFacts,
    now_ms: i64,
) -> Admission {
    classify_live(attachment, organization, facts, now_ms)
        .map(|decision| decision.map_err(|refusal| refusal.reason))
}

pub(crate) fn classify_live(
    attachment: &MachineConnectionAttachment,
    organization: &OrganizationId,
    facts: &EnrollmentFacts,
    now_ms: i64,
) -> LiveAdmission {
    use MachineLinkCloseCode::{Expired, Inactive, Revoked};

    validate_shape(attachment)?;
    if attachment.organization_id() != organization || &facts.organization_id != organization {
        return refuse_authority(
            MachineConnectionRefusal::MachineInAnotherOrganization {},
            Inactive,
        );
    }
    if now_ms.div_euclid(1000) >= attachment.effective_expires_at().0 {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Expired);
    }
    let Some(machine) = &facts.machine else {
        return refuse_authority(MachineConnectionRefusal::UnknownMachine {}, Inactive);
    };
    let Some(enrollment) = &facts.enrollment else {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Inactive);
    };
    if &machine.machine_id != attachment.machine_id()
        || &enrollment.machine_id != attachment.machine_id()
        || &enrollment.id != attachment.enrollment_id()
    {
        return refuse_authority(MachineConnectionRefusal::EnrollmentNotActive {}, Inactive);
    }
    // Keep the previous refusal for an inactive machine, while distinguishing
    // an explicit owner revocation from an enrollment that ended another way.
    let refused_close = if matches!(
        enrollment.state,
        StoredEnrollmentState::Revoked {
            reason: EnrollmentRevocationReason::Owner {},
            ..
        }
    ) {
        Revoked
    } else {
        Inactive
    };
    if machine.state != MachineState::Enrolled {
        return refuse_authority(
            MachineConnectionRefusal::EnrollmentNotActive {},
            refused_close,
        );
    }
    if facts.active.as_ref().map(|active| &active.id) != Some(&enrollment.id) {
        return refuse_authority(MachineConnectionRefusal::Superseded {}, refused_close);
    }
    if attachment.client_id().as_str() != attachment.enrollment_id().as_str() {
        return refuse_authority(MachineConnectionRefusal::WrongClient {}, Inactive);
    }
    match &enrollment.state {
        StoredEnrollmentState::Enrolled { client_id, .. }
            if client_id == attachment.client_id() => {}
        StoredEnrollmentState::Enrolled { .. } => {
            return refuse_authority(MachineConnectionRefusal::WrongClient {}, Inactive);
        }
        StoredEnrollmentState::Approved
        | StoredEnrollmentState::Lapsed
        | StoredEnrollmentState::Revoked { .. }
        | StoredEnrollmentState::Expired { .. } => {
            return refuse_authority(
                MachineConnectionRefusal::EnrollmentNotActive {},
                refused_close,
            );
        }
    }
    Ok(Ok(ValidatedMachineConnection {
        attachment: attachment.clone(),
    }))
}

#[cfg(test)]
#[path = "machine_connection_test.rs"]
mod tests;
