//! Prepare connection transitions from checked token provenance and the owning store.
use crate::enrollment_reads;
use crate::link_authority::{
    LiveAdmission, ValidatedMachineConnection, classify_link, refuse_authority, validate_shape,
};
use crate::policy::DecisionTime;
use crate::{MachineLoginPlan, MachineLoginService};
use central_storage::StoreFailure;
use fleet_contracts::{
    EnrollmentInvalidation, MachineConnectionAttachment, MachineConnectionRefusal,
    mail_link::MachineLinkCloseCode,
};
use fleet_runtime_ports::CentralStore;
use fleet_signin::machine_token::VerifiedMachineToken;

pub enum PreparedLinkAdmission {
    Admitted(ValidatedMachineConnection),
    Refused(MachineConnectionRefusal),
    Ended(EnrollmentInvalidation),
    EndIdle(MachineLoginPlan<()>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MachineTokenRenewalRefusal {
    TokenRejected,
    DifferentMachine,
    DifferentEnrollment,
    PresentedTokenExpired,
    CurrentAuthorityEnded(MachineLinkCloseCode),
    IdleEnded(EnrollmentInvalidation),
}

pub struct RenewedMachineConnection {
    pub connection: ValidatedMachineConnection,
    pub central_time: chrono::DateTime<chrono::Utc>,
}

pub enum PreparedLinkRenewal {
    Renewed(RenewedMachineConnection),
    Refused(MachineTokenRenewalRefusal),
    EndIdle(MachineLoginPlan<()>),
}

impl<Host: CentralStore> MachineLoginService<'_, Host> {
    pub async fn prepare_link_admission(
        &self,
        token: &VerifiedMachineToken,
        time: DecisionTime,
    ) -> Result<PreparedLinkAdmission, StoreFailure> {
        let attachment = attachment_from_verified(token);
        validate_shape(&attachment)?;
        let authority = enrollment_reads::read_authority(
            self.store,
            attachment.machine_id(),
            attachment.enrollment_id(),
        )
        .await?;
        match classify_link(&attachment, self.organization, &authority, time.now_ms)? {
            Ok(connection) => {
                let expired = self.prepare_link_idle(authority, time).await?;
                Ok(match expired {
                    Some(plan) => PreparedLinkAdmission::EndIdle(plan),
                    None => PreparedLinkAdmission::Admitted(connection),
                })
            }
            Err(refused) => Ok(match refused.ending {
                Some(ending) => PreparedLinkAdmission::Ended(ending),
                None => PreparedLinkAdmission::Refused(refused.reason),
            }),
        }
    }

    pub async fn restore_link(
        &self,
        attachment: &MachineConnectionAttachment,
        time: DecisionTime,
    ) -> LiveAdmission {
        validate_shape(attachment)?;
        if attachment.organization_id() != self.organization {
            return refuse_authority(
                MachineConnectionRefusal::MachineInAnotherOrganization {},
                MachineLinkCloseCode::Inactive,
            );
        }
        let authority = enrollment_reads::read_authority(
            self.store,
            attachment.machine_id(),
            attachment.enrollment_id(),
        )
        .await?;
        classify_link(attachment, self.organization, &authority, time.now_ms)
    }

    pub async fn prepare_link_renewal(
        &self,
        current: &MachineConnectionAttachment,
        token: &VerifiedMachineToken,
        time: DecisionTime,
    ) -> Result<PreparedLinkRenewal, StoreFailure> {
        use MachineTokenRenewalRefusal as Refusal;
        let authority = enrollment_reads::read_authority(
            self.store,
            current.machine_id(),
            current.enrollment_id(),
        )
        .await?;
        if let Err(refused) = classify_link(current, self.organization, &authority, time.now_ms)? {
            return Ok(PreparedLinkRenewal::Refused(
                Refusal::CurrentAuthorityEnded(refused.close),
            ));
        }
        if token.organization_id() != current.organization_id()
            || token.machine_id() != current.machine_id()
        {
            return Ok(PreparedLinkRenewal::Refused(Refusal::DifferentMachine));
        }
        if token.enrollment_id() != current.enrollment_id()
            || token.client_id() != current.client_id()
        {
            return Ok(PreparedLinkRenewal::Refused(Refusal::DifferentEnrollment));
        }
        let candidate = attachment_from_verified(token);
        validate_shape(&candidate)?;
        let renewed = match classify_link(&candidate, self.organization, &authority, time.now_ms)? {
            Ok(connection) => connection,
            Err(refused) => {
                return match refused.close {
                    MachineLinkCloseCode::Expired => {
                        Ok(PreparedLinkRenewal::Refused(Refusal::PresentedTokenExpired))
                    }
                    // Both attachments have the same identity and use the same
                    // authority snapshot. Only their deadlines can differ.
                    MachineLinkCloseCode::Inactive | MachineLinkCloseCode::Revoked => {
                        Err(fleet_runtime_ports::workspace_decode_error::corrupt(
                            "connection.renewal_authority",
                        ))
                    }
                };
            }
        };
        if let Some(plan) = self.prepare_link_idle(authority, time).await? {
            return Ok(PreparedLinkRenewal::EndIdle(plan));
        }
        Ok(PreparedLinkRenewal::Renewed(RenewedMachineConnection {
            connection: renewed,
            central_time: chrono::DateTime::from_timestamp_millis(time.now_ms)
                .ok_or(StoreFailure::Unavailable)?,
        }))
    }

    async fn prepare_link_idle(
        &self,
        authority: crate::policy::EnrollmentAuthority,
        time: DecisionTime,
    ) -> Result<Option<MachineLoginPlan<()>>, StoreFailure> {
        let current = authority.current().ok_or(StoreFailure::Unavailable)?;
        let crate::policy::StoredEnrollmentState::Enrolled { last_grant_at, .. } =
            &current.enrollment().state
        else {
            return Err(StoreFailure::Unavailable);
        };
        if !crate::policy::idle_limit_reached(time, *last_grant_at) {
            return Ok(None);
        }
        let facts =
            enrollment_reads::facts_for_ending(self.store, self.organization.clone(), authority)
                .await?;
        self.prepare_idle_expiry(&facts, time)
    }
}

fn attachment_from_verified(token: &VerifiedMachineToken) -> MachineConnectionAttachment {
    MachineConnectionAttachment::from_token_claims(
        token.organization_id().clone(),
        token.machine_id().clone(),
        token.enrollment_id().clone(),
        token.client_id().clone(),
        token.effective_expires_at(),
    )
}
