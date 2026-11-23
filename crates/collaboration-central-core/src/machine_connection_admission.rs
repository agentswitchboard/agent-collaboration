//! The organization turn owns read-only connection admission and restoration.
use crate::WorkspaceRuntime;
use central_link_protocol::{MachineLinkCloseCode, TokenRenewalRefusal};
use central_storage::StoreFailure;
use fleet_contracts::{
    EnrollmentInvalidation, LiveMachineConnection, MachineConnectionAttachment,
    MachineConnectionRefusal,
};
use fleet_machine_login::{
    MachineLoginPlan,
    link_authority::{
        Admission, LiveAdmission, ValidatedMachineConnection, refuse_authority, validate_shape,
    },
    link_transitions::{
        MachineTokenRenewalRefusal, PreparedLinkAdmission, PreparedLinkRenewal,
        RenewedMachineConnection,
    },
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_ids::{MachineId, WorkspaceId};

/// Live authority for one ordinary link request or one member of a write batch.
pub enum MachineLinkRequestAuthority {
    Admitted(ValidatedMachineConnection),
    Refused(MachineLinkCloseCode),
    TemporarilyUnavailable,
}

pub enum MachineLinkAdmission {
    Admitted(ValidatedMachineConnection),
    Refused(MachineConnectionRefusal),
    Ended(EnrollmentInvalidation),
}

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub async fn restore_machine_link_request(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> Result<MachineLinkRequestAuthority, StoreFailure> {
        match self.restore_live_authority(attachment).await {
            Ok(Ok(connection)) => Ok(MachineLinkRequestAuthority::Admitted(connection)),
            Ok(Err(refused)) => Ok(MachineLinkRequestAuthority::Refused(refused.close)),
            Err(StoreFailure::Unavailable | StoreFailure::AnswerLost) => {
                Ok(MachineLinkRequestAuthority::TemporarilyUnavailable)
            }
            Err(
                error @ (StoreFailure::Corrupt { .. } | StoreFailure::AlarmUnpublishable { .. }),
            ) => Err(error),
        }
    }

    /// Restore the current attachment while the caller retains the organization's turn.
    pub async fn restore_machine_in_held_turn(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> Result<Result<ValidatedMachineConnection, MachineLinkCloseCode>, StoreFailure> {
        Ok(self
            .restore_live_authority_held(attachment)
            .await?
            .map_err(|refusal| refusal.close))
    }

    pub async fn admit_machine_connection(&self, token: &str) -> Admission {
        Ok(match self.admit_machine_link(token).await? {
            MachineLinkAdmission::Admitted(connection) => Ok(connection),
            MachineLinkAdmission::Refused(reason) => Err(reason),
            MachineLinkAdmission::Ended(_) => Err(MachineConnectionRefusal::EnrollmentNotActive {}),
        })
    }

    pub async fn admit_machine_link(
        &self,
        token: &str,
    ) -> Result<MachineLinkAdmission, StoreFailure> {
        let check = self
            .signin_authority
            .check()
            .ok_or(StoreFailure::Unavailable)?;
        let verified = match check.verify_machine_token(token).await {
            Ok(verified) => verified,
            Err(fleet_signin::machine_token::MachineTokenRefusal::Rejected) => {
                return Ok(MachineLinkAdmission::Refused(
                    MachineConnectionRefusal::EnrollmentNotActive {},
                ));
            }
            Err(fleet_signin::machine_token::MachineTokenRefusal::Unavailable) => {
                return Err(StoreFailure::Unavailable);
            }
        };
        let _turn = self.execution.take_central_turn_for_admission().await;
        if verified.organization_id() != self.execution.organization() {
            return Ok(MachineLinkAdmission::Refused(
                MachineConnectionRefusal::MachineInAnotherOrganization {},
            ));
        }
        let time = self.machine_login_time(verified.organization_id()).await?;
        match self
            .machine_login_service()
            .prepare_link_admission(&verified, time)
            .await?
        {
            PreparedLinkAdmission::Admitted(connection) => {
                Ok(MachineLinkAdmission::Admitted(connection))
            }
            PreparedLinkAdmission::Refused(reason) => Ok(MachineLinkAdmission::Refused(reason)),
            PreparedLinkAdmission::Ended(ending) => Ok(MachineLinkAdmission::Ended(ending)),
            PreparedLinkAdmission::EndIdle(plan) => self
                .commit_idle_expiry(plan)
                .await
                .map(MachineLinkAdmission::Ended),
        }
    }

    /// Rechecks both the attached authority and the verified candidate under the organization turn.
    pub async fn renew_machine_connection(
        &self,
        current: &MachineConnectionAttachment,
        token: &str,
    ) -> Renewal {
        use MachineTokenRenewalRefusal as Refusal;
        if let Err(refused) = self.restore_live_authority(current).await? {
            return Ok(Err(Refusal::CurrentAuthorityEnded(refused.close)));
        }
        let check = self
            .signin_authority
            .check()
            .ok_or(StoreFailure::Unavailable)?;
        let verified = match check.verify_machine_token(token).await {
            Ok(verified) => verified,
            Err(fleet_signin::machine_token::MachineTokenRefusal::Rejected) => {
                return Ok(Err(Refusal::TokenRejected));
            }
            Err(fleet_signin::machine_token::MachineTokenRefusal::Unavailable) => {
                return Err(StoreFailure::Unavailable);
            }
        };
        let _turn = self.execution.take_central_turn_for_admission().await;
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeRenewalApplyingSample)
            .await;
        let time = self.machine_login_time(current.organization_id()).await?;
        Ok(
            match self
                .machine_login_service()
                .prepare_link_renewal(current, &verified, time)
                .await?
            {
                PreparedLinkRenewal::Renewed(renewed) => Ok(renewed),
                PreparedLinkRenewal::Refused(refusal) => Err(refusal),
                PreparedLinkRenewal::EndIdle(plan) => {
                    Err(Refusal::IdleEnded(self.commit_idle_expiry(plan).await?))
                }
            },
        )
    }

    async fn commit_idle_expiry(
        &self,
        plan: MachineLoginPlan<()>,
    ) -> Result<EnrollmentInvalidation, StoreFailure> {
        let (_, invalidation) = self.apply_machine_login(plan).await?;
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterIdleExpiryCommit)
            .await;
        invalidation.ok_or(StoreFailure::Unavailable)
    }

    pub async fn restore_machine_connection(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> Admission {
        self.restore_live_authority(attachment)
            .await
            .map(|decision| decision.map_err(|refusal| refusal.reason))
    }

    async fn restore_live_authority(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> LiveAdmission {
        let _turn = self.execution.take_central_turn_for_admission().await;
        #[cfg(feature = "test-support")]
        if let Some(failure) = self.take_machine_restore_failure() {
            return Err(failure);
        }
        self.restore_live_authority_held(attachment).await
    }

    /// Fail one restore after the specified number of successful fixture restores.
    #[cfg(feature = "test-support")]
    pub fn fail_machine_restore_after(&self, skip: usize, failure: StoreFailure) {
        *self
            .machine_restore_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((skip, failure));
    }

    #[cfg(feature = "test-support")]
    fn take_machine_restore_failure(&self) -> Option<StoreFailure> {
        let mut next = self
            .machine_restore_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match next.as_mut() {
            Some((remaining, _)) if *remaining > 0 => {
                *remaining -= 1;
                None
            }
            _ => next.take().map(|(_, failure)| failure),
        }
    }

    /// Register socket ownership while the live authority check still holds
    /// the organization turn. Revocation then either sees the registration or
    /// wins the turn first and refuses it.
    pub async fn register_active_machine<Value>(
        &self,
        attachment: &MachineConnectionAttachment,
        register: impl FnOnce() -> Value,
    ) -> Result<Result<Value, MachineLinkCloseCode>, StoreFailure> {
        let _turn = self.execution.take_central_turn_for_admission().await;
        Ok(self
            .restore_live_authority_held(attachment)
            .await?
            .map(|_| register())
            .map_err(|refusal| refusal.close))
    }

    pub(crate) async fn restore_live_authority_held(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> LiveAdmission {
        if attachment.organization_id() != self.execution.organization() {
            return refuse_authority(
                MachineConnectionRefusal::MachineInAnotherOrganization {},
                MachineLinkCloseCode::Inactive,
            );
        }
        validate_shape(attachment)?;
        let time = self
            .machine_login_time(attachment.organization_id())
            .await?;
        self.machine_login_service()
            .restore_link(attachment, time)
            .await
    }
}

type Renewal = Result<Result<RenewedMachineConnection, MachineTokenRenewalRefusal>, StoreFailure>;

impl<Host: CentralStore, Stores: WorkspaceStores> LiveMachineConnection
    for WorkspaceRuntime<Host, Stores>
{
    async fn allows_workspace_held(
        &self,
        machine: &MachineId,
        workspace: &WorkspaceId,
    ) -> Result<bool, StoreFailure> {
        self.machine_login_service()
            .allows_workspace(machine, workspace)
            .await
    }

    async fn restore_held(
        &self,
        attachment: &MachineConnectionAttachment,
    ) -> Result<Result<(), MachineLinkCloseCode>, StoreFailure> {
        self.restore_live_authority_held(attachment)
            .await
            .map(|admission| admission.map(|_| ()).map_err(|refusal| refusal.close))
    }
}

pub enum RenewalTermination {
    CurrentAuthority(MachineLinkCloseCode),
    EndEnrollment(EnrollmentInvalidation),
}

impl RenewalTermination {
    pub fn cause(&self) -> MachineLinkCloseCode {
        match self {
            Self::CurrentAuthority(cause) => *cause,
            Self::EndEnrollment(invalidation) => invalidation.cause,
        }
    }
}

pub fn project_renewal_refusal(
    refusal: MachineTokenRenewalRefusal,
) -> (TokenRenewalRefusal, Option<RenewalTermination>) {
    use MachineTokenRenewalRefusal as Reason;
    match refusal {
        Reason::TokenRejected => (TokenRenewalRefusal::TokenRejected, None),
        Reason::DifferentMachine => (TokenRenewalRefusal::DifferentMachine, None),
        Reason::DifferentEnrollment => (TokenRenewalRefusal::DifferentEnrollment, None),
        Reason::PresentedTokenExpired => (TokenRenewalRefusal::PresentedTokenExpired, None),
        Reason::CurrentAuthorityEnded(cause) => (
            match cause {
                MachineLinkCloseCode::Expired => TokenRenewalRefusal::CentralUnavailable,
                MachineLinkCloseCode::Inactive | MachineLinkCloseCode::Revoked => {
                    TokenRenewalRefusal::ConnectionEnrollmentEnded
                }
            },
            Some(RenewalTermination::CurrentAuthority(cause)),
        ),
        Reason::IdleEnded(invalidation) => (
            TokenRenewalRefusal::ConnectionEnrollmentEnded,
            Some(RenewalTermination::EndEnrollment(invalidation)),
        ),
    }
}

#[cfg(test)]
#[path = "machine_connection_admission_test.rs"]
mod tests;
