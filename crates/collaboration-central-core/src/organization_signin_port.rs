//! The private sign-in link delegates organization work to a scoped runtime.
use crate::{
    identity_contract::{AuthorizationRevision, AuthorizationRevisionAcknowledgement},
    organization_admin_contract::OrganizationAdminAnswer,
    person_request::PersonRequestInput,
};
use central_storage::StoreFailure;
use fleet_contracts::AuthorizationRevisionFailure;
use fleet_contracts::{
    OrganizationStatementDigestReply, ProvisionOrganization, ProvisionOrganizationResult,
    ReadOrganizationStatementDigest,
};
use fleet_identity_protocol::OrganizationId;
use std::future::Future;

/// Hosts supply routing to the named organization's runtime; this port does
/// not grant trust or bypass that runtime's admission and commit boundary.
pub trait OrganizationSignInPort: Send + Sync + 'static {
    fn provision(
        &self,
        request: &ProvisionOrganization,
    ) -> impl Future<Output = Result<ProvisionOrganizationResult, fleet_contracts::ProvisionFailure>>
    + Send;

    fn digest(
        &self,
        request: &ReadOrganizationStatementDigest,
    ) -> impl Future<
        Output = Result<OrganizationStatementDigestReply, fleet_contracts::DigestFailure>,
    > + Send;

    fn revision(
        &self,
        request: &AuthorizationRevision,
    ) -> impl Future<
        Output = Result<AuthorizationRevisionAcknowledgement, AuthorizationRevisionFailure>,
    > + Send;

    fn admin(
        &self,
        organization: OrganizationId,
        input: PersonRequestInput,
    ) -> impl Future<Output = Result<OrganizationAdminAnswer, StoreFailure>> + Send;

    fn device_login(
        &self,
        request: &fleet_contracts::DecideDeviceLogin,
    ) -> impl Future<Output = Result<fleet_contracts::DeviceLoginDecision, StoreFailure>> + Send;
    fn machine_token(
        &self,
        request: &fleet_contracts::DecideMachineToken,
    ) -> impl Future<Output = Result<fleet_contracts::MachineTokenDecision, StoreFailure>> + Send;
}
impl<H: fleet_runtime_ports::CentralStore, S: fleet_runtime_ports::WorkspaceStores>
    OrganizationSignInPort for crate::WorkspaceRuntime<H, S>
{
    async fn provision(
        &self,
        request: &ProvisionOrganization,
    ) -> Result<ProvisionOrganizationResult, fleet_contracts::ProvisionFailure> {
        let answer = self.provision_organization(request).await;
        self.after_signin_event().await;
        answer
    }
    async fn digest(
        &self,
        request: &ReadOrganizationStatementDigest,
    ) -> Result<OrganizationStatementDigestReply, fleet_contracts::DigestFailure> {
        let answer = self.read_statement_digest(request).await;
        self.after_signin_event().await;
        answer
    }
    async fn revision(
        &self,
        request: &AuthorizationRevision,
    ) -> Result<AuthorizationRevisionAcknowledgement, AuthorizationRevisionFailure> {
        let answer = self.deliver_authorization_revision(request).await;
        self.after_signin_event().await;
        answer
    }
    async fn admin(
        &self,
        organization: OrganizationId,
        input: PersonRequestInput,
    ) -> Result<OrganizationAdminAnswer, StoreFailure> {
        if &organization != self.execution.organization() {
            return Ok(OrganizationAdminAnswer::Unauthenticated);
        }
        let answer = self.admin_person_request(&input).await;
        self.after_signin_event().await;
        answer
    }

    async fn device_login(
        &self,
        request: &fleet_contracts::DecideDeviceLogin,
    ) -> Result<fleet_contracts::DeviceLoginDecision, StoreFailure> {
        let answer = self.decide_device_login(request).await;
        self.after_signin_event().await;
        answer
    }
    async fn machine_token(
        &self,
        request: &fleet_contracts::DecideMachineToken,
    ) -> Result<fleet_contracts::MachineTokenDecision, StoreFailure> {
        let answer = self.decide_machine_token(request).await;
        self.after_signin_event().await;
        answer
    }
}

impl<H: fleet_runtime_ports::CentralStore, S: fleet_runtime_ports::WorkspaceStores>
    crate::WorkspaceRuntime<H, S>
{
    /// Every sign-in entry is an event (P6): after its own work, and after its
    /// central turn is released, it runs the due batch and re-arms. An alarm
    /// problem never replaces the entry's answer; it is observed.
    async fn after_signin_event(&self) {
        let _ = self.after_event().await;
    }
}
