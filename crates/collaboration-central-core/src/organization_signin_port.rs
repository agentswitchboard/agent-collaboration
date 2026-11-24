//! The private sign-in link delegates organization work to a scoped runtime.
#[cfg(feature = "test-support")]
use crate::person_approval_application::decide_operation_id;
use crate::{
    PersonApprovalAnswer,
    identity_contract::{AuthorizationRevision, AuthorizationRevisionAcknowledgement},
    organization_admin_contract::OrganizationAdminAnswer,
    organization_authority_cache::StampStanding,
    person_approval_application::PersonApprovalApplication,
    person_request::PersonRequestInput,
};
use central_storage::StoreFailure;
use fleet_contracts::AuthorizationRevisionFailure;
use fleet_contracts::{
    OrganizationStatementDigestReply, ProvisionOrganization, ProvisionOrganizationResult,
    ReadOrganizationStatementDigest,
};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{CentralStore, SyncTime, WorkspaceStores};
use fleet_signin::{ApprovalPersonVerificationInput, person_proof::PersonRequestRefusal};
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
    fn person_approval(
        &self,
        organization: OrganizationId,
        input: ApprovalPersonVerificationInput<'_>,
    ) -> impl Future<Output = Result<PersonApprovalAnswer, StoreFailure>> + Send;

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
    async fn person_approval(
        &self,
        organization: OrganizationId,
        input: ApprovalPersonVerificationInput<'_>,
    ) -> Result<PersonApprovalAnswer, StoreFailure> {
        let answer = self
            .apply_person_approval_request(&organization, input)
            .await;
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
        self.after_request_event().await;
    }
}

impl<H: CentralStore, S: WorkspaceStores> crate::WorkspaceRuntime<H, S> {
    pub(crate) async fn apply_person_approval_request(
        &self,
        organization: &OrganizationId,
        input: ApprovalPersonVerificationInput<'_>,
    ) -> Result<PersonApprovalAnswer, StoreFailure> {
        if organization != self.execution.organization() {
            return Ok(PersonApprovalAnswer::Unauthenticated);
        }
        {
            let _turn = self.execution.take_central_turn().await;
            self.prepare_signin_authority().await?;
            if self.signin_authority.standing(self.execution.clock().now()) != StampStanding::Valid
            {
                return Err(StoreFailure::Unavailable);
            }
        }
        let check = self
            .signin_authority
            .check()
            .ok_or(StoreFailure::Unavailable)?;
        if check.sign_in_service().is_none() {
            return Err(StoreFailure::Unavailable);
        }
        let verified = match check.verify_person_approval(input).await {
            Ok(verified) => verified,
            Err(PersonRequestRefusal::VerificationUnavailable | PersonRequestRefusal::Policy) => {
                return Err(StoreFailure::Unavailable);
            }
            Err(
                PersonRequestRefusal::DpopRequired
                | PersonRequestRefusal::MalformedJwt
                | PersonRequestRefusal::UnsupportedHeader
                | PersonRequestRefusal::IssuerKey
                | PersonRequestRefusal::TokenSignature
                | PersonRequestRefusal::ProofSignature
                | PersonRequestRefusal::PersonClaims
                | PersonRequestRefusal::Issuer
                | PersonRequestRefusal::Audience
                | PersonRequestRefusal::Profile
                | PersonRequestRefusal::ExpiryBounds
                | PersonRequestRefusal::Expired
                | PersonRequestRefusal::ProofKey
                | PersonRequestRefusal::KeyBinding
                | PersonRequestRefusal::Method
                | PersonRequestRefusal::Target
                | PersonRequestRefusal::ProofTime
                | PersonRequestRefusal::RequestTime
                | PersonRequestRefusal::RequestId
                | PersonRequestRefusal::TokenHash
                | PersonRequestRefusal::OperationBody
                | PersonRequestRefusal::Intent
                | PersonRequestRefusal::OrganizationMismatch,
            ) => return Ok(PersonApprovalAnswer::Unauthenticated),
        };
        if verified.organization() != organization {
            return Ok(PersonApprovalAnswer::Unauthenticated);
        }

        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforePersonApprovalTurn)
            .await;
        let turn = self.execution.take_central_turn().await;
        self.prepare_signin_authority().await?;
        let time = SyncTime::from_unix_millis(self.execution.now()?.timestamp_millis());
        if self.signin_authority.standing(time) != StampStanding::Valid {
            return Err(StoreFailure::Unavailable);
        }
        #[cfg(feature = "test-support")]
        let operation_id = decide_operation_id(verified.operation()).cloned();
        let transactions = self
            .person_approval_transactions
            .as_deref()
            .ok_or(StoreFailure::Unavailable)?;
        let schedule = self
            .approval_delivery_completion
            .as_ref()
            .ok_or(StoreFailure::Unavailable)?;
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeSignInSample)
            .await;
        let application = PersonApprovalApplication::new(organization, verified);
        let (proof, applied) = application.commit(transactions).await?;
        #[cfg(feature = "test-support")]
        let reply_was_admitted = applied.is_admitted();
        let publication_failure = applied.publication_failure();
        let (answer, offers) = applied.publish(&proof)?;
        drop(turn);
        if let Some(completion) = transactions.delivery_after_commit(offers) {
            schedule(completion);
        }
        if let Some(reason) = publication_failure {
            return Err(StoreFailure::AlarmUnpublishable { reason });
        }
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterPersonApprovalCommit)
            .await;
        #[cfg(feature = "test-support")]
        if reply_was_admitted
            && operation_id
                .as_ref()
                .is_some_and(|id| self.execution.take_lost_reply(id))
        {
            return Err(StoreFailure::AnswerLost);
        }
        Ok(answer)
    }
}
