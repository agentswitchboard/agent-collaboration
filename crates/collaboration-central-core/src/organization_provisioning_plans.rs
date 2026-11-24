//! Core composes the sign-in owner with its existing revision route.
use crate::{
    WorkspaceRuntime,
    identity_contract::{AuthorizationRevision, AuthorizationRevisionAcknowledgement},
};
use central_storage::{BatchOutcome, StatementBatch, StoreFailure};
use fleet_contracts::{
    AuthorizationRevisionFailure, DigestFailure, OrganizationStatementDigestReply,
    ProvisionFailure, ProvisionOrganization, ProvisionOrganizationResult,
    ReadOrganizationStatementDigest,
};
use fleet_runtime_ports::{CentralStore, SyncTime, WorkspaceStores};
use fleet_signin::{SignInService, StampStanding, StampVerification};
use std::sync::Arc;

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    fn sign_in_service(&self) -> SignInService<'_, H, S> {
        SignInService::for_owning_runtime(
            &self.execution,
            self.stamp_store.as_ref(),
            &self.signin_authority.cache,
            self.signin_authority
                .check()
                .map(|check| -> Arc<dyn StampVerification> { check }),
        )
    }

    pub(crate) async fn prepare_signin_authority(&self) -> Result<(), StoreFailure> {
        self.sign_in_service().prepare_standing().await
    }

    pub async fn provision_organization(
        &self,
        request: &ProvisionOrganization,
    ) -> Result<ProvisionOrganizationResult, ProvisionFailure> {
        self.sign_in_service().provision_organization(request).await
    }

    pub async fn read_statement_digest(
        &self,
        request: &ReadOrganizationStatementDigest,
    ) -> Result<OrganizationStatementDigestReply, DigestFailure> {
        self.sign_in_service().read_statement_digest(request).await
    }

    pub async fn deliver_authorization_revision(
        &self,
        revision: &AuthorizationRevision,
    ) -> Result<AuthorizationRevisionAcknowledgement, AuthorizationRevisionFailure> {
        use AuthorizationRevisionFailure as Failure;
        if revision.organization_id() != self.execution.organization() {
            return Err(Failure::OrganizationMismatch);
        }
        let _turn = self.execution.take_central_turn().await;
        self.prepare_signin_authority().await?;
        let now = self.execution.now()?;
        if self
            .signin_authority
            .standing(SyncTime::from_unix_millis(now.timestamp_millis()))
            != StampStanding::Valid
        {
            return Err(Failure::Unavailable);
        }
        let plan = crate::authorization_revision_plans::prepare_authorization_revision(
            self.execution.statement_host(),
            revision,
            now,
        )
        .await?;
        let pushed_person = match revision {
            AuthorizationRevision::Person { person_id, .. } => person_id.as_str().parse().ok(),
            AuthorizationRevision::Organization { .. } => None,
        };
        let applied = self.apply_revision_plan(plan).await?;
        let revision_number = applied.acknowledged_revision.get();
        let event = match revision {
            AuthorizationRevision::Organization {
                organization_id, ..
            } => Some(central_person_link::PersonRevision::Organization {
                organization: organization_id.clone(),
                revision: revision_number,
            }),
            AuthorizationRevision::Person {
                organization_id, ..
            } => pushed_person.map(|person| central_person_link::PersonRevision::Person {
                organization: organization_id.clone(),
                person,
                revision: revision_number,
            }),
        };
        if let Some(event) = event {
            (self.person_revision_committed)(event);
        }
        Ok(applied)
    }
}
impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    async fn apply_revision_plan(
        &self,
        plan: crate::authorization_revision_plans::AuthorizationRevisionPlan,
    ) -> Result<AuthorizationRevisionAcknowledgement, AuthorizationRevisionFailure> {
        use crate::person_request::floor_delivery::FloorDelivery;
        use AuthorizationRevisionFailure as Failure;
        let revision = match plan.delivery {
            FloorDelivery::Acknowledged(revision) => revision,
            FloorDelivery::Conflict => return Err(Failure::Conflict),
            FloorDelivery::OrganizationFloorMissing => {
                return Err(Failure::OrganizationFloorMissing);
            }
        };
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
            .await;
        match self
            .execution
            .statement_host()
            .run_batch(StatementBatch {
                statements: plan.statements,
                alarm: central_storage::AlarmUpdate::Unchanged,
            })
            .await?
        {
            BatchOutcome::Applied { .. } => Ok(AuthorizationRevisionAcknowledgement {
                acknowledged_revision: revision,
            }),
            _ => Err(Failure::Unavailable),
        }
    }
}
