//! An organization's injected checks against the sign-in service's keys: its
//! organization stamp, a person's DPoP-bound admin request and machine access tokens.
use crate::person_request::{
    PersonRequestInput, PersonRequestRefusal, PersonRequestVerifier, VerifiedPersonRequest,
};
use fleet_contracts::FleetOrganizationStatementRefusal;
use fleet_runtime_ports::Es256Verification;
use fleet_runtime_ports::{Ed25519Verification, IssuerKeySource};
use fleet_signin::machine_token::{
    MachineTokenRefusal, MachineTokenVerifier, VerifiedMachineToken,
};
use fleet_signin::organization_authority::{
    FleetOrganizationStatementVerifier, StoredStatementFacts, VerifiedFleetOrganizationStatement,
};
use futures_util::future::BoxFuture;
use std::sync::Arc;

/// Hosts build one per organization runtime, bound to that organization, the
/// sign-in issuer and audiences, its published keys and the host's crypto.
pub trait SignInVerification: fleet_signin::StampVerification + 'static {
    fn verify_person_link<'a>(
        &'a self,
        token: &'a str,
    ) -> BoxFuture<'a, Result<fleet_signin::person_proof::CheckedPersonLink, PersonRequestRefusal>>
    {
        let _ = token;
        Box::pin(async { Err(PersonRequestRefusal::Audience) })
    }

    fn verify_person_request<'a>(
        &'a self,
        input: &'a PersonRequestInput,
    ) -> BoxFuture<'a, Result<VerifiedPersonRequest, PersonRequestRefusal>>;

    fn verify_machine_token<'a>(
        &'a self,
        token: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedMachineToken, MachineTokenRefusal>> {
        let _ = token;
        Box::pin(async { Err(MachineTokenRefusal::Unavailable) })
    }

    /// Origin recorded for writes made by the sign-in authority for a person.
    /// Admin calls require this origin to be configured.
    fn sign_in_service(&self) -> Option<message_board::ServiceId>;

    #[cfg(feature = "test-support")]
    fn fail_next_issuer_cache(&self, _operation: fleet_signin::IssuerCacheOperation) -> bool {
        false
    }
}

/// The stamp, person and machine verifiers share the configured issuer keys and host crypto.
pub struct SignInVerifiers<Ed, Es, StatementKeys, PersonKeys> {
    pub statements: FleetOrganizationStatementVerifier<Ed, StatementKeys>,
    pub persons: PersonRequestVerifier<Ed, Es, PersonKeys>,
    pub machines: MachineTokenVerifier<Ed, PersonKeys>,
    pub sign_in_service: Option<message_board::ServiceId>,
    person_link_audience: Option<String>,
    #[cfg(feature = "test-support")]
    issuer_cache: Arc<fleet_signin::IssuerKeyCache>,
}

impl<Ed, Es, StatementKeys, PersonKeys> SignInVerifiers<Ed, Es, StatementKeys, PersonKeys> {
    /// Build the verifier set while Core owns its feature-dependent test hooks.
    #[must_use]
    pub fn from_host_verifiers(
        statements: FleetOrganizationStatementVerifier<Ed, StatementKeys>,
        persons: PersonRequestVerifier<Ed, Es, PersonKeys>,
        machines: MachineTokenVerifier<Ed, PersonKeys>,
        sign_in_service: Option<message_board::ServiceId>,
        issuer_cache: &Arc<fleet_signin::IssuerKeyCache>,
        person_link_audience: Option<String>,
    ) -> Self {
        #[cfg(not(feature = "test-support"))]
        let _ = issuer_cache;
        Self {
            statements,
            persons,
            machines,
            sign_in_service,
            person_link_audience,
            #[cfg(feature = "test-support")]
            issuer_cache: Arc::clone(issuer_cache),
        }
    }
}

impl<Ed, Es, StatementKeys, PersonKeys> fleet_signin::StampVerification
    for SignInVerifiers<Ed, Es, StatementKeys, PersonKeys>
where
    Ed: Ed25519Verification,
    Es: Es256Verification,
    StatementKeys: IssuerKeySource + 'static,
    PersonKeys: IssuerKeySource + 'static,
{
    fn verify_statement<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedFleetOrganizationStatement, FleetOrganizationStatementRefusal>>
    {
        Box::pin(async {
            self.statements
                .verify(statement)
                .await
                .map_err(FleetOrganizationStatementRefusal::from)
        })
    }

    fn verify_stored_statement_facts<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<StoredStatementFacts, fleet_signin::StoredStampVerificationFailure>>
    {
        Box::pin(async {
            self.statements
                .verify_stored_statement_facts(statement)
                .await
                .map_err(fleet_signin::StoredStampVerificationFailure::Verification)
        })
    }
}

impl<Ed, Es, StatementKeys, PersonKeys> SignInVerification
    for SignInVerifiers<Ed, Es, StatementKeys, PersonKeys>
where
    Ed: Ed25519Verification,
    Es: Es256Verification,
    StatementKeys: IssuerKeySource + 'static,
    PersonKeys: IssuerKeySource + 'static,
{
    fn verify_person_link<'a>(
        &'a self,
        token: &'a str,
    ) -> BoxFuture<'a, Result<fleet_signin::person_proof::CheckedPersonLink, PersonRequestRefusal>>
    {
        Box::pin(async move {
            let audience = self
                .person_link_audience
                .as_deref()
                .ok_or(PersonRequestRefusal::Audience)?;
            self.persons.verify_person_link(token, audience).await
        })
    }

    fn verify_person_request<'a>(
        &'a self,
        input: &'a PersonRequestInput,
    ) -> BoxFuture<'a, Result<VerifiedPersonRequest, PersonRequestRefusal>> {
        Box::pin(self.persons.verify(input))
    }

    fn verify_machine_token<'a>(
        &'a self,
        token: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedMachineToken, MachineTokenRefusal>> {
        Box::pin(self.machines.verify(token))
    }

    #[cfg(feature = "test-support")]
    fn fail_next_issuer_cache(&self, operation: fleet_signin::IssuerCacheOperation) -> bool {
        self.issuer_cache.fail_next(operation);
        true
    }

    fn sign_in_service(&self) -> Option<message_board::ServiceId> {
        self.sign_in_service.clone()
    }
}
