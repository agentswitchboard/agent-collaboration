//! Core composes the stamp verifier with its signed-person profile.
use crate::organization_authority::SignInVerification;
use fleet_contracts::FleetOrganizationStatementRefusal;
use fleet_runtime_ports::SyncTime;
pub(crate) use fleet_signin::StampStanding;
use fleet_signin::StandingCache;
use fleet_signin::organization_authority::{
    StoredStatementFacts, VerifiedFleetOrganizationStatement,
};
use futures_util::future::BoxFuture;
use std::sync::{Arc, RwLock};

pub(crate) struct OrganizationAuthority {
    check: RwLock<Arc<dyn SignInVerification>>,
    pub(crate) cache: StandingCache,
}

impl Default for OrganizationAuthority {
    fn default() -> Self {
        Self {
            check: RwLock::new(Arc::new(UnconfiguredStatementCheck)),
            cache: StandingCache::default(),
        }
    }
}

impl OrganizationAuthority {
    pub(crate) fn install_check(&self, check: Arc<dyn SignInVerification>) {
        if let Ok(mut current) = self.check.write() {
            *current = check;
        }
        self.cache.forget();
    }

    pub(crate) fn check(&self) -> Option<Arc<dyn SignInVerification>> {
        self.check.read().ok().map(|check| check.clone())
    }

    pub(crate) fn standing(&self, now: SyncTime) -> StampStanding {
        self.cache.standing(now)
    }
}

/// A host that configured no sign-in trust can never accept a stamp.
struct UnconfiguredStatementCheck;
impl fleet_signin::StampVerification for UnconfiguredStatementCheck {
    fn verify_stored_statement_facts<'a>(
        &'a self,
        _statement: &'a str,
    ) -> BoxFuture<'a, Result<StoredStatementFacts, FleetOrganizationStatementRefusal>> {
        Box::pin(std::future::ready(Err(
            FleetOrganizationStatementRefusal::InvalidConfiguration,
        )))
    }

    fn verify_statement<'a>(
        &'a self,
        _statement: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedFleetOrganizationStatement, FleetOrganizationStatementRefusal>>
    {
        Box::pin(std::future::ready(Err(
            FleetOrganizationStatementRefusal::InvalidConfiguration,
        )))
    }
}

impl SignInVerification for UnconfiguredStatementCheck {
    fn verify_person_request<'a>(
        &'a self,
        _input: &'a crate::person_request::PersonRequestInput,
    ) -> BoxFuture<
        'a,
        Result<
            crate::person_request::VerifiedPersonRequest,
            crate::person_request::PersonRequestRefusal,
        >,
    > {
        Box::pin(std::future::ready(Err(
            crate::person_request::PersonRequestRefusal::Policy,
        )))
    }
    fn sign_in_service(&self) -> Option<message_board::ServiceId> {
        None
    }
}
