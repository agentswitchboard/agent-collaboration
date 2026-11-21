//! One applying turn owns a verified stamp and its guarded effects.
use crate::{
    organization_authority::{
        OfferStamp, StampEffect, StampMachine, StampRefusal, StoredStampRead, StoredStatementFacts,
        VerifiedFleetOrganizationStatement,
    },
    organization_change::{
        OrganizationChangeKind, OrganizationChangeOrigin, OrganizationChangeRecord,
    },
    stamp_store::{StampMutation, StampStore, StampWrite},
    standing::{StampStanding, StandingCache},
};
use central_storage::{AlarmUpdate, BatchOutcome, Statement, StatementBatch, StoreFailure};
use fleet_contracts::{
    DigestFailure, FleetOrganizationStatementRefusal, OrganizationStatementDigestReply,
    ProvisionFailure, ProvisionOrganization, ProvisionOrganizationResult,
    ReadOrganizationStatementDigest,
};
use fleet_runtime::FleetRuntime;
use fleet_runtime_ports::{CentralStore, SyncTime, WorkspaceStores};
use futures_util::future::BoxFuture;
use state_machine_core::{StateMachine, TransitionError};
use std::sync::Arc;

pub trait StampVerification: Send + Sync {
    fn verify_statement<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<VerifiedFleetOrganizationStatement, FleetOrganizationStatementRefusal>>;

    fn verify_stored_statement_facts<'a>(
        &'a self,
        statement: &'a str,
    ) -> BoxFuture<'a, Result<StoredStatementFacts, FleetOrganizationStatementRefusal>>;
}

pub struct SignInService<'owner, Host: CentralStore, Stores: WorkspaceStores> {
    execution: &'owner FleetRuntime<Host, Stores>,
    store: &'owner dyn StampStore,
    standing: &'owner StandingCache,
    verify: Option<Arc<dyn StampVerification>>,
    alarm: Statement,
    #[cfg(feature = "test-support")]
    lost_reply: &'owner std::sync::atomic::AtomicBool,
}

impl<'owner, H: CentralStore, S: WorkspaceStores> SignInService<'owner, H, S> {
    pub fn new(
        execution: &'owner FleetRuntime<H, S>,
        store: &'owner dyn StampStore,
        standing: &'owner StandingCache,
        verify: Option<Arc<dyn StampVerification>>,
        alarm: Statement,
        #[cfg(feature = "test-support")] lost_reply: &'owner std::sync::atomic::AtomicBool,
    ) -> Self {
        Self {
            execution,
            store,
            standing,
            verify,
            alarm,
            #[cfg(feature = "test-support")]
            lost_reply,
        }
    }

    pub async fn prepare_standing(&self) -> Result<(), StoreFailure> {
        let authority = self.standing;
        if authority.needs_check() {
            let stored = self.store.read_stamp().await?;
            if let StoredStampRead::Stamped(stamp) = &stored
                && stamp.organization_id() != self.execution.organization()
            {
                return Err(fleet_runtime_ports::workspace_decode_error::corrupt(
                    "signin_organization_identity.id",
                ));
            }
            let verified = match &stored {
                StoredStampRead::Unprovisioned => None,
                StoredStampRead::Stamped(stamp) => {
                    let check = self.verify.as_deref().ok_or(StoreFailure::Unavailable)?;
                    Some(check.verify_statement(stamp.statement()).await)
                }
            };
            authority.record(
                &stored,
                verified
                    .as_ref()
                    .map(|result| result.as_ref().map_err(|reason| *reason)),
            );
        }
        Ok(())
    }
    pub async fn provision_organization(
        &self,
        request: &ProvisionOrganization,
    ) -> Result<ProvisionOrganizationResult, ProvisionFailure> {
        use ProvisionFailure as Failure;
        if &request.organization_id != self.execution.organization() {
            return Err(Failure::OrganizationMismatch);
        }
        {
            let _turn = self.execution.take_central_turn().await;
            self.prepare_standing().await?;
        }
        let check = self.verify.as_deref().ok_or(Failure::Unavailable)?;
        let verified = check
            .verify_statement(request.organization_statement.as_str())
            .await
            .map_err(|reason| {
                self.execution.observe(
                    fleet_contracts::CentralObservation::OrganizationStatementRefused {
                        organization: self.execution.organization().clone(),
                        refusal: reason,
                    },
                );
                match reason {
                    FleetOrganizationStatementRefusal::OrganizationMismatch => {
                        Failure::OrganizationMismatch
                    }
                    FleetOrganizationStatementRefusal::VerificationUnavailable(_)
                    | FleetOrganizationStatementRefusal::InvalidConfiguration => {
                        Failure::Unavailable
                    }
                    _ => Failure::InvalidStatement,
                }
            })?;
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeSignInSample)
            .await;
        let _turn = self.execution.take_central_turn().await;
        self.prepare_standing().await?;
        let now = self.execution.clock().now();
        verified.require_valid_at(now).map_err(|refusal| {
            self.execution.observe(
                fleet_contracts::CentralObservation::OrganizationStatementRefused {
                    organization: self.execution.organization().clone(),
                    refusal,
                },
            );
            Failure::InvalidStatement
        })?;
        let stored_trusted = self.standing.standing(now) != StampStanding::Refused;
        let stored = self.store.read_stamp().await?;
        let (_, effects) = StampMachine::transition(
            &stored,
            OfferStamp {
                verified: &verified,
                compact: request.organization_statement.as_str(),
            },
        )
        .map_err(|reason| match reason {
            TransitionError::Domain(StampRefusal::OrganizationMismatch) => {
                Failure::OrganizationMismatch
            }
            TransitionError::Domain(StampRefusal::StaleOrganizationStatement) => {
                Failure::StaleStatement
            }
            TransitionError::NotAllowed { .. } | TransitionError::Stale => Failure::Unavailable,
        })?;
        let (stamp, kind, request_id) = match effects.into_iter().next() {
            None => {
                if !stored_trusted || !stored_unexpired(&stored, now) {
                    return Err(Failure::StaleStatement);
                }
                let repair = async {
                    applied(
                        self.execution
                            .statement_host()
                            .run_batch(StatementBatch {
                                statements: vec![],
                                alarm: AlarmUpdate::SetEngineFromQuery {
                                    query: self.alarm.clone(),
                                },
                            })
                            .await?,
                    )
                }
                .await;
                repair.map_err(|error| match error {
                    StoreFailure::Unavailable | StoreFailure::AnswerLost => Failure::OutcomeUnknown,
                    StoreFailure::Corrupt { .. } | StoreFailure::AlarmUnpublishable { .. } => {
                        Failure::Unavailable
                    }
                })?;
                self.standing.forget();
                return Ok(ProvisionOrganizationResult::Provisioned {});
            }
            Some(StampEffect::InitializeStamp) => (
                StampMutation::Initialize,
                OrganizationChangeKind::OrganizationProvisioned,
                self.execution.organization().as_str().to_owned(),
            ),
            Some(StampEffect::ReplaceStamp { predecessor }) => (
                StampMutation::Replace(predecessor),
                OrganizationChangeKind::StampRenewed,
                verified.digest().as_str().to_owned(),
            ),
        };
        let record = OrganizationChangeRecord {
            id: uuid::Uuid::parse_str(&self.execution.ids().new_id().to_string())
                .map_err(|_| Failure::Unavailable)?,
            request_id: request_id.try_into()?,
            kind,
            origin: OrganizationChangeOrigin::SignInProvisioning {},
            target_id: self.execution.organization().as_str().to_owned(),
            created_at_millis: now.unix_millis(),
        };

        let write = StampWrite {
            mutation: stamp,
            organization: self.execution.organization().clone(),
            statement: request.organization_statement.as_str().to_owned(),
            now_ms: now.unix_millis(),
            issued_seconds: verified.issued_at().0,
            expires_seconds: verified.expires_at().0,
            audit: record,
        };
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
            .await;
        match self.store.write_stamp(write, self.alarm.clone()).await? {
            BatchOutcome::Applied { .. } => {}
            // A rejected statement keeps main's store-outage answer on this path.
            BatchOutcome::StatementRejected { .. } => return Err(Failure::Unavailable),
            _ => return Err(Failure::StaleStatement),
        }
        self.standing.forget();
        #[cfg(feature = "test-support")]
        if self
            .lost_reply
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Failure::OutcomeUnknown);
        }
        Ok(ProvisionOrganizationResult::Provisioned {})
    }
    pub async fn read_statement_digest(
        &self,
        request: &ReadOrganizationStatementDigest,
    ) -> Result<OrganizationStatementDigestReply, DigestFailure> {
        use DigestFailure as Failure;
        if &request.organization_id != self.execution.organization() {
            return Err(Failure::OrganizationMismatch);
        }
        let _turn = self.execution.take_central_turn().await;
        let StoredStampRead::Stamped(stamp) = self
            .store
            .read_stamp()
            .await
            .map_err(|_| Failure::Unavailable)?
        else {
            return Err(Failure::Unprovisioned);
        };
        if stamp.organization_id() != self.execution.organization() {
            return Err(Failure::Unavailable);
        }
        let check = self.verify.as_deref().ok_or(Failure::Unavailable)?;
        let verified = check
            .verify_stored_statement_facts(stamp.statement())
            .await
            .map_err(|_| Failure::Unavailable)?;
        Ok(OrganizationStatementDigestReply {
            digest: verified.digest().as_str().to_owned(),
            expires_at: verified.expires_at().0,
            signing_key_id: verified.key_id().to_owned(),
        })
    }
}
fn stored_unexpired(stored: &StoredStampRead, now: SyncTime) -> bool {
    let StoredStampRead::Stamped(stamp) = stored else {
        return false;
    };
    crate::organization_authority::stored_statement_unexpired(stamp.statement(), now)
}

fn applied(outcome: BatchOutcome) -> Result<(), StoreFailure> {
    match outcome {
        BatchOutcome::Applied { .. } => Ok(()),
        BatchOutcome::GuardFailed { .. }
        | BatchOutcome::ConstraintViolated { .. }
        | BatchOutcome::StatementRejected { .. } => Err(StoreFailure::Unavailable),
    }
}
