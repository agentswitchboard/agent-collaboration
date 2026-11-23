//! The person approval transaction checks proof freshness and applies lifecycle changes.

use central_engine_model::{EngineTime, IdSource as EngineIdSource, supported_time};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalApplyingStore, ApprovalCommitOwner, ApprovalCommitProof,
    ApprovalCommitScope, ApprovalPersonStoreTransaction, ApprovalReplyAddress,
    CommittedApprovalOffer, PendingApprovalHolderEffects, PendingApprovalReply,
    complete_approval_commit,
};
use fleet_engine::{approval_time_floor_statement, decode_approval_time_floor};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{Clock, IdSource, SyncTime, workspace_decode_error::corrupt};
use fleet_signin::{
    ApprovalApplyingContext, FleetPermission, VerifiedApprovalPersonRequest,
    approval_person_wire::{
        ApprovalDecideReply, ApprovalListReply, ApprovalPersonOperation, ApprovalReadReply,
    },
    person_proof::{AdmissionRefusal, admission_before_person, request_time_refusal},
};
use futures_util::future::BoxFuture;
use workspace_ids::RequestId;

use crate::{
    authorization_floor_rows::{
        decode_organization_floor, decode_person_floor, organization_floor_statement,
        person_floor_statement,
    },
    person_proof_repository::{proof_was_used, reserve_approval_proof, used_proof_statement},
};

pub enum PersonApprovalAnswer {
    Unauthenticated,
    Refused(AdmissionRefusal),
    List(ApprovalListReply),
    Read(ApprovalReadReply),
    Decide(ApprovalDecideReply),
}

/// Opens and commits the physical organization transaction on the host.
pub trait PersonApprovalTransactions: Send + Sync + 'static {
    fn apply_and_commit<'request>(
        &'request self,
        application: PersonApprovalApplication,
        scope: ApprovalCommitScope,
    ) -> BoxFuture<'request, Result<AppliedPersonApproval, StoreFailure>>;

    fn delivery_after_commit(
        &self,
        offers: Vec<CommittedApprovalOffer>,
    ) -> Option<crate::LinkCallCompletion>;
}

/// Verified input for the current organization turn. Only Core builds this.
pub struct PersonApprovalApplication {
    verified: VerifiedApprovalPersonRequest,
    organization: OrganizationId,
    time: SyncTime,
}

impl PersonApprovalApplication {
    pub(super) fn new(
        organization: &OrganizationId,
        verified: VerifiedApprovalPersonRequest,
        time: SyncTime,
    ) -> Self {
        Self {
            verified,
            organization: organization.clone(),
            time,
        }
    }

    pub(super) async fn commit(
        self,
        transactions: &dyn PersonApprovalTransactions,
    ) -> Result<(ApprovalCommitProof, AppliedPersonApproval), StoreFailure> {
        complete_approval_commit(PersonApprovalCommit {
            transactions,
            application: self,
        })
        .await
    }

    pub fn organization(&self) -> &OrganizationId {
        &self.organization
    }

    pub async fn apply(
        self,
        transaction: &mut (impl ApprovalPersonStoreTransaction + Send),
        commit: &ApprovalCommitScope,
        ids: &dyn IdSource,
    ) -> Result<AppliedPersonApproval, StoreFailure> {
        if transaction.organization_id() != &self.organization {
            return Err(StoreFailure::Unavailable);
        }
        let proof = self.verified.proof();
        let now_ms = self.time.unix_millis();
        if let Some(reason) = request_time_refusal(&proof.into(), now_ms) {
            return Ok(AppliedPersonApproval::refused(reason));
        }
        let organization = self.organization.as_str();
        let organization_rows = transaction
            .read_approval_statement(organization_floor_statement(organization))
            .await?;
        let organization_floor = decode_organization_floor(&organization_rows, organization)?;
        if let Err(reason) =
            admission_before_person(&proof.into(), now_ms, organization_floor.as_ref())?
        {
            return Ok(AppliedPersonApproval::refused(reason));
        }
        let person_rows = transaction
            .read_approval_statement(person_floor_statement(organization, &proof.person_id))
            .await?;
        let person_floor = decode_person_floor(&person_rows, organization, &proof.person_id)?;
        let proof_id = proof.request_id.to_string();
        let proof_rows = transaction
            .read_approval_statement(used_proof_statement(&proof_id))
            .await?;
        let applying = ApprovalApplyingContext {
            organization: &self.organization,
            now_ms,
            organization_floor: organization_floor.as_ref(),
            person_floor: person_floor.as_ref(),
            proof_used: proof_was_used(&proof_rows, &proof_id)?,
        };
        let retained_at = EngineTime::from_timestamp_millis(now_ms)
            .filter(|time| supported_time(*time))
            .ok_or_else(|| corrupt("approval_store.applying_time"))?;
        let rows = transaction
            .read_approval_statement(approval_time_floor_statement(organization))
            .await?;
        let floor = decode_approval_time_floor(&rows)?;
        let lifecycle_at = floor.map_or(retained_at, |previous| previous.max(retained_at));
        let moves_time_floor = floor.is_none_or(|previous| lifecycle_at > previous);
        let clock = ApplyingClock(SyncTime::from_unix_millis(lifecycle_at.timestamp_millis()));
        let mut ids = ApprovalIds(ids);
        let mut approvals = ApprovalApplyingStore::new(commit, transaction, &(), &clock, &mut ids);

        let operation_id = decide_operation_id(self.verified.operation());
        if !proof
            .permissions
            .iter()
            .any(|grant| grant == FleetPermission::DecideApproval.as_str())
            && let Some(retry) = self.verified.authorize_retained_decision(applying)?
            && let Some(reply) = approvals
                .replay_person_decision(&retry, retained_at)
                .await
                .map_err(apply_failure)?
        {
            let person = reply_address(&self.organization, retry.person());
            let reservation = reserve_approval_proof(proof, operation_id, now_ms)?;
            let applied = AppliedPersonApproval::admitted(
                PendingReply::Decide(reply),
                person,
                approvals.finish_holder_effects(),
            );
            return reserve_and_finish(transaction, reservation, applied).await;
        }

        let admitted = match self.verified.admit(applying)? {
            Ok(admitted) => admitted,
            Err(reason) => return Ok(AppliedPersonApproval::refused(reason)),
        };
        let (authority, operation, proof) = admitted.into_parts();
        let reservation = reserve_approval_proof(&proof, decide_operation_id(&operation), now_ms)?;
        let reply = match operation {
            ApprovalPersonOperation::List(request) => PendingReply::List(
                approvals
                    .list_person_requests(&authority, request.filter, request.cursor.as_ref())
                    .await
                    .map_err(apply_failure)?,
            ),
            ApprovalPersonOperation::Read(request) => PendingReply::Read(
                approvals
                    .read_person_request(&authority, &request.request_id)
                    .await
                    .map_err(apply_failure)?,
            ),
            ApprovalPersonOperation::Decide(request) => PendingReply::Decide(
                approvals
                    .decide_person_request(&authority, request, retained_at)
                    .await
                    .map_err(apply_failure)?,
            ),
        };
        let effects = approvals
            .finish_person_effects(moves_time_floor)
            .await
            .map_err(apply_failure)?;
        let applied = AppliedPersonApproval::admitted(
            reply,
            reply_address(authority.organization(), authority.person()),
            effects,
        );
        reserve_and_finish(transaction, reservation, applied).await
    }
}

fn reply_address(
    organization: &OrganizationId,
    person: &workspace_ids::PersonId,
) -> ApprovalReplyAddress {
    ApprovalReplyAddress::Person {
        organization: organization.clone(),
        person: person.clone(),
    }
}

async fn reserve_and_finish(
    transaction: &mut (impl ApprovalPersonStoreTransaction + Send),
    reservation: GuardedStatement,
    applied: AppliedPersonApproval,
) -> Result<AppliedPersonApproval, StoreFailure> {
    let affected = transaction
        .execute_approval_statement(reservation.statement)
        .await?;
    if !reservation.expect.admits(affected) {
        // Failure here must roll back the preceding decision, identity and log together.
        return Err(StoreFailure::Unavailable);
    }
    Ok(applied)
}

pub(super) fn decide_operation_id(operation: &ApprovalPersonOperation) -> Option<&RequestId> {
    match operation {
        ApprovalPersonOperation::Decide(request) => Some(&request.operation_id),
        ApprovalPersonOperation::List(_) | ApprovalPersonOperation::Read(_) => None,
    }
}

fn apply_failure(error: ApprovalApplyFailure) -> StoreFailure {
    match error {
        ApprovalApplyFailure::Store(error) => error,
        ApprovalApplyFailure::Lifecycle(_) | ApprovalApplyFailure::GuardChanged => {
            StoreFailure::Unavailable
        }
    }
}

struct ApplyingClock(SyncTime);

impl Clock for ApplyingClock {
    fn now(&self) -> SyncTime {
        self.0
    }
}

struct ApprovalIds<'request>(&'request dyn IdSource);

impl EngineIdSource for ApprovalIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

enum PendingReply {
    List(PendingApprovalReply<ApprovalListReply>),
    Read(PendingApprovalReply<ApprovalReadReply>),
    Decide(PendingApprovalReply<ApprovalDecideReply>),
}

enum ApplyingOutcome {
    Refused(AdmissionRefusal),
    Admitted {
        reply: Box<PendingReply>,
        address: ApprovalReplyAddress,
        effects: PendingApprovalHolderEffects,
    },
}

/// Can be published only by the completion of the matching host transaction.
pub struct AppliedPersonApproval(ApplyingOutcome);

impl AppliedPersonApproval {
    #[cfg(feature = "test-support")]
    pub(super) fn is_admitted(&self) -> bool {
        matches!(&self.0, ApplyingOutcome::Admitted { .. })
    }

    fn refused(reason: AdmissionRefusal) -> Self {
        Self(ApplyingOutcome::Refused(reason))
    }

    fn admitted(
        reply: PendingReply,
        address: ApprovalReplyAddress,
        effects: PendingApprovalHolderEffects,
    ) -> Self {
        Self(ApplyingOutcome::Admitted {
            reply: Box::new(reply),
            address,
            effects,
        })
    }

    pub(super) fn publish(
        self,
        commit: &ApprovalCommitProof,
    ) -> Result<(PersonApprovalAnswer, Vec<CommittedApprovalOffer>), StoreFailure> {
        let (reply, address, effects) = match self.0 {
            ApplyingOutcome::Refused(reason) => {
                return Ok((PersonApprovalAnswer::Refused(reason), Vec::new()));
            }
            ApplyingOutcome::Admitted {
                reply,
                address,
                effects,
            } => (reply, address, effects),
        };
        let offers = effects
            .release(commit)
            .map_err(|_| StoreFailure::Unavailable)?;
        let answer = match *reply {
            PendingReply::List(pending) => pending
                .take_for_recipient(commit, &address)
                .map(PersonApprovalAnswer::List),
            PendingReply::Read(pending) => pending
                .take_for_recipient(commit, &address)
                .map(PersonApprovalAnswer::Read),
            PendingReply::Decide(pending) => pending
                .take_for_recipient(commit, &address)
                .map(PersonApprovalAnswer::Decide),
        }
        .map_err(|_| StoreFailure::Unavailable)?;
        Ok((answer, offers))
    }
}

struct PersonApprovalCommit<'request> {
    transactions: &'request dyn PersonApprovalTransactions,
    application: PersonApprovalApplication,
}

impl<'request> ApprovalCommitOwner<AppliedPersonApproval> for PersonApprovalCommit<'request> {
    type Error = StoreFailure;
    type Commit = BoxFuture<'request, Result<AppliedPersonApproval, StoreFailure>>;

    fn apply_and_commit(self, scope: ApprovalCommitScope) -> Self::Commit {
        self.transactions.apply_and_commit(self.application, scope)
    }
}
