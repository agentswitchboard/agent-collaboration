//! The person approval transaction checks proof freshness and applies lifecycle changes.

use central_engine_model::IdSource as EngineIdSource;
use central_storage::{AlarmPublicationFailure, StoreFailure};
use fleet_approvals::{
    ApprovalApplyFailure, ApprovalCommitOwner, ApprovalCommitProof, ApprovalCommitScope,
    ApprovalPersonStoreTransaction, ApprovalReplyAddress, CommittedApprovalEffects,
    PendingApprovalEffects, PersonApprovalStoreReply, apply_person_command,
    complete_approval_commit,
};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::IdSource;
#[cfg(feature = "test-support")]
use fleet_signin::approval_person_wire::ApprovalPersonOperation;
use fleet_signin::{
    VerifiedApprovalPersonRequest,
    approval_person_wire::{ApprovalDecideReply, ApprovalListReply, ApprovalReadReply},
    person_proof::AdmissionRefusal,
};
use futures_util::future::BoxFuture;
#[cfg(feature = "test-support")]
use workspace_ids::RequestId;

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
        offers: CommittedApprovalEffects,
    ) -> Option<crate::LinkCallCompletion>;
}

/// Verified input for the current organization turn. Only Core builds this.
pub struct PersonApprovalApplication {
    verified: VerifiedApprovalPersonRequest,
    organization: OrganizationId,
}

impl PersonApprovalApplication {
    pub(super) fn new(
        organization: &OrganizationId,
        verified: VerifiedApprovalPersonRequest,
    ) -> Self {
        Self {
            verified,
            organization: organization.clone(),
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
        transaction: &mut impl ApprovalPersonStoreTransaction,
        commit: &ApprovalCommitScope,
        ids: &dyn IdSource,
    ) -> Result<AppliedPersonApproval, StoreFailure> {
        let command = match self.verified.admit(transaction).await? {
            Ok(command) => command,
            Err(refusal) => return Ok(AppliedPersonApproval::refused(refusal)),
        };
        let mut ids = ApprovalIds(ids);
        let result = apply_person_command(command, commit, &mut ids)
            .await
            .map_err(apply_failure)?;
        Ok(match result {
            Some(result) => {
                AppliedPersonApproval::admitted(result.reply, result.address, result.effects)
            }
            None => AppliedPersonApproval::refused(AdmissionRefusal::NotPermitted),
        })
    }
}

#[cfg(feature = "test-support")]
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

struct ApprovalIds<'request>(&'request dyn IdSource);

impl EngineIdSource for ApprovalIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

enum ApplyingOutcome {
    Refused(AdmissionRefusal),
    Admitted {
        reply: Box<PersonApprovalStoreReply>,
        address: ApprovalReplyAddress,
        effects: PendingApprovalEffects,
    },
}

/// Can be published only by the completion of the matching host transaction.
pub struct AppliedPersonApproval {
    outcome: ApplyingOutcome,
    publication_failure: Option<AlarmPublicationFailure>,
}

impl AppliedPersonApproval {
    #[cfg(feature = "test-support")]
    pub(super) fn is_admitted(&self) -> bool {
        matches!(&self.outcome, ApplyingOutcome::Admitted { .. })
    }

    fn refused(reason: AdmissionRefusal) -> Self {
        Self {
            outcome: ApplyingOutcome::Refused(reason),
            publication_failure: None,
        }
    }

    fn admitted(
        reply: PersonApprovalStoreReply,
        address: ApprovalReplyAddress,
        effects: PendingApprovalEffects,
    ) -> Self {
        Self {
            outcome: ApplyingOutcome::Admitted {
                reply: Box::new(reply),
                address,
                effects,
            },
            publication_failure: None,
        }
    }

    #[must_use]
    pub fn with_publication_failure(mut self, failure: Option<AlarmPublicationFailure>) -> Self {
        self.publication_failure = failure;
        self
    }

    pub(super) const fn publication_failure(&self) -> Option<AlarmPublicationFailure> {
        self.publication_failure
    }

    pub(super) fn publish(
        self,
        commit: &ApprovalCommitProof,
    ) -> Result<(PersonApprovalAnswer, CommittedApprovalEffects), StoreFailure> {
        let (reply, address, effects) = match self.outcome {
            ApplyingOutcome::Refused(reason) => {
                return Ok((
                    PersonApprovalAnswer::Refused(reason),
                    CommittedApprovalEffects::default(),
                ));
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
            PersonApprovalStoreReply::List(pending) => pending
                .take_for_recipient(commit, &address)
                .map(PersonApprovalAnswer::List),
            PersonApprovalStoreReply::Read(pending) => pending
                .take_for_recipient(commit, &address)
                .map(PersonApprovalAnswer::Read),
            PersonApprovalStoreReply::Decide(pending) => pending
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
