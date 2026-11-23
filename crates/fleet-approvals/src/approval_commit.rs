//! Outputs are released with proof from the same owning transaction's commit path.
use std::{future::Future, sync::Arc};

use approval_request_protocol::DeliverEndingRequest;
use fleet_identity_protocol::OrganizationId;
use workspace_ids::PersonId;

use crate::{ApprovalHolderAdmission, ApprovalHolderScope, ApprovalSession};

/// The host uses this scope only for the one applying transaction it commits.
pub struct ApprovalCommitScope(Arc<()>);

/// The host's transaction owner returns Ok only after the actual transaction commits.
pub trait ApprovalCommitOwner<Output> {
    type Error;

    fn apply_and_commit(
        self,
        scope: ApprovalCommitScope,
    ) -> impl Future<Output = Result<Output, Self::Error>>;
}

pub struct ApprovalCommitProof(Arc<()>);

pub async fn complete_approval_commit<Output, Owner: ApprovalCommitOwner<Output>>(
    owner: Owner,
) -> Result<(ApprovalCommitProof, Output), Owner::Error> {
    let scope = Arc::new(());
    let committed = owner
        .apply_and_commit(ApprovalCommitScope(scope.clone()))
        .await?;
    Ok((ApprovalCommitProof(scope), committed))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalReplyAddress {
    Holder(ApprovalHolderScope),
    Session {
        organization: OrganizationId,
        session: ApprovalSession,
    },
    Person {
        organization: OrganizationId,
        person: PersonId,
    },
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalReleaseFailure {
    OtherCommit,
    OtherRecipient,
}

/// Pending data supports neither inspection nor deserialization.
///
/// ```compile_fail,E0616
/// fn inspect(value: fleet_approvals::PendingApprovalReply<u8>) -> u8 { value.reply }
/// ```
pub struct PendingApprovalReply<Reply> {
    transaction: Arc<()>,
    address: ApprovalReplyAddress,
    reply: Reply,
}

impl<Reply> PendingApprovalReply<Reply> {
    pub fn take_for_recipient(
        self,
        proof: &ApprovalCommitProof,
        address: &ApprovalReplyAddress,
    ) -> Result<Reply, ApprovalReleaseFailure> {
        if !Arc::ptr_eq(&self.transaction, &proof.0) {
            return Err(ApprovalReleaseFailure::OtherCommit);
        }
        if &self.address != address {
            return Err(ApprovalReleaseFailure::OtherRecipient);
        }
        Ok(self.reply)
    }

    #[cfg(feature = "test-support")]
    pub fn take_fixture_reply(
        self,
        proof: &ApprovalCommitProof,
    ) -> Result<Reply, ApprovalReleaseFailure> {
        let address = self.address.clone();
        self.take_for_recipient(proof, &address)
    }
}

/// Kept by the transaction owner for the holder, separately from the caller reply.
///
/// ```compile_fail,E0616
/// fn inspect(value: fleet_approvals::PendingApprovalHolderEffects) { let _ = value.offers; }
/// ```
pub struct PendingApprovalHolderEffects {
    transaction: Arc<()>,
    offers: Vec<CommittedApprovalOffer>,
}

impl PendingApprovalHolderEffects {
    pub fn release(
        self,
        proof: &ApprovalCommitProof,
    ) -> Result<Vec<CommittedApprovalOffer>, ApprovalReleaseFailure> {
        if !Arc::ptr_eq(&self.transaction, &proof.0) {
            return Err(ApprovalReleaseFailure::OtherCommit);
        }
        Ok(self.offers)
    }
}

pub struct CommittedApprovalOffer {
    holder: ApprovalHolderScope,
    request: Box<DeliverEndingRequest>,
}

impl CommittedApprovalOffer {
    pub(crate) fn new(holder: ApprovalHolderScope, request: Box<DeliverEndingRequest>) -> Self {
        Self { holder, request }
    }

    pub fn holder_address(&self) -> &ApprovalHolderScope {
        &self.holder
    }

    pub fn take_for_holder(
        self,
        holder: &ApprovalHolderAdmission,
    ) -> Result<Box<DeliverEndingRequest>, ApprovalReleaseFailure> {
        if &self.holder != holder.holder_scope() {
            return Err(ApprovalReleaseFailure::OtherRecipient);
        }
        Ok(self.request)
    }
}

impl ApprovalCommitScope {
    pub(crate) fn pending_reply<Reply>(
        &self,
        address: ApprovalReplyAddress,
        reply: Reply,
    ) -> PendingApprovalReply<Reply> {
        PendingApprovalReply {
            transaction: self.0.clone(),
            address,
            reply,
        }
    }

    pub(crate) fn pending_holder_effects(
        &self,
        offers: Vec<CommittedApprovalOffer>,
    ) -> PendingApprovalHolderEffects {
        PendingApprovalHolderEffects {
            transaction: self.0.clone(),
            offers,
        }
    }
}
