use std::{future::Future, pin::Pin};

use super::*;

#[derive(Debug)]
pub struct FixtureHolderOffer {
    pub request: Box<approval_request_protocol::DeliverEndingRequest>,
}

#[derive(Debug)]
pub struct FixtureCommittedApply<Reply> {
    pub reply: Reply,
    pub ending_offers: Vec<FixtureHolderOffer>,
}

pub(super) struct NativeApprovalCommit<'fixture, Command> {
    pub(super) fixture: &'fixture NativeApprovalStoreFixture,
    pub(super) fail_after_write: Option<usize>,
    pub(super) erased_marker_read_fault: Option<ErasedMarkerReadFault>,
    pub(super) run_command: Command,
    pub(super) evidence: &'fixture mut ApprovalTransactionEvidence,
}

impl<'fixture, Reply, Command>
    ApprovalCommitOwner<(PendingApprovalReply<Reply>, PendingApprovalEffects)>
    for NativeApprovalCommit<'fixture, Command>
where
    Reply: 'fixture,
    Command: 'fixture
        + AsyncFnOnce(
            &mut ApprovalApplyingStore<'_, NativeApprovalTransaction, StoredSessionFacts>,
        ) -> Result<PendingApprovalReply<Reply>, ApprovalApplyFailure>,
{
    type Error = ApprovalApplyFailure;

    type Commit = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (PendingApprovalReply<Reply>, PendingApprovalEffects),
                        Self::Error,
                    >,
                > + 'fixture,
        >,
    >;

    fn apply_and_commit(self, commit: ApprovalCommitScope) -> Self::Commit {
        Box::pin(async move {
            let pool = self.fixture.reopen_pool().await;
            let mut transaction = NativeApprovalTransaction {
                organization_id: fixture_organization(),

                transaction: pool.begin_with("BEGIN IMMEDIATE").await.unwrap(),
                fail_after_write: self.fail_after_write,
                erased_marker_read_fault: self.erased_marker_read_fault,
                evidence: ApprovalTransactionEvidence::default(),
            };

            let mut ids = StoreFixtureIds(&self.fixture.ids);
            let mut store = ApprovalApplyingStore::new(
                &commit,
                &mut transaction,
                &StoredSessionFacts,
                &self.fixture.clock,
                &mut ids,
            );
            let result = (self.run_command)(&mut store).await;
            let holder_effects = store.finish_effects();
            assert!(
                transaction.erased_marker_read_fault.is_none(),
                "the applying command must reach the faulted erased-marker read"
            );
            if result.is_ok() {
                transaction.transaction.commit().await.unwrap();
            } else {
                transaction.transaction.rollback().await.unwrap();
            }
            *self.evidence = transaction.evidence;
            pool.close().await;
            result.map(|reply| (reply, holder_effects))
        })
    }
}
