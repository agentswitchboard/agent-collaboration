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

impl<'fixture, Output, Command> ApprovalCommitOwner<Output>
    for NativeApprovalCommit<'fixture, Command>
where
    Output: 'fixture,
    Command: 'fixture
        + AsyncFnOnce(
            &mut NativeApprovalTransaction,
            ApprovalCommitScope,
            &mut dyn IdSource,
        ) -> Result<Output, ApprovalApplyFailure>,
{
    type Error = ApprovalApplyFailure;

    type Commit = Pin<Box<dyn Future<Output = Result<Output, Self::Error>> + 'fixture>>;

    fn apply_and_commit(self, commit: ApprovalCommitScope) -> Self::Commit {
        Box::pin(async move {
            let pool = self.fixture.reopen_pool().await;
            let mut transaction = NativeApprovalTransaction {
                organization_id: fixture_organization(),
                transaction: pool.begin_with("BEGIN IMMEDIATE").await.unwrap(),
                sampled: self.fixture.clock.now(),
                fail_after_write: self.fail_after_write,
                erased_marker_read_fault: self.erased_marker_read_fault,
                evidence: ApprovalTransactionEvidence::default(),
            };

            let mut ids = StoreFixtureIds(&self.fixture.ids);
            let result = (self.run_command)(&mut transaction, commit, &mut ids).await;
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
            result
        })
    }
}
