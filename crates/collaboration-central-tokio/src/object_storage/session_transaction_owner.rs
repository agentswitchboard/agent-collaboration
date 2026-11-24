use super::{NativeTransaction, ObjectStorage};
use central_link_call_composition::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture, SessionApplyResult,
    SessionEntropyUnavailable,
};
use fleet_identity_protocol::OrganizationId;
use std::{ops::ControlFlow, sync::Arc};

#[derive(Clone)]
pub(crate) struct NativeSessionTransactions {
    pub(crate) storage: Arc<ObjectStorage>,
}

#[cfg(feature = "test-support")]
impl ObjectStorage {
    pub fn fail_next_session_loan(&self) {
        self.fail_session_loan
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn fail_next_reserved_session_reply(&self) {
        self.fail_reserved_session_reply
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl OrganizationSessionTransactions for NativeSessionTransactions {
    type Transaction<'connection> = OrganizationLoan<NativeTransaction<'connection>>;

    async fn apply(
        &self,
        admitted_organization: OrganizationId,
        dispatch: impl for<'loan, 'connection> FnOnce(
            &'loan mut Self::Transaction<'connection>,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> SessionApplyResult {
        #[cfg(feature = "test-support")]
        if self
            .storage
            .fail_session_loan
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(central_storage::StoreFailure::Unavailable.into());
        }
        let _order = self.storage.lock_batch_order().await;
        let result = self
            .storage
            .opened()
            .await?
            .run_transaction(async |connection| {
                let result = match OrganizationLoan::verify(
                    NativeTransaction(connection),
                    &admitted_organization,
                )
                .await
                {
                    Ok(mut loan) => dispatch(&mut loan).await,
                    Err(failure) => Err(failure.into()),
                };
                #[cfg(feature = "test-support")]
                let result = central_link_call_composition::inject_reserved_reply_fault(
                    &self.storage.fail_reserved_session_reply,
                    result,
                );
                Ok(result.map_or_else(ControlFlow::Break, ControlFlow::Continue))
            })
            .await?;
        match result {
            ControlFlow::Continue(reply) => Ok(reply),
            ControlFlow::Break(failure) => Err(failure),
        }
    }
}

pub(crate) fn fill_session_secret(bytes: &mut [u8; 32]) -> Result<(), SessionEntropyUnavailable> {
    use ring::rand::{SecureRandom, SystemRandom};
    SystemRandom::new()
        .fill(bytes)
        .map_err(|_| SessionEntropyUnavailable)
}

#[cfg(all(test, feature = "test-support"))]
#[path = "session_transaction_owner_test.rs"]
mod tests;
