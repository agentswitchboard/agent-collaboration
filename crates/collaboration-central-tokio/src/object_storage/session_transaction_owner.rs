use super::{NativeTransaction, ObjectStorage};
use central_link_call_composition::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture,
    SessionEntropyUnavailable,
};
use collaboration_central_core::CentralCallDispatch;
use fleet_identity_protocol::OrganizationId;
use std::{ops::ControlFlow, sync::Arc};

pub(crate) struct NativeSessionTransactions {
    pub(crate) organization: OrganizationId,
    pub(crate) storage: Arc<ObjectStorage>,
}

impl OrganizationSessionTransactions for NativeSessionTransactions {
    type Transaction<'loan> = NativeTransaction<'loan>;

    async fn apply(
        &self,
        dispatch: impl for<'loan> FnOnce(
            OrganizationLoan<'loan, Self::Transaction<'loan>>,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> CentralCallDispatch {
        let _order = self.storage.lock_batch_order().await;
        let result = self
            .storage
            .opened()
            .await?
            .run_transaction(async |connection| {
                dispatch(OrganizationLoan::new(
                    &self.organization,
                    NativeTransaction(connection),
                ))
                .await
                .map(ControlFlow::<std::convert::Infallible, _>::Continue)
            })
            .await?;
        match result {
            ControlFlow::Continue(reply) => Ok(reply),
            ControlFlow::Break(impossible) => match impossible {},
        }
    }
}

pub(crate) fn fill_session_secret(bytes: &mut [u8; 32]) -> Result<(), SessionEntropyUnavailable> {
    use ring::rand::{SecureRandom, SystemRandom};
    SystemRandom::new()
        .fill(bytes)
        .map_err(|_| SessionEntropyUnavailable)
}
