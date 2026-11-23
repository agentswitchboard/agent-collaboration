use crate::statement_store::WorkerExecutorLender;
use central_link_call_composition::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFailure, SessionApplyFuture,
    SessionApplyResult, SessionEntropyUnavailable,
};
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use sqlx_durable_object::{DoConnection, DoTransaction};
use std::sync::Arc;
use worker::{send::SendWrapper, wasm_bindgen::JsCast};

pub(super) struct WorkerSessionTransactions {
    #[cfg(feature = "test-support")]
    pub(super) fail_reserved_session_reply: Arc<std::sync::atomic::AtomicBool>,
    pub(super) connection: Arc<SendWrapper<DoConnection>>,
}

impl OrganizationSessionTransactions for WorkerSessionTransactions {
    type Transaction<'connection> =
        OrganizationLoan<WorkerExecutorLender<'connection, DoTransaction>>;

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
        let fail_reserved_session_reply = self.fail_reserved_session_reply.clone();
        self.connection
            .transaction(move |transaction| async move {
                let mut loan = OrganizationLoan::verify(
                    WorkerExecutorLender::from_storage_executor(&transaction),
                    &admitted_organization,
                )
                .await
                .map_err(|failure| SessionTransactionError::Apply(failure.into()))?;
                let result = dispatch(&mut loan).await;
                #[cfg(feature = "test-support")]
                let result = central_link_call_composition::inject_reserved_reply_fault(
                    &fail_reserved_session_reply,
                    result,
                );
                result.map_err(SessionTransactionError::Apply)
            })
            .await
            .map_err(|error| match error {
                SessionTransactionError::Apply(failure) => failure,
                SessionTransactionError::Commit => StoreFailure::AnswerLost.into(),
            })
    }
}

enum SessionTransactionError {
    Apply(SessionApplyFailure),
    Commit,
}

impl From<sqlx_core::Error> for SessionTransactionError {
    fn from(_: sqlx_core::Error) -> Self {
        Self::Commit
    }
}

pub(super) fn fill_session_secret(bytes: &mut [u8; 32]) -> Result<(), SessionEntropyUnavailable> {
    let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    global
        .crypto()
        .and_then(|crypto| crypto.get_random_values_with_u8_array(bytes))
        .map(|_| ())
        .map_err(|_| SessionEntropyUnavailable)
}
