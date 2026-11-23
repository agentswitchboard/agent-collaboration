use crate::statement_store::WorkerExecutorLender;
use central_link_call_composition::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture,
};
use central_storage::StoreFailure;
use collaboration_central_core::CentralCallDispatch;
use fleet_actor_identity::SessionServiceError;
use fleet_identity_protocol::OrganizationId;
use sqlx_durable_object::{DoConnection, DoTransaction};
use std::sync::Arc;
use worker::{send::SendWrapper, wasm_bindgen::JsCast};

pub(super) struct WorkerSessionTransactions {
    pub(super) organization: OrganizationId,
    pub(super) connection: Arc<SendWrapper<DoConnection>>,
}

impl OrganizationSessionTransactions for WorkerSessionTransactions {
    type Transaction<'loan> = WorkerExecutorLender<'loan, DoTransaction>;

    async fn apply(
        &self,
        dispatch: impl for<'loan> FnOnce(
            OrganizationLoan<'loan, Self::Transaction<'loan>>,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> CentralCallDispatch {
        let organization = self.organization.clone();
        self.connection
            .transaction(move |transaction| async move {
                let loan = OrganizationLoan::new(
                    &organization,
                    WorkerExecutorLender::from_storage_executor(&transaction),
                );
                dispatch(loan).await.map_err(SessionTransactionError::Apply)
            })
            .await
            .map_err(|error| match error {
                SessionTransactionError::Apply(failure) => failure,
                SessionTransactionError::Commit => StoreFailure::AnswerLost,
            })
    }
}

enum SessionTransactionError {
    Apply(StoreFailure),
    Commit,
}

impl From<sqlx_core::Error> for SessionTransactionError {
    fn from(_: sqlx_core::Error) -> Self {
        Self::Commit
    }
}

pub(super) fn fill_session_secret(bytes: &mut [u8; 32]) -> Result<(), SessionServiceError> {
    let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    global
        .crypto()
        .and_then(|crypto| crypto.get_random_values_with_u8_array(bytes))
        .map(|_| ())
        .map_err(|_| SessionServiceError::SecretUnavailable)
}
