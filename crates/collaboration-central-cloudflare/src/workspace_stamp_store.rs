//! Durable Object bindings for local checked stamp operations.
use super::{
    AlarmPolicy, BatchOutcome, CentralStatements, SendFuture, StoreFailure, statement_store,
};
use fleet_signin::{
    organization_authority::StoredStampRead,
    stamp_store::{StampStore, StampWrite, read_stamp},
};
use futures_util::future::BoxFuture;

impl StampStore for CentralStatements {
    fn read_stamp(&self) -> BoxFuture<'_, Result<StoredStampRead, StoreFailure>> {
        Box::pin(SendFuture::new(async move {
            read_stamp(&*self.connection).await
        }))
    }

    fn write_stamp(&self, write: StampWrite) -> BoxFuture<'_, Result<BatchOutcome, StoreFailure>> {
        Box::pin(SendFuture::new(async move {
            let policy = AlarmPolicy::Engine(
                self.state.storage(),
                self.clock.clone(),
                self.engine_decider(true),
            );
            statement_store::write_stamp(&self.connection, write, policy).await
        }))
    }
}
