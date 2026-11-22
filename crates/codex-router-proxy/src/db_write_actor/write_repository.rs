//! Storage boundary and SQLite implementation for durable write commands.

use codex_router_core::ids::{AccountId, ReservationId};
use codex_router_core::routes::RouteBand;
use codex_router_state::affinity_owner::PreviousResponseAffinityOwnerRecord;
use codex_router_state::session_account_affinity::SessionAccountAffinity;
use codex_router_state::sqlite::{AsyncSqliteStateStore, StateStoreError};
use futures_util::future::BoxFuture;

use crate::provider_error::ProviderErrorClassification;
use thiserror::Error;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QuotaWriteCompletion {
    pub(crate) account_id: AccountId,
    pub(crate) route_band: RouteBand,
    pub(crate) result: Result<(), StateStoreError>,
}

/// Storage boundary used by the write actor.
pub trait DbWriteRepository: Send + Sync + 'static {
    /// Persists a provider quota exhaustion observation from derived fields.
    fn record_provider_quota_exhausted<'a>(
        &'a self,
        account_id: AccountId,
        route_band: RouteBand,
        classification: ProviderErrorClassification,
        observed_unix_seconds: u64,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>>;

    /// Persists an active-client mirror acquisition.
    fn record_active_client_acquired<'a>(
        &'a self,
        _route_band: RouteBand,
        _process_run_id: String,
        _reservation_id: ReservationId,
        _account_id: AccountId,
        _acquired_unix_seconds: u64,
        _active_pressure: u32,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async { Ok(()) })
    }

    /// Persists an active-client mirror release.
    fn record_active_client_released<'a>(
        &'a self,
        _route_band: RouteBand,
        _process_run_id: String,
        _reservation_id: ReservationId,
        _released_unix_seconds: u64,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async { Ok(()) })
    }

    /// Persists a previous-response affinity owner record.
    fn record_previous_response_affinity_owner<'a>(
        &'a self,
        _owner: PreviousResponseAffinityOwnerRecord,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async { Ok(()) })
    }

    /// Persists a Codex session account affinity.
    fn record_session_account_affinity<'a>(
        &'a self,
        _affinity: SessionAccountAffinity,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Durable write repository failure.
#[derive(Debug, Error)]
pub enum DbWriteRepositoryError {
    /// SQLite or state repository failed.
    #[error("state store unavailable while recording DB write actor command")]
    State(#[from] StateStoreError),
}

/// SQLite-backed DB write repository.
#[derive(Clone, Debug)]
pub struct SqliteDbWriteRepository {
    state: AsyncSqliteStateStore,
    #[cfg(test)]
    quota_write_completion: Option<tokio::sync::mpsc::UnboundedSender<QuotaWriteCompletion>>,
}

impl SqliteDbWriteRepository {
    /// Creates a SQLite-backed DB write repository.
    #[must_use]
    pub const fn new(state: AsyncSqliteStateStore) -> Self {
        Self {
            state,
            #[cfg(test)]
            quota_write_completion: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_quota_write_completion(
        mut self,
        sender: Option<tokio::sync::mpsc::UnboundedSender<QuotaWriteCompletion>>,
    ) -> Self {
        self.quota_write_completion = sender;
        self
    }
}

impl DbWriteRepository for SqliteDbWriteRepository {
    fn record_provider_quota_exhausted<'a>(
        &'a self,
        account_id: AccountId,
        route_band: RouteBand,
        classification: ProviderErrorClassification,
        observed_unix_seconds: u64,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async move {
            if classification == ProviderErrorClassification::AccountQuotaExhausted {
                let result = self
                    .state
                    .mark_route_band_quota_exhausted(
                        &account_id,
                        route_band.as_str(),
                        observed_unix_seconds,
                    )
                    .await;
                #[cfg(test)]
                if let Some(sender) = &self.quota_write_completion {
                    let _receiver_closed = sender.send(QuotaWriteCompletion {
                        account_id,
                        route_band,
                        result: result.clone(),
                    });
                }
                result?;
            }
            Ok(())
        })
    }

    fn record_active_client_acquired<'a>(
        &'a self,
        route_band: RouteBand,
        process_run_id: String,
        reservation_id: ReservationId,
        account_id: AccountId,
        acquired_unix_seconds: u64,
        active_pressure: u32,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async move {
            self.state
                .record_active_client_acquired(
                    route_band.as_str(),
                    &process_run_id,
                    &reservation_id,
                    &account_id,
                    acquired_unix_seconds,
                    active_pressure,
                )
                .await?;
            Ok(())
        })
    }

    fn record_active_client_released<'a>(
        &'a self,
        route_band: RouteBand,
        process_run_id: String,
        reservation_id: ReservationId,
        released_unix_seconds: u64,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async move {
            self.state
                .record_active_client_released(
                    route_band.as_str(),
                    &process_run_id,
                    &reservation_id,
                    released_unix_seconds,
                )
                .await?;
            Ok(())
        })
    }

    fn record_previous_response_affinity_owner<'a>(
        &'a self,
        owner: PreviousResponseAffinityOwnerRecord,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async move {
            self.state.write_previous_response_owner(&owner).await?;
            Ok(())
        })
    }

    fn record_session_account_affinity<'a>(
        &'a self,
        affinity: SessionAccountAffinity,
    ) -> BoxFuture<'a, Result<(), DbWriteRepositoryError>> {
        Box::pin(async move {
            self.state
                .upsert_session_account_affinity(&affinity)
                .await?;
            Ok(())
        })
    }
}
