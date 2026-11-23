//! The organization's central store: one atomic statement batch, with its alarm update.
use central_storage::{
    BatchOutcome, EngineAlarmTimes, ReadOutcome, Statement, StatementBatch, StoreFailure,
};
use std::future::Future;

#[derive(Clone, Copy, Debug, Default)]
pub struct EngineDueTimes {
    pub look_ahead_due_ms: Option<i64>,
    pub exact_due_ms: Option<i64>,
    pub approval_due_ms: Option<i64>,
}

/// The organization's central store. Its statement batch owns the alarm update.
pub trait CentralStore: Send + Sync + 'static {
    /// Runs every statement in one transaction; a failed guard or constraint rolls it all
    /// back. `AlarmUpdate::SetFromQuery` runs last in that transaction and sets the alarm.
    fn run_batch(
        &self,
        batch: StatementBatch,
    ) -> impl Future<Output = Result<BatchOutcome, StoreFailure>> + Send;
    fn read(
        &self,
        statement: Statement,
    ) -> impl Future<Output = Result<ReadOutcome, StoreFailure>> + Send;
    /// Executes the owner's checked due-list SELECT on this store.
    fn read_timed_due_rows(
        &self,
        query: crate::DueListQuery,
    ) -> impl Future<Output = Result<Vec<crate::DueRow>, StoreFailure>> + Send;
    fn engine_alarm_inputs(
        &self,
    ) -> impl Future<Output = Result<EngineAlarmTimes, StoreFailure>> + Send;
    fn engine_due_times(&self)
    -> impl Future<Output = Result<EngineDueTimes, StoreFailure>> + Send;
    /// The physical alarm actually armed, for health (P8): Cloudflare reads the
    /// Durable Object alarm, Tokio the last successfully published deadline.
    fn published_alarm(
        &self,
    ) -> impl Future<Output = Result<Option<crate::EngineTime>, StoreFailure>> + Send;
}
