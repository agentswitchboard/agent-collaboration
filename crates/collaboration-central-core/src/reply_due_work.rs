//! Project one event's due work to its shared transport reply scope.
use crate::WorkspaceRuntime;
use fleet_engine::FleetEngine;
use fleet_runtime::{DeferredDueTask, has_reply_due_scope, queue_reply_due_work};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use std::sync::Arc;

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub fn enable_reply_due_work(self: &Arc<Self>) {
        let _ = self.reply_due_owner.set(Arc::downgrade(self));
    }

    pub(crate) async fn after_request_event(&self) {
        let Some(owner) = self
            .reply_due_owner
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            let _ = self.after_event().await;
            return;
        };
        if !has_reply_due_scope() {
            let _ = self.after_event().await;
            return;
        }
        self.execution.record_completion_attempt();
        if let Some(claim) = FleetEngine::new(self).prepare_reply_due_work().await {
            #[cfg(feature = "test-support")]
            let pending = self.execution.record_pending_reply_drain();
            let work: DeferredDueTask = Box::pin(async move {
                #[cfg(feature = "test-support")]
                let _pending = pending;
                #[cfg(feature = "test-support")]
                {
                    if owner.runtime().test_faults().take_deferred_due_work_drop() {
                        return;
                    }
                    owner
                        .runtime()
                        .pause_points()
                        .reached_central(fleet_runtime::PausePoint::BeforeDeferredDueWork)
                        .await;
                }
                FleetEngine::new(owner.as_ref())
                    .complete_reply_due_work(claim)
                    .await;
            });
            queue_reply_due_work(work);
        }
    }

    /// Yields only between passes, after all of the current pass's writes settle.
    pub fn with_due_batch_yield(
        mut self,
        yield_between_passes: Arc<dyn Fn() -> fleet_runtime::DueBatchYield + Send + Sync>,
    ) -> Self {
        self.yield_due_batches = yield_between_passes;
        self
    }
}
