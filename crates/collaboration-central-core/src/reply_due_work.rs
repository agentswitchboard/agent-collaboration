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
        let Some(owner) = self.reply_due_owner.get().filter(|_| has_reply_due_scope()) else {
            let _ = self.after_event().await;
            return;
        };
        self.execution.record_completion_attempt();
        if let Some(wake_id) = FleetEngine::new(self).prepare_reply_due_work().await {
            let owner = owner.clone();
            let due_batches = self.execution.due_batches().clone();
            queue_reply_due_work(move |transport| {
                #[cfg(feature = "test-support")]
                due_batches.record_reply_accepted();
                due_batches.accept_reply(wake_id, move |claim| {
                    let Some(owner) = owner.upgrade() else {
                        return;
                    };
                    #[cfg(feature = "test-support")]
                    let pending = owner.execution.record_pending_reply_drain();
                    #[cfg(feature = "test-support")]
                    let cut = owner.execution.test_faults().reply_drain_cut();
                    let apply = async move {
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
                    };
                    let task: DeferredDueTask = Box::pin(async move {
                        #[cfg(feature = "test-support")]
                        {
                            let _pending = pending;
                            let _ = futures_util::future::select(
                                std::pin::pin!(cut),
                                std::pin::pin!(apply),
                            )
                            .await;
                        }
                        #[cfg(not(feature = "test-support"))]
                        apply.await;
                    });
                    transport.run_after_reply(task);
                });
            });
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
