//! The host cancels and joins these tasks before its organization stores close.
#[derive(Clone, Default)]
pub(crate) struct WorkspaceBackgroundTasks {
    tasks: tokio_util::task::TaskTracker,
    stop: tokio_util::sync::CancellationToken,
}

impl WorkspaceBackgroundTasks {
    pub(crate) fn schedule(&self, completion: fleet_runtime::DeferredDueTask) {
        let stop = self.stop.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stop.cancelled() => {}
                () = completion => {}
            }
        });
    }

    pub(super) async fn close(&self) {
        self.stop.cancel();
        self.tasks.close();
        self.tasks.wait().await;
    }
}
