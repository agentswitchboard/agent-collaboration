use tokio::sync::oneshot;

pub(super) struct InboxCommitPause {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl InboxCommitPause {
    pub(super) fn new() -> (Self, HeldInboxCommit) {
        let (reached, entering) = oneshot::channel();
        let (leaving, release) = oneshot::channel();
        (
            Self { reached, release },
            HeldInboxCommit { entering, leaving },
        )
    }

    pub(super) async fn wait_for_release(self) {
        let _ = self.reached.send(());
        let _ = self.release.await;
    }
}

/// Controls a transaction paused after inserting a delivery and before commit.
pub struct HeldInboxCommit {
    entering: oneshot::Receiver<()>,
    leaving: oneshot::Sender<()>,
}

impl HeldInboxCommit {
    pub async fn until_held(&mut self) -> Result<(), oneshot::error::RecvError> {
        (&mut self.entering).await
    }

    pub fn release(self) {
        let _ = self.leaving.send(());
    }
}
