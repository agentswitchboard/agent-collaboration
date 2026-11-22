//! Host-lifetime ownership of unfinished Codex creates and detached prompt drains.
use codex_acp_adapter::{AcpSessionBinding, HeldBindingCheckout, UnmaterializedBindingStore};
use collaboration_protocol::CodexGeneration;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Mutex,
};

enum HeldThread {
    Ready(Box<AcpSessionBinding>),
    Busy(CodexGeneration),
}

#[derive(Default)]
pub struct UnmaterializedThreadHolder {
    threads: Mutex<BTreeMap<String, HeldThread>>,
    host_tasks: tokio_util::task::TaskTracker,
}

impl UnmaterializedThreadHolder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn contains(&self, session_id: &str) -> bool {
        self.threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .is_some_and(|thread| match thread {
                HeldThread::Ready(binding) => !binding.is_retired(),
                HeldThread::Busy(_) => true,
            })
    }

    pub async fn drain_host_tasks(&self) {
        self.host_tasks.close();
        self.host_tasks.wait().await;
    }
}

/// Clears a checked-out slot if dispatch exits before it can restore or finish it.
pub(crate) struct HeldBindingCleanup<'a> {
    holder: &'a UnmaterializedThreadHolder,
    session_id: String,
    generation: CodexGeneration,
    active: bool,
}

impl<'a> HeldBindingCleanup<'a> {
    pub(crate) fn new(holder: &'a UnmaterializedThreadHolder, binding: &AcpSessionBinding) -> Self {
        Self {
            holder,
            session_id: binding.session_id().into(),
            generation: binding.generation().clone(),
            active: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.active = false;
    }

    pub(crate) fn finish(&mut self) {
        self.holder.finish(&self.session_id, &self.generation);
        self.disarm();
    }
}

impl Drop for HeldBindingCleanup<'_> {
    fn drop(&mut self) {
        if self.active {
            self.holder.finish(&self.session_id, &self.generation);
        }
    }
}

impl UnmaterializedBindingStore for UnmaterializedThreadHolder {
    fn hold(&self, binding: AcpSessionBinding) {
        if !binding.is_unmaterialized() {
            return;
        }
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !binding.is_retired() {
            threads.insert(
                binding.session_id().to_owned(),
                HeldThread::Ready(Box::new(binding)),
            );
        }
    }

    fn checkout(&self, session_id: &str) -> HeldBindingCheckout {
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match threads.remove_entry(session_id) {
            Some((key, HeldThread::Ready(binding))) if !binding.is_retired() => {
                threads.insert(key, HeldThread::Busy(binding.generation().clone()));
                HeldBindingCheckout::Ready(binding)
            }
            Some((key, busy @ HeldThread::Busy(_))) => {
                threads.insert(key, busy);
                HeldBindingCheckout::Busy
            }
            Some((_, HeldThread::Ready(_))) | None => HeldBindingCheckout::Missing,
        }
    }

    fn restore(&self, binding: AcpSessionBinding) {
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Entry::Occupied(mut slot) = threads.entry(binding.session_id().to_owned())
            && matches!(slot.get(), HeldThread::Busy(generation) if generation == binding.generation())
        {
            if binding.is_retired() || !binding.is_unmaterialized() {
                slot.remove();
            } else {
                slot.insert(HeldThread::Ready(Box::new(binding)));
            }
        }
    }

    fn finish(&self, session_id: &str, generation: &CodexGeneration) {
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(threads.get(session_id), Some(HeldThread::Busy(owner)) if owner == generation) {
            threads.remove(session_id);
        }
    }

    fn host_tasks(&self) -> tokio_util::task::TaskTracker {
        self.host_tasks.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_drains_in_flight_codex_create_task() {
        let holder = std::sync::Arc::new(UnmaterializedThreadHolder::new());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        holder.host_tasks().spawn(async move {
            let _entered = entered_tx.send(());
            let _released = release_rx.await;
        });
        entered_rx.await.expect("create task started");
        let draining = tokio::spawn({
            let holder = std::sync::Arc::clone(&holder);
            async move { holder.drain_host_tasks().await }
        });
        tokio::task::yield_now().await;
        assert!(!draining.is_finished());
        let _released = release_tx.send(());
        draining.await.expect("create tasks drained");
    }
}
