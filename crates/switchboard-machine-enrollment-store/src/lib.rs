//! Ordered custody of the Mac's enrollment row and encrypted refresh credential.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use router_clock::RuntimeClock;
use switchboard_machine_core_store::{MachineLocalStore, MachineLocalStoreError};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentEnd, EnrollmentMetadata, MachineId, StoreDurability, StoredEnrollment,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

mod enrollment_row_mapping;
mod enrollment_worker;
#[cfg(all(feature = "test-support", debug_assertions))]
pub mod test_support {
    pub use codex_router_secret_store::keychain_data_key::{KeychainAccess, KeychainAccessError};
}

use enrollment_worker::{EnrollmentWorker, ReadFailure};

/// An enrollment store with its own core connection and one FIFO worker.
#[derive(Clone)]
pub struct MachineEnrollmentStore {
    requests: mpsc::UnboundedSender<EnrollmentRequest>,
    shutdown: watch::Sender<bool>,
    enrollment_changes: watch::Sender<u64>,
    worker: Arc<Mutex<WorkerCompletion>>,
}

impl MachineEnrollmentStore {
    /// Opens a dedicated connection to the machine's core database.
    pub async fn open(
        database: &Path,
        secret_root: PathBuf,
        machine_id: MachineId,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Result<Self, MachineLocalStoreError> {
        let database = MachineLocalStore::open(database).await?;
        Ok(Self::start(EnrollmentWorker::new(
            database,
            secret_root,
            machine_id,
            clock,
        )))
    }

    /// Opens a real fixture database with a caller-owned Keychain test double.
    #[cfg(all(feature = "test-support", debug_assertions))]
    pub async fn open_with_test_keychain(
        database: &Path,
        secret_root: PathBuf,
        machine_id: MachineId,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        keychain: Arc<dyn test_support::KeychainAccess>,
    ) -> Result<Self, MachineLocalStoreError> {
        let database = MachineLocalStore::open(database).await?;
        let worker =
            EnrollmentWorker::new(database, secret_root, machine_id, clock).with_keychain(keychain);
        Ok(Self::start(worker))
    }

    fn start(worker: EnrollmentWorker) -> Self {
        let (requests, pending) = mpsc::unbounded_channel();
        let (shutdown, closing) = watch::channel(false);
        let (enrollment_changes, _) = watch::channel(0);
        let task = tokio::spawn(worker.run(pending, closing, enrollment_changes.clone()));
        let worker = Arc::new(Mutex::new(WorkerCompletion::Running(task)));
        Self {
            requests,
            shutdown,
            enrollment_changes,
            worker,
        }
    }

    #[cfg(test)]
    fn abort_handle(&self) -> tokio::task::AbortHandle {
        let worker = self.worker.try_lock().expect("uncontended fixture worker");
        match &*worker {
            WorkerCompletion::Running(task) => task.abort_handle(),
            WorkerCompletion::Closed(_) => panic!("fixture worker already closed"),
        }
    }

    /// Tracks enrollment changes from this store's worker.
    pub fn subscribe_enrollment_changes(&self) -> watch::Receiver<u64> {
        self.enrollment_changes.subscribe()
    }

    /// Stops admission and waits for the active operation and connection to settle.
    pub async fn close(&self) -> Result<(), EnrollmentStoreCloseError> {
        self.shutdown.send_replace(true);
        let mut completion = self.worker.lock().await;
        let result = match &mut *completion {
            WorkerCompletion::Running(task) => match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(EnrollmentStoreCloseError::Connection(Arc::new(error))),
                Err(_) => Err(EnrollmentStoreCloseError::WorkerStopped),
            },
            WorkerCompletion::Closed(result) => return result.clone(),
        };
        *completion = WorkerCompletion::Closed(result.clone());
        result
    }

    async fn mutate(
        &self,
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        definite_error: CredentialStoreError,
    ) -> WriteResult {
        if *self.shutdown.borrow() {
            return Err(definite_error);
        }
        let (reply, result) = oneshot::channel();
        let stage = Arc::new(MutationStage::default());
        self.requests
            .send(EnrollmentRequest::Mutate {
                expected,
                mutation,
                reply,
                stage: Arc::clone(&stage),
            })
            .map_err(|_| definite_error)?;
        result
            .await
            .unwrap_or_else(|_| Err(stage.missing_reply(definite_error)))
    }
}

impl CredentialStore for MachineEnrollmentStore {
    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        if *self.shutdown.borrow() {
            return Err(CredentialStoreError::Read);
        }
        let (reply, result) = oneshot::channel();
        self.requests
            .send(EnrollmentRequest::Read(reply))
            .map_err(|_| CredentialStoreError::Read)?;
        result
            .await
            .map_err(|_| CredentialStoreError::Read)?
            .map_err(|_| CredentialStoreError::Read)
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        if *self.shutdown.borrow() {
            return Err(CredentialStoreError::Read);
        }
        let (reply, result) = oneshot::channel();
        self.requests
            .send(EnrollmentRequest::ReadMetadata(reply))
            .map_err(|_| CredentialStoreError::Read)?;
        result
            .await
            .map_err(|_| CredentialStoreError::Read)?
            .map_err(|_| CredentialStoreError::Read)
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> WriteResult {
        self.mutate(
            expected,
            EnrollmentMutation::Replace(binding.clone()),
            CredentialStoreError::Replace,
        )
        .await
    }

    async fn remove(&self, expected: BindingGeneration, why: EnrollmentEnd) -> WriteResult {
        self.mutate(
            expected,
            EnrollmentMutation::End(why),
            CredentialStoreError::Remove,
        )
        .await
    }

    async fn mark_unusable(&self, expected: BindingGeneration, why: EnrollmentEnd) -> WriteResult {
        self.remove(expected, why).await
    }
}

/// An enrollment worker or its dedicated database connection could not close.
#[derive(Clone, Debug, thiserror::Error)]
pub enum EnrollmentStoreCloseError {
    #[error("the enrollment worker stopped before closing its connection")]
    WorkerStopped,
    #[error("could not close the enrollment database connection")]
    Connection(#[source] Arc<MachineLocalStoreError>),
}

enum WorkerCompletion {
    Running(JoinHandle<Result<(), MachineLocalStoreError>>),
    Closed(Result<(), EnrollmentStoreCloseError>),
}

type WriteResult = Result<ConditionalWrite, CredentialStoreError>;

enum EnrollmentRequest {
    Read(oneshot::Sender<Result<StoredEnrollment, ReadFailure>>),
    ReadMetadata(oneshot::Sender<Result<EnrollmentMetadata, ReadFailure>>),
    Mutate {
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        reply: oneshot::Sender<WriteResult>,
        stage: Arc<MutationStage>,
    },
}

#[derive(Default)]
struct MutationStage {
    transaction_started: AtomicBool,
}

impl MutationStage {
    fn begin_transaction(&self) {
        self.transaction_started.store(true, Ordering::Release);
    }

    fn missing_reply(&self, definite_error: CredentialStoreError) -> CredentialStoreError {
        if self.transaction_started.load(Ordering::Acquire) {
            CredentialStoreError::OutcomeNotKnown
        } else {
            definite_error
        }
    }
}

enum EnrollmentMutation {
    Replace(EnrollmentBinding),
    End(EnrollmentEnd),
}

#[cfg(test)]
#[path = "enrollment_store_test.rs"]
mod tests;
