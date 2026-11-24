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
    EnrollmentEnd, EnrollmentKey, EnrollmentMetadata, KeyedEnrollment, MachineId, StoreDurability,
    StoredEnrollment,
};
use tokio::sync::{mpsc, oneshot, watch};

mod enrollment_row_mapping;
mod enrollment_worker;
mod worker_lifecycle;
#[cfg(all(feature = "test-support", debug_assertions))]
pub mod test_support {
    pub use codex_router_secret_store::keychain_data_key::{KeychainAccess, KeychainAccessError};
}

use enrollment_worker::{EnrollmentWorker, ReadFailure};
use worker_lifecycle::WorkerLifecycle;

pub use enrollment_worker::EnrollmentWorkerExit;
pub use worker_lifecycle::{EnrollmentJoin, EnrollmentObservationError};

/// An enrollment store with its own core connection and one FIFO worker.
#[derive(Clone)]
pub struct MachineEnrollmentStore {
    requests: mpsc::UnboundedSender<EnrollmentRequest>,
    shutdown: watch::Sender<bool>,
    enrollment_changes: watch::Sender<u64>,
    worker: WorkerLifecycle,
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
        let worker = WorkerLifecycle::start(worker, pending, closing, enrollment_changes.clone());
        Self {
            requests,
            shutdown,
            enrollment_changes,
            worker,
        }
    }

    #[cfg(test)]
    fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.worker.abort_handle()
    }

    /// Tracks enrollment changes from this store's worker.
    pub fn subscribe_enrollment_changes(&self) -> watch::Receiver<u64> {
        self.enrollment_changes.subscribe()
    }

    /// Stops admitting requests immediately. Repeated calls have no effect.
    pub fn request_close(&self) {
        self.shutdown.send_if_modified(|closing| {
            if *closing {
                return false;
            }
            *closing = true;
            true
        });
    }

    /// Observes the worker task's exit, even while its resources are still
    /// being reaped. Returns an observation error if the publisher is lost first.
    pub async fn worker_exit(&self) -> Result<EnrollmentWorkerExit, EnrollmentObservationError> {
        self.worker.worker_exit().await
    }

    /// Waits for the worker and its resources to end without requesting close.
    /// The receipt survives cancellation of earlier waits. An observation error
    /// leaves the store outstanding with ownership unconfirmed.
    pub async fn join(&self) -> Result<EnrollmentJoin, EnrollmentObservationError> {
        self.worker.join().await
    }

    /// Stops admission and waits for the active operation and connection to settle.
    pub async fn close(&self) -> Result<(), EnrollmentStoreCloseError> {
        self.request_close();
        self.join()
            .await
            .map_err(|_| EnrollmentStoreCloseError::WorkerStopped)?
            .outcome
    }

    fn enqueue_request(
        &self,
        request: EnrollmentRequest,
        definite_error: CredentialStoreError,
    ) -> Result<(), CredentialStoreError> {
        // Keep the watch's read guard through the send so request_close cannot
        // set the stop flag between the check and admission to the queue.
        let closing = self.shutdown.borrow();
        if *closing {
            return Err(definite_error);
        }
        self.requests.send(request).map_err(|_| definite_error)
    }

    async fn read_enrollment(
        &self,
        key: Option<EnrollmentKey>,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        let (reply, result) = oneshot::channel();
        self.enqueue_request(
            EnrollmentRequest::Read { key, reply },
            CredentialStoreError::Read,
        )?;
        result
            .await
            .map_err(|_| CredentialStoreError::Read)?
            .map_err(|_| CredentialStoreError::Read)
    }

    async fn mutate(
        &self,
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        definite_error: CredentialStoreError,
    ) -> WriteResult {
        let (reply, result) = oneshot::channel();
        let stage = Arc::new(MutationStage::default());
        self.enqueue_request(
            EnrollmentRequest::Mutate {
                expected,
                mutation,
                reply,
                stage: Arc::clone(&stage),
            },
            definite_error,
        )?;
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
        match self.read_enrollment(None).await? {
            KeyedEnrollment::Snapshot(stored) => Ok(stored),
            KeyedEnrollment::DifferentEnrollment => Err(CredentialStoreError::Read),
        }
    }

    async fn read_for_key(
        &self,
        requested: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        self.read_enrollment(Some(requested.clone())).await
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        let (reply, result) = oneshot::channel();
        self.enqueue_request(
            EnrollmentRequest::ReadMetadata(reply),
            CredentialStoreError::Read,
        )?;
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

/// A failure reported by the enrollment store when reaping its worker and connection.
#[derive(Clone, Debug, thiserror::Error)]
pub enum EnrollmentStoreCloseError {
    #[error("the enrollment worker stopped before closing its connection")]
    WorkerStopped,
    #[error("could not close the enrollment database connection")]
    Connection(#[source] Arc<MachineLocalStoreError>),
}

type WriteResult = Result<ConditionalWrite, CredentialStoreError>;

enum EnrollmentRequest {
    Read {
        key: Option<EnrollmentKey>,
        reply: oneshot::Sender<Result<KeyedEnrollment, ReadFailure>>,
    },
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
