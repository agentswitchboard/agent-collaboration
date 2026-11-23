//! Ordered custody of the Mac's enrollment row and encrypted refresh credential.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use router_clock::RuntimeClock;
use switchboard_machine_core_store::{MachineLocalStore, MachineLocalStoreError};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentEnd, MachineId, StoreDurability, StoredEnrollment,
};
use tokio::sync::{mpsc, oneshot};

mod enrollment_row_mapping;
mod enrollment_worker;

use enrollment_worker::{EnrollmentWorker, ReadFailure};

/// An enrollment store with its own core connection and one FIFO worker.
#[derive(Clone)]
pub struct MachineEnrollmentStore {
    requests: mpsc::UnboundedSender<EnrollmentRequest>,
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

    fn start(worker: EnrollmentWorker) -> Self {
        let (requests, pending) = mpsc::unbounded_channel();
        tokio::spawn(worker.run(pending));
        Self { requests }
    }

    async fn mutate(
        &self,
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        definite_error: CredentialStoreError,
    ) -> WriteResult {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(EnrollmentRequest::Mutate {
                expected,
                mutation,
                reply,
            })
            .map_err(|_| definite_error)?;
        result
            .await
            .unwrap_or(Err(CredentialStoreError::OutcomeNotKnown))
    }
}

impl CredentialStore for MachineEnrollmentStore {
    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .send(EnrollmentRequest::Read(reply))
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

type WriteResult = Result<ConditionalWrite, CredentialStoreError>;

enum EnrollmentRequest {
    Read(oneshot::Sender<Result<StoredEnrollment, ReadFailure>>),
    Mutate {
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        reply: oneshot::Sender<WriteResult>,
    },
}

enum EnrollmentMutation {
    Replace(EnrollmentBinding),
    End(EnrollmentEnd),
}

#[cfg(test)]
#[path = "enrollment_store_test.rs"]
mod tests;
