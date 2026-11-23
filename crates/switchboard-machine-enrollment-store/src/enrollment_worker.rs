use std::{path::PathBuf, sync::Arc};

use codex_router_secret_store::{
    enrollment_row_sealer::{EnrollmentRowCredential, EnrollmentRowSealer},
    model::SecretStoreError,
};
use router_clock::RuntimeClock;
use switchboard_machine_core_store::{
    EnrollmentRowChange, EnrollmentRowCorruption, EnrollmentRowStoreError, EnrollmentRowWrite,
    MachineLocalStore, MachineLocalStoreError, StoredEnrollmentRow,
};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, EnrollmentMetadata, MachineId, RefreshCredential,
    SecretValue, StoredEnrollment,
};
use tokio::sync::{mpsc, watch};

use crate::{
    EnrollmentMutation, EnrollmentRequest, MutationStage, WriteResult,
    enrollment_row_mapping::{EnrollmentSnapshot, enrollment_metadata, seal_context, stored_end},
};

pub(crate) struct EnrollmentWorker {
    store: MachineLocalStore,
    secret_root: PathBuf,
    machine_id: MachineId,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    #[cfg(test)]
    test_dependencies: Option<TestDependencies>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadFailure {
    #[error(transparent)]
    Corrupt(#[from] EnrollmentRowCorruption),
    #[error("enrollment row storage is unavailable")]
    StorageUnavailable,
    #[error("enrollment row key is unavailable")]
    KeyUnavailable,
    #[error("enrollment row credential could not be opened")]
    SealedBytesUnreadable,
}

impl From<EnrollmentRowStoreError> for ReadFailure {
    fn from(error: EnrollmentRowStoreError) -> Self {
        match error {
            EnrollmentRowStoreError::Corrupt(reason) => Self::Corrupt(reason),
            _ => Self::StorageUnavailable,
        }
    }
}

impl EnrollmentWorker {
    pub(crate) fn new(
        store: MachineLocalStore,
        secret_root: PathBuf,
        machine_id: MachineId,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
    ) -> Self {
        Self {
            store,
            secret_root,
            machine_id,
            clock,
            #[cfg(test)]
            test_dependencies: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_test_dependencies(
        &mut self,
        keychain: Arc<dyn codex_router_secret_store::keychain_data_key::KeychainAccess>,
        faults: Arc<TestWriteFaults>,
    ) {
        self.test_dependencies = Some(TestDependencies { keychain, faults });
    }

    pub(crate) async fn run(
        mut self,
        mut requests: mpsc::UnboundedReceiver<EnrollmentRequest>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), MachineLocalStoreError> {
        while !*shutdown.borrow_and_update() {
            let request = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                request = requests.recv() => request,
            };
            let Some(request) = request else {
                break;
            };
            if *shutdown.borrow() {
                break;
            }
            self.handle(request).await;
        }
        requests.close();
        drop(requests);
        self.store.close().await
    }

    async fn handle(&mut self, request: EnrollmentRequest) {
        match request {
            EnrollmentRequest::Read(reply) if !reply.is_closed() => {
                let _ = reply.send(self.read().await);
            }
            EnrollmentRequest::ReadMetadata(reply) if !reply.is_closed() => {
                let _ = reply.send(self.read_metadata().await);
            }
            EnrollmentRequest::Mutate {
                expected,
                mutation,
                reply,
                stage,
            } if !reply.is_closed() => {
                let _ = reply.send(self.mutate(expected, mutation, &stage).await);
            }
            _ => {}
        }
    }

    async fn read_metadata(&mut self) -> Result<EnrollmentMetadata, ReadFailure> {
        let row = self
            .store
            .read_enrollment_metadata(&self.machine_id)
            .await?;
        Ok(enrollment_metadata(row, &self.machine_id)?)
    }

    async fn read(&mut self) -> Result<StoredEnrollment, ReadFailure> {
        let snapshot = self.snapshot().await?;
        match snapshot {
            EnrollmentSnapshot::Empty => Ok(StoredEnrollment::Empty),
            EnrollmentSnapshot::Ended {
                generation,
                why,
                key,
            } => Ok(StoredEnrollment::Ended {
                generation,
                why,
                key,
            }),
            EnrollmentSnapshot::Active {
                key,
                deployment,
                generation,
                sealed,
            } => {
                let row_context = seal_context(&key, &deployment, generation)
                    .map_err(|_| ReadFailure::Corrupt(EnrollmentRowCorruption::Deployment))?;
                let sealer = self
                    .acquire_key(true)
                    .await
                    .map_err(|_| ReadFailure::KeyUnavailable)?;
                let credential = sealer
                    .open(&row_context, &sealed)
                    .map_err(|_| ReadFailure::SealedBytesUnreadable)?;
                let refresh_token = SecretValue::copy_from_utf8(credential.expose_bytes())
                    .ok_or(ReadFailure::SealedBytesUnreadable)?;
                Ok(StoredEnrollment::Active(EnrollmentBinding {
                    generation,
                    context: EnrollmentContext {
                        machine_id: key.machine_id,
                        organization_id: key.organization_id,
                        deployment,
                    },
                    credential: RefreshCredential {
                        enrollment_id: key.enrollment_id,
                        refresh_token,
                    },
                }))
            }
        }
    }

    async fn mutate(
        &mut self,
        expected: BindingGeneration,
        mutation: EnrollmentMutation,
        stage: &MutationStage,
    ) -> WriteResult {
        let (decision, definite_error) = match mutation {
            EnrollmentMutation::Replace(binding) => {
                let prepared = self.prepare_replacement(expected, &binding).await;
                #[cfg(test)]
                if prepared.is_some()
                    && let Some(test) = &self.test_dependencies
                {
                    test.faults.at_prepared_replacement().await;
                }
                (
                    MutationDecision::Replace(prepared),
                    CredentialStoreError::Replace,
                )
            }
            EnrollmentMutation::End(why) => {
                (MutationDecision::End(why), CredentialStoreError::Remove)
            }
        };

        let expected_sql = i64::try_from(expected.get()).map_err(|_| definite_error)?;
        let timestamp = self.clock.now().unix_millis();
        let machine_id = &self.machine_id;
        let write = async {
            // The caller can distinguish reply loss before the first poll of
            // this transaction from loss during a possible durable write.
            stage.begin_transaction();
            match decision {
                MutationDecision::Replace(prepared) => {
                    self.store
                        .write_enrollment_row(machine_id, expected_sql, timestamp, |row| {
                            let snapshot = match EnrollmentSnapshot::from_row(row, machine_id) {
                                Ok(snapshot) => snapshot,
                                Err(corrupt) => return EnrollmentRowChange::Corrupt(corrupt),
                            };
                            match prepared {
                                Some(replacement) if snapshot == replacement.witness => {
                                    EnrollmentRowChange::Write(replacement.row)
                                }
                                _ => EnrollmentRowChange::Abort(()),
                            }
                        })
                        .await
                }
                MutationDecision::End(why) => {
                    self.store
                        .write_enrollment_ending(machine_id, expected_sql, timestamp, |row| {
                            match enrollment_metadata(row, machine_id) {
                                Ok(metadata) => end_row(metadata, why),
                                Err(corrupt) => EnrollmentRowChange::Corrupt(corrupt),
                            }
                        })
                        .await
                }
            }
        };
        #[cfg(not(test))]
        let outcome = write.await;
        #[cfg(test)]
        let outcome = if let Some(test) = &self.test_dependencies {
            test.faults.apply(write).await
        } else {
            write.await
        };
        self.finish_mutation(expected, outcome, definite_error)
            .await
    }

    async fn finish_mutation(
        &mut self,
        expected: BindingGeneration,
        outcome: Result<EnrollmentRowWrite<()>, EnrollmentRowStoreError>,
        definite_error: CredentialStoreError,
    ) -> WriteResult {
        match outcome {
            Ok(EnrollmentRowWrite::Applied) => Ok(ConditionalWrite::Applied),
            Ok(EnrollmentRowWrite::Superseded) => Ok(ConditionalWrite::Superseded),
            Ok(EnrollmentRowWrite::Aborted(())) => Err(definite_error),
            Err(EnrollmentRowStoreError::Commit(_)) => {
                #[cfg(test)]
                if let Some(test) = &self.test_dependencies {
                    test.faults
                        .probes
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                let observed = if definite_error == CredentialStoreError::Remove {
                    self.store
                        .probe_enrollment_ending(&self.machine_id)
                        .await
                        .ok()
                        .and_then(|row| enrollment_metadata(row, &self.machine_id).ok())
                        .map(|metadata| match metadata {
                            EnrollmentMetadata::Empty => BindingGeneration::EMPTY,
                            EnrollmentMetadata::Active { generation, .. }
                            | EnrollmentMetadata::Ended { generation, .. } => generation,
                        })
                } else {
                    self.store
                        .probe_enrollment_commit(&self.machine_id)
                        .await
                        .ok()
                        .and_then(|row| EnrollmentSnapshot::from_row(row, &self.machine_id).ok())
                        .map(|snapshot| snapshot.generation())
                };
                if observed == Some(expected) {
                    Err(definite_error)
                } else {
                    Err(CredentialStoreError::OutcomeNotKnown)
                }
            }
            Err(_) => Err(definite_error),
        }
    }

    async fn prepare_replacement(
        &mut self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> Option<Box<PreparedReplacement>> {
        let current = self.snapshot().await.ok()?;
        if current.generation() != expected
            || expected.binding_successor() != Some(binding.generation)
            || self.machine_id != binding.context.machine_id
        {
            return None;
        }
        #[cfg(test)]
        if let Some(test) = &self.test_dependencies {
            test.faults.at_replacement_witness().await;
        }
        let sealed_bytes_present = matches!(current, EnrollmentSnapshot::Active { .. });
        let sealer = match self.acquire_key(true).await {
            Ok(sealer) => sealer,
            Err(SecretStoreError::KeyMissing) if !sealed_bytes_present => {
                let fresh = self.snapshot().await.ok()?;
                if fresh.generation() != expected
                    || matches!(fresh, EnrollmentSnapshot::Active { .. })
                {
                    return None;
                }
                self.acquire_key(false).await.ok()?
            }
            Err(_) => return None,
        };
        if let EnrollmentSnapshot::Active {
            key,
            deployment,
            generation,
            sealed,
        } = &current
        {
            let predecessor = seal_context(key, deployment, *generation).ok()?;
            sealer.open(&predecessor, sealed).ok()?;
        }
        let context = seal_context(
            &binding.key(),
            &binding.context.deployment,
            binding.generation,
        )
        .ok()?;
        let credential =
            EnrollmentRowCredential::copy_from(binding.credential.refresh_token.expose_secret())?;
        let sealed = sealer.seal(&context, &credential).ok()?;
        let row = StoredEnrollmentRow::Active {
            generation: i64::try_from(binding.generation.get()).ok()?,
            organization_id: binding.context.organization_id.to_string(),
            deployment: binding.context.deployment.origin().as_str().to_owned(),
            enrollment_id: binding.credential.enrollment_id.to_string(),
            sealed,
        };
        Some(Box::new(PreparedReplacement {
            witness: current,
            row,
        }))
    }

    async fn snapshot(&mut self) -> Result<EnrollmentSnapshot, ReadFailure> {
        let row = self.store.read_enrollment_row(&self.machine_id).await?;
        Ok(EnrollmentSnapshot::from_row(row, &self.machine_id)?)
    }

    async fn acquire_key(
        &self,
        sealed_bytes_present: bool,
    ) -> Result<EnrollmentRowSealer, SecretStoreError> {
        let root = self.secret_root.clone();
        #[cfg(test)]
        let keychain = self
            .test_dependencies
            .as_ref()
            .map(|test| Arc::clone(&test.keychain));
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(keychain) = keychain {
                return EnrollmentRowSealer::acquire_with_keychain(
                    &root,
                    sealed_bytes_present,
                    keychain.as_ref(),
                );
            }
            EnrollmentRowSealer::acquire(&root, sealed_bytes_present)
        })
        .await
        .map_err(|_| SecretStoreError::KeyUnavailable)?
    }
}

struct PreparedReplacement {
    witness: EnrollmentSnapshot,
    row: StoredEnrollmentRow,
}

enum MutationDecision {
    Replace(Option<Box<PreparedReplacement>>),
    End(EnrollmentEnd),
}

fn end_row(metadata: EnrollmentMetadata, why: EnrollmentEnd) -> EnrollmentRowChange<()> {
    let EnrollmentMetadata::Active {
        key, generation, ..
    } = metadata
    else {
        return EnrollmentRowChange::Abort(());
    };
    let Some(generation) = generation
        .successor()
        .and_then(|next| i64::try_from(next.get()).ok())
    else {
        return EnrollmentRowChange::Abort(());
    };
    EnrollmentRowChange::Write(StoredEnrollmentRow::Ended {
        generation,
        organization_id: key.organization_id.to_string(),
        enrollment_id: key.enrollment_id.to_string(),
        why: stored_end(why),
    })
}

#[cfg(test)]
#[path = "enrollment_worker_test_faults.rs"]
mod test_faults;

#[cfg(test)]
pub(crate) use test_faults::{TestDependencies, TestWriteFault, TestWriteFaults};
