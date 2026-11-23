use std::{path::PathBuf, sync::Arc};

use codex_router_secret_store::{
    enrollment_row_sealer::{EnrollmentRowCredential, EnrollmentRowSealer},
    model::SecretStoreError,
};
use router_clock::RuntimeClock;
use switchboard_machine_core_store::{
    EnrollmentRowChange, EnrollmentRowCorruption, EnrollmentRowStoreError, EnrollmentRowWrite,
    MachineLocalStore, StoredEnrollmentRow,
};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, MachineId, RefreshCredential, SecretValue, StoredEnrollment,
};
use tokio::sync::mpsc;

use crate::{
    EnrollmentMutation, EnrollmentRequest, WriteResult,
    enrollment_row_mapping::{EnrollmentSnapshot, seal_context, stored_end},
};

pub(crate) struct EnrollmentWorker {
    store: MachineLocalStore,
    secret_root: PathBuf,
    machine_id: MachineId,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
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
        }
    }

    pub(crate) async fn run(mut self, mut requests: mpsc::UnboundedReceiver<EnrollmentRequest>) {
        while let Some(request) = requests.recv().await {
            match request {
                EnrollmentRequest::Read(reply) if !reply.is_closed() => {
                    let _ = reply.send(self.read().await);
                }
                EnrollmentRequest::Mutate {
                    expected,
                    mutation,
                    reply,
                } if !reply.is_closed() => {
                    let _ = reply.send(self.mutate(expected, mutation).await);
                }
                _ => {}
            }
        }
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
    ) -> WriteResult {
        let (decision, definite_error) = match mutation {
            EnrollmentMutation::Replace(binding) => {
                let prepared = self.prepare_replacement(expected, &binding).await;
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
        let outcome = self
            .store
            .write_enrollment_row(machine_id, expected_sql, timestamp, |row| {
                let snapshot = match EnrollmentSnapshot::from_row(row, machine_id) {
                    Ok(snapshot) => snapshot,
                    Err(corrupt) => return EnrollmentRowChange::Corrupt(corrupt),
                };
                match decision {
                    MutationDecision::Replace(Some(replacement)) => {
                        EnrollmentRowChange::Write(replacement)
                    }
                    MutationDecision::Replace(None) => EnrollmentRowChange::Abort(()),
                    MutationDecision::End(why) => end_row(snapshot, why),
                }
            })
            .await;
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
                let observed = self
                    .store
                    .probe_enrollment_commit(&self.machine_id)
                    .await
                    .ok()
                    .and_then(|row| EnrollmentSnapshot::from_row(row, &self.machine_id).ok());
                if observed.is_some_and(|snapshot| snapshot.generation() == expected) {
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
    ) -> Option<StoredEnrollmentRow> {
        let current = self.snapshot().await.ok()?;
        if current.generation() != expected
            || expected.binding_successor() != Some(binding.generation)
            || self.machine_id != binding.context.machine_id
        {
            return None;
        }
        let sealed_bytes_present = matches!(current, EnrollmentSnapshot::Active { .. });
        let sealer = self.acquire_key(sealed_bytes_present).await.ok()?;
        let context = seal_context(
            &binding.key(),
            &binding.context.deployment,
            binding.generation,
        )
        .ok()?;
        let credential =
            EnrollmentRowCredential::copy_from(binding.credential.refresh_token.expose_secret())?;
        let sealed = sealer.seal(&context, &credential).ok()?;
        Some(StoredEnrollmentRow::Active {
            generation: i64::try_from(binding.generation.get()).ok()?,
            organization_id: binding.context.organization_id.to_string(),
            deployment: binding.context.deployment.origin().as_str().to_owned(),
            enrollment_id: binding.credential.enrollment_id.to_string(),
            sealed,
        })
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
        tokio::task::spawn_blocking(move || {
            EnrollmentRowSealer::acquire(&root, sealed_bytes_present)
        })
        .await
        .map_err(|_| SecretStoreError::KeyUnavailable)?
    }
}

enum MutationDecision {
    Replace(Option<StoredEnrollmentRow>),
    End(EnrollmentEnd),
}

fn end_row(snapshot: EnrollmentSnapshot, why: EnrollmentEnd) -> EnrollmentRowChange<()> {
    let EnrollmentSnapshot::Active {
        key, generation, ..
    } = snapshot
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
