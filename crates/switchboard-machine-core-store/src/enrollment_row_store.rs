//! Immediate SQLite operations on the existing machine identity row.

use sqlite_storage_maintenance::{CommitStageError, TransactionStart};
use sqlx::SqliteConnection;
use workspace_ids::MachineId;

use crate::{EnrollmentRowCorruption, MachineLocalStore, StoredEnrollmentRow};

/// Failures before commit are distinct from a commit attempt with an uncertain outcome.
#[derive(Debug, thiserror::Error)]
pub enum EnrollmentRowStoreError {
    #[error(transparent)]
    Corrupt(#[from] EnrollmentRowCorruption),
    #[error("enrollment row database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("enrollment row commit failed")]
    Commit(#[source] sqlx::Error),
    #[error("enrollment write does not contain the next valid row")]
    InvalidTransition,
}

/// A pure decision about a decoded row under the write lock.
pub enum EnrollmentRowChange<Abort> {
    /// The stored row failed domain validation. This takes precedence over generation mismatch.
    Corrupt(EnrollmentRowCorruption),
    /// The operation cannot proceed on this row at the expected generation.
    Abort(Abort),
    /// A successor row to persist if the generation matches.
    Write(StoredEnrollmentRow),
}

/// The result of an immediate transaction that did not report a database error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnrollmentRowWrite<Abort> {
    Applied,
    Superseded,
    Aborted(Abort),
}

impl MachineLocalStore {
    /// Reads a coherent SQLite snapshot without reserving the writer.
    pub async fn read_enrollment_row(
        &mut self,
        machine_id: &MachineId,
    ) -> Result<StoredEnrollmentRow, EnrollmentRowStoreError> {
        fetch_enrollment_row(&mut self.connection, machine_id).await
    }

    /// Takes an immediate snapshot to settle the generation after an issued COMMIT failed.
    pub async fn probe_enrollment_commit(
        &mut self,
        machine_id: &MachineId,
    ) -> Result<StoredEnrollmentRow, EnrollmentRowStoreError> {
        let mut transaction = self.transaction(TransactionStart::Immediate).await?;
        let row = fetch_enrollment_row(&mut transaction, machine_id).await?;
        transaction.commit().await?;
        Ok(row)
    }

    /// Updates only the named machine at the supplied generation.
    ///
    /// The decision must finish synchronously. It may validate stored metadata
    /// or propose a replacement, with all external work finished before entry.
    pub async fn write_enrollment_row<Abort>(
        &mut self,
        machine_id: &MachineId,
        expected: i64,
        now_ms: i64,
        decide: impl FnOnce(StoredEnrollmentRow) -> EnrollmentRowChange<Abort> + Send,
    ) -> Result<EnrollmentRowWrite<Abort>, EnrollmentRowStoreError> {
        let mut transaction = self.transaction(TransactionStart::Immediate).await?;
        let row = fetch_enrollment_row(&mut transaction, machine_id).await?;
        let current_generation = row.generation();
        let decision = decide(row);
        if let EnrollmentRowChange::Corrupt(reason) = decision {
            return Err(reason.into());
        }
        if current_generation != expected {
            transaction.rollback().await?;
            return Ok(EnrollmentRowWrite::Superseded);
        }
        let replacement = match decision {
            EnrollmentRowChange::Write(row) => row,
            EnrollmentRowChange::Abort(reason) => {
                transaction.rollback().await?;
                return Ok(EnrollmentRowWrite::Aborted(reason));
            }
            EnrollmentRowChange::Corrupt(reason) => return Err(reason.into()),
        };
        let generation = replacement.generation();
        if Some(generation) != expected.checked_add(1) {
            return Err(EnrollmentRowStoreError::InvalidTransition);
        }
        let (json, sealed) = replacement
            .to_columns()
            .map_err(|_| EnrollmentRowStoreError::InvalidTransition)?;
        let machine_id = machine_id.as_str();
        let result = sqlx::query!(
            "UPDATE machine_identity
                SET enrollment_generation = ?, enrollment_json = ?, enrollment_sealed = ?, updated_at = ?
                WHERE machine_id = ? AND enrollment_generation = ?",
            generation,
            json,
            sealed,
            now_ms,
            machine_id,
            expected,
        )
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(EnrollmentRowStoreError::InvalidTransition);
        }
        transaction
            .commit_with_stage()
            .await
            .map_err(|error| match error {
                CommitStageError::BeforeCommit(source) => EnrollmentRowStoreError::Database(source),
                CommitStageError::CommitIssued(source) => EnrollmentRowStoreError::Commit(source),
            })?;
        Ok(EnrollmentRowWrite::Applied)
    }
}

async fn fetch_enrollment_row(
    connection: &mut SqliteConnection,
    machine_id: &MachineId,
) -> Result<StoredEnrollmentRow, EnrollmentRowStoreError> {
    let rows = sqlx::query!(
        r#"SELECT CAST(machine_id AS BLOB) AS "machine_id!: Vec<u8>",
                  CASE WHEN typeof(enrollment_generation) = 'integer' THEN enrollment_generation END
                      AS "generation?: i64",
                  CAST(enrollment_json AS BLOB) AS "metadata?: Vec<u8>",
                  CAST(enrollment_sealed AS BLOB) AS "sealed?: Vec<u8>",
                  (typeof(machine_id) = 'text'
                   AND typeof(enrollment_json) IN ('text', 'null')
                   AND typeof(enrollment_sealed) IN ('blob', 'null')) AS "valid_storage!: bool"
           FROM machine_identity LIMIT 2"#,
    )
    .fetch_all(connection)
    .await?;
    let mut rows = rows.into_iter();
    let row = rows.next().ok_or(EnrollmentRowCorruption::MissingMachine)?;
    if rows.next().is_some() {
        return Err(EnrollmentRowCorruption::MultipleMachines.into());
    }
    if !row.valid_storage {
        return Err(EnrollmentRowCorruption::StorageType.into());
    }
    let stored_machine: MachineId = std::str::from_utf8(&row.machine_id)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(EnrollmentRowCorruption::MachineId)?;
    if &stored_machine != machine_id {
        return Err(EnrollmentRowCorruption::WrongMachine.into());
    }
    let generation = row.generation.ok_or(EnrollmentRowCorruption::StorageType)?;
    Ok(StoredEnrollmentRow::from_columns(
        generation,
        row.metadata.as_deref(),
        row.sealed,
    )?)
}

#[cfg(test)]
#[path = "enrollment_row_store_test.rs"]
mod tests;
