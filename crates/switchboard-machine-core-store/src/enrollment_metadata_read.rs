//! Read-only enrollment metadata from an idle machine database.

use std::{fs, io, path::Path};

use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use workspace_ids::MachineId;

use crate::{EnrollmentRowCorruption, StoredEnrollmentRowMetadata};

/// A read found no database, no identity, or metadata on the durable identity.
#[derive(Debug)]
pub enum MachineEnrollmentMetadataRow {
    DatabaseAbsent,
    MachineAbsent,
    Found {
        machine_id: MachineId,
        enrollment: StoredEnrollmentRowMetadata,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum EnrollmentMetadataReadError {
    #[error("could not inspect the machine database")]
    Filesystem(#[from] io::Error),
    #[error(transparent)]
    Query(#[from] EnrollmentMetadataQueryError),
    #[error("the machine database still has a write-ahead log")]
    PendingWal,
}

#[derive(Debug, thiserror::Error)]
pub enum EnrollmentMetadataQueryError {
    #[error("could not read the machine database")]
    Sqlite(#[from] sqlx::Error),
    #[error("stored enrollment metadata is corrupt")]
    Corrupt(#[from] EnrollmentRowCorruption),
}

/// Observes an idle database without creating a database, journal or shared-memory file.
pub async fn read_machine_enrollment_row(
    database: &Path,
) -> Result<MachineEnrollmentMetadataRow, EnrollmentMetadataReadError> {
    if existing_file_size(database)?.is_none() {
        return Ok(MachineEnrollmentMetadataRow::DatabaseAbsent);
    }
    let mut wal = database.as_os_str().to_owned();
    wal.push("-wal");
    if existing_file_size(Path::new(&wal))?.is_some_and(|size| size != 0) {
        // The immutable reader could miss changes held in a WAL. The state
        // owner must settle that log before this read can describe the row.
        return Err(EnrollmentMetadataReadError::PendingWal);
    }
    // A plain read-only WAL connection can create sidecars. The caller owns
    // the idle-root precondition for SQLite's immutable, no-lock reader.
    let options = SqliteConnectOptions::new()
        .filename(database)
        .read_only(true)
        .immutable(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .map_err(EnrollmentMetadataQueryError::from)?;
    let result = fetch_enrollment_metadata(&mut connection, None).await;
    let closed = connection.close().await;
    let row = result?;
    closed.map_err(EnrollmentMetadataQueryError::from)?;
    Ok(match row {
        Some((machine_id, enrollment)) => MachineEnrollmentMetadataRow::Found {
            machine_id,
            enrollment,
        },
        None => MachineEnrollmentMetadataRow::MachineAbsent,
    })
}

fn existing_file_size(path: &Path) -> io::Result<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) async fn fetch_enrollment_metadata(
    connection: &mut SqliteConnection,
    expected_machine_id: Option<&MachineId>,
) -> Result<Option<(MachineId, StoredEnrollmentRowMetadata)>, EnrollmentMetadataQueryError> {
    let rows = sqlx::query!(
        r#"SELECT CAST(machine_id AS BLOB) AS "machine_id!: Vec<u8>",
                  CASE WHEN typeof(enrollment_generation) = 'integer' THEN enrollment_generation END
                      AS "generation?: i64",
                  CAST(enrollment_json AS BLOB) AS "metadata?: Vec<u8>",
                  (typeof(machine_id) = 'text'
                   AND typeof(enrollment_json) IN ('text', 'null')) AS "valid_storage!: bool"
           FROM machine_identity LIMIT 2"#,
    )
    .fetch_all(connection)
    .await?;
    let mut rows = rows.into_iter();
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(EnrollmentRowCorruption::MultipleMachines.into());
    }
    if !row.valid_storage {
        return Err(EnrollmentRowCorruption::StorageType.into());
    }
    let machine_id = parse_machine_id(&row.machine_id)?;
    if expected_machine_id.is_some_and(|expected| expected != &machine_id) {
        return Err(EnrollmentRowCorruption::WrongMachine.into());
    }
    let generation = row.generation.ok_or(EnrollmentRowCorruption::StorageType)?;
    let enrollment =
        StoredEnrollmentRowMetadata::from_columns(generation, row.metadata.as_deref())?;
    Ok(Some((machine_id, enrollment)))
}

pub(super) fn parse_machine_id(stored_id: &[u8]) -> Result<MachineId, EnrollmentRowCorruption> {
    std::str::from_utf8(stored_id)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(EnrollmentRowCorruption::MachineId)
}
