//! Read-only enrollment metadata from an existing machine database.

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
    Query(EnrollmentMetadataQueryError),
    #[error("the machine database is busy or locked. Try again")]
    Busy(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum EnrollmentMetadataQueryError {
    #[error("could not read the machine database")]
    Sqlite(#[from] sqlx::Error),
    #[error("stored enrollment metadata is corrupt")]
    Corrupt(#[from] EnrollmentRowCorruption),
}

impl From<EnrollmentMetadataQueryError> for EnrollmentMetadataReadError {
    fn from(error: EnrollmentMetadataQueryError) -> Self {
        match error {
            EnrollmentMetadataQueryError::Sqlite(source) if is_sqlite_busy_or_locked(&source) => {
                Self::Busy(source)
            }
            other => Self::Query(other),
        }
    }
}

fn is_sqlite_busy_or_locked(error: &sqlx::Error) -> bool {
    const SQLITE_PRIMARY_CODE_MASK: u32 = 0xff;
    const SQLITE_BUSY: u32 = 5;
    const SQLITE_LOCKED: u32 = 6;
    error
        .as_database_error()
        .and_then(|error| error.code())
        .and_then(|code| code.parse::<u32>().ok())
        .is_some_and(|code| matches!(code & SQLITE_PRIMARY_CODE_MASK, SQLITE_BUSY | SQLITE_LOCKED))
}

/// Observes an existing database without creating a database file or changing any rows.
pub async fn read_machine_enrollment_row(
    database: &Path,
) -> Result<MachineEnrollmentMetadataRow, EnrollmentMetadataReadError> {
    if existing_file_size(database)?.is_none() {
        return Ok(MachineEnrollmentMetadataRow::DatabaseAbsent);
    }
    let mut wal = database.as_os_str().to_owned();
    wal.push("-wal");
    let has_nonempty_wal = existing_file_size(Path::new(&wal))?.is_some_and(|size| size != 0);
    // Use SQLite's WAL reader when committed changes may still be in the log.
    // Without a log, the immutable reader avoids creating sidecar files.
    let options = SqliteConnectOptions::new()
        .filename(database)
        .create_if_missing(false)
        .read_only(true)
        .immutable(!has_nonempty_wal);
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
