//! Keychain-free observation of the Mac's stored enrollment metadata.

use std::path::Path;

use switchboard_machine_core_store::{
    EnrollmentMetadataQueryError, EnrollmentMetadataReadError, MachineEnrollmentMetadataRow,
    read_machine_enrollment_row,
};
use switchboard_machine_enrollment::{EnrollmentMetadata, MachineId};

use crate::enrollment_row_mapping::enrollment_metadata;

/// The distinction between missing files, a missing identity, and a stored state.
#[derive(Debug)]
pub enum MachineEnrollmentStatus {
    DatabaseAbsent,
    MachineAbsent,
    Present {
        machine_id: MachineId,
        metadata: EnrollmentMetadata,
    },
}

/// Observes enrollment without initializing storage or reading a credential.
pub async fn read_machine_enrollment_status(
    database: &Path,
) -> Result<MachineEnrollmentStatus, EnrollmentMetadataReadError> {
    Ok(match read_machine_enrollment_row(database).await? {
        MachineEnrollmentMetadataRow::DatabaseAbsent => MachineEnrollmentStatus::DatabaseAbsent,
        MachineEnrollmentMetadataRow::MachineAbsent => MachineEnrollmentStatus::MachineAbsent,
        MachineEnrollmentMetadataRow::Found {
            machine_id,
            enrollment,
        } => MachineEnrollmentStatus::Present {
            metadata: enrollment_metadata(enrollment, &machine_id)
                .map_err(EnrollmentMetadataQueryError::from)?,
            machine_id,
        },
    })
}
