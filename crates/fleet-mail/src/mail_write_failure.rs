//! Applying failures before the host settles a mail transaction.
use crate::direct_store_port::{DirectReadFailure, DirectWriteFailure};
use central_storage::StoreFailure;
use fleet_lent_transaction::CentralTransaction;
use sqlx_core::database::Database;

#[derive(Debug, thiserror::Error)]
pub enum DirectApplyFailure {
    #[error("direct-message statement was rejected: {0}")]
    Sql(sqlx_core::Error),
    #[error(transparent)]
    Store(#[from] StoreFailure),
    #[error("the applying guard no longer holds")]
    Conflict,
    #[error("the observed direct message cannot be decoded: {0}")]
    CorruptObserved(StoreFailure),
}
impl From<sqlx_core::Error> for DirectApplyFailure {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<DirectReadFailure> for DirectApplyFailure {
    fn from(failure: DirectReadFailure) -> Self {
        Self::Store(failure.into())
    }
}

impl DirectWriteFailure {
    pub fn from_store(error: StoreFailure) -> Self {
        match error {
            StoreFailure::Unavailable => Self::Read(DirectReadFailure::Unavailable),
            StoreFailure::Corrupt { reason } => Self::Read(DirectReadFailure::Corrupt { reason }),
            StoreFailure::AnswerLost => Self::AnswerLost,
            StoreFailure::AlarmUnpublishable { reason } => Self::AlarmUnpublishable { reason },
        }
    }
}

impl From<DirectWriteFailure> for StoreFailure {
    fn from(failure: DirectWriteFailure) -> Self {
        match failure {
            DirectWriteFailure::Read(failure) => failure.into(),
            DirectWriteFailure::AnswerLost => Self::AnswerLost,
            DirectWriteFailure::Conflict => Self::Unavailable,
            DirectWriteFailure::AlarmUnpublishable { reason } => {
                Self::AlarmUnpublishable { reason }
            }
        }
    }
}

impl DirectApplyFailure {
    pub fn into_write_failure(self) -> DirectWriteFailure {
        match self {
            Self::Conflict => DirectWriteFailure::Conflict,
            Self::Sql(sqlx_core::Error::Database(error))
                if matches!(
                    error.kind(),
                    sqlx_core::error::ErrorKind::UniqueViolation
                        | sqlx_core::error::ErrorKind::ForeignKeyViolation
                        | sqlx_core::error::ErrorKind::NotNullViolation
                        | sqlx_core::error::ErrorKind::CheckViolation
                ) =>
            {
                DirectWriteFailure::Conflict
            }
            Self::Sql(_) => DirectWriteFailure::Read(DirectReadFailure::Unavailable),
            Self::Store(error) | Self::CorruptObserved(error) => {
                DirectWriteFailure::from_store(error)
            }
        }
    }
}

pub(super) fn require_one_affected_row<T: CentralTransaction>(
    result: <T::Database as Database>::QueryResult,
) -> Result<(), DirectApplyFailure> {
    if T::rows_affected(result) == 1 {
        Ok(())
    } else {
        Err(DirectApplyFailure::Conflict)
    }
}
