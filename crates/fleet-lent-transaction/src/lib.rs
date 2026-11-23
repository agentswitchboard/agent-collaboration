//! The SQLx executor a host lends to an owning Fleet repository.
#![forbid(unsafe_code)]

use fleet_identity_protocol::OrganizationId;
use sqlx::{Database, Executor};

/// An owning Central transaction lends the repository an executor.
/// The host retains responsibility for begin, commit and physical alarm writes.
pub trait CentralTransaction {
    type Database: Database;

    fn executor(&mut self) -> impl Executor<'_, Database = Self::Database>;

    fn rows_affected(result: <Self::Database as Database>::QueryResult) -> u64;
}

/// The Central host identifies the physical organization for the borrowed transaction.
pub trait OrganizationTransaction: CentralTransaction {
    fn organization_id(&self) -> &OrganizationId;
}
