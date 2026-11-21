//! Signed stamp facts and replacement decisions.
mod fleet_statement;
#[cfg(test)]
mod fleet_statement_test_support;
mod fleet_statement_wire;
mod statement_replacement;
mod stored_stamp;

pub use fleet_statement::{
    FleetOrganizationStatementTrust, FleetOrganizationStatementVerifier,
    MAXIMUM_STATEMENT_LIFETIME_SECONDS, StoredStatementFacts, VerifiedFleetOrganizationStatement,
};
pub use fleet_statement_wire::{OrganizationStatementDigest, stored_statement_unexpired};
pub(crate) use statement_replacement::{OfferStamp, StampEffect, StampMachine, StampRefusal};
pub use stored_stamp::{StoredOrganizationStamp, StoredStampRead};
