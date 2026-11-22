//! Signed stamp facts and replacement decisions.
mod fleet_statement;
#[cfg(test)]
mod fleet_statement_test_support;
mod fleet_statement_wire;
mod statement_replacement;
mod stored_stamp;

pub use fleet_statement::{
    ConfiguredStatementRefusal, FleetOrganizationStatementTrust,
    FleetOrganizationStatementVerifier, InvalidStampTrust, StampTimeRefusal, StoredStatementFacts,
    VerifiedFleetOrganizationStatement,
};
pub(crate) use statement_replacement::{
    OfferStamp, ReplayStanding, StampApplication, StampMachine, StampMutation, StampRefusal,
};
pub use stored_stamp::{StoredOrganizationStamp, StoredStampRead};
