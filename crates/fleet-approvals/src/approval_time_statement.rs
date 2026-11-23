//! The owning approval transaction advances its durable logical time.
use central_storage::{SafeInteger, Statement};
use fleet_engine::checked_engine_statement;

pub fn advance_approval_time_statement(
    organization: &fleet_identity_protocol::OrganizationId,
    observed_ms: SafeInteger,
    observed_at: String,
) -> Statement {
    checked_engine_statement!(
        query_file,
        [],
        "src/queries/advance_approval_applying_time.sql",
        organization = organization.as_str().to_owned(),
        observed_ms = observed_ms,
        observed_at = observed_at,
    )
}
