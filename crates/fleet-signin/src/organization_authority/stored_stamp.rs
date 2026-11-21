use super::fleet_statement_wire::digest_compact_statement;
use fleet_contracts::OrganizationStatementDigest;
use fleet_identity_protocol::OrganizationId;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOrganizationStamp {
    organization: OrganizationId,
    statement: String,
}
impl StoredOrganizationStamp {
    pub(crate) fn new(organization: OrganizationId, statement: String) -> Self {
        Self {
            organization,
            statement,
        }
    }
    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization
    }
    #[must_use]
    pub fn statement(&self) -> &str {
        &self.statement
    }
    #[must_use]
    pub(crate) fn digest(&self) -> OrganizationStatementDigest {
        digest_compact_statement(&self.statement)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredStampRead {
    Unprovisioned,
    Stamped(StoredOrganizationStamp),
}
