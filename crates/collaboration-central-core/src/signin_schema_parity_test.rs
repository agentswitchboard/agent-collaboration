use crate::organization_admin_contract::OrganizationAdminResult;
use fleet_contracts::{
    MachineEnrollmentRevocationResult, OrganizationStatementDigestReply, ProvisionOrganization,
    ProvisionOrganizationResult, ReadOrganizationStatementDigest, RevokeMachineEnrollmentRequest,
};
use serde_json::json;

#[path = "../tests/support/contract_artifacts.rs"]
mod contract_artifacts;
#[path = "../tests/support/schema_validation.rs"]
mod schema_validation;

#[test]
fn provisioning_and_revocation_wire_contracts() {
    contract_artifacts::assert_contract::<OrganizationAdminResult>(
        "OrganizationAdminResult",
        serde_json::from_str(include_str!(
            "../contracts/OrganizationAdminResult.fixtures.json"
        ))
        .unwrap(),
        false,
    );
    contract_artifacts::assert_request_contract::<RevokeMachineEnrollmentRequest>(
        "RevokeMachineEnrollmentRequest",
        vec![json!({
            "organizationId": "019a3dce-2000-7000-8000-000000000001",
            "enrollmentId": "019a3dce-2000-7000-8000-000000000002"
        })],
    );
    contract_artifacts::assert_contract::<MachineEnrollmentRevocationResult>(
        "MachineEnrollmentRevocationResult",
        vec![
            json!({"result":"revoked"}),
            json!({"result":"notFound"}),
            json!({"result":"alreadyEnded"}),
            json!({"result":"refused","reason":"notPermitted"}),
            json!({"result":"refused","reason":"factorNotRecent"}),
        ],
        false,
    );
    contract_artifacts::assert_contract::<ProvisionOrganization>(
        "ProvisionOrganization",
        vec![json!({
            "organizationId": "019a3dce-2000-7000-8000-000000000001",
            "organizationStatement": "e30.e30.AA"
        })],
        false,
    );
    contract_artifacts::assert_contract::<ProvisionOrganizationResult>(
        "ProvisionOrganizationResult",
        vec![json!({"result":"provisioned"})],
        false,
    );
    contract_artifacts::assert_contract::<ReadOrganizationStatementDigest>(
        "ReadOrganizationStatementDigest",
        vec![json!({"organizationId":"019a3dce-2000-7000-8000-000000000001"})],
        false,
    );
    contract_artifacts::assert_contract::<OrganizationStatementDigestReply>(
        "OrganizationStatementDigestReply",
        vec![
            serde_json::from_str(include_str!(
                "../../fleet-contracts/tests/fixtures/organization-statement-digest.json"
            ))
            .unwrap(),
        ],
        false,
    );
}
