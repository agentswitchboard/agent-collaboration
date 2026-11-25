use std::{path::Path, sync::Arc, time::Duration};

use collaboration_central_conformance::signin_fixtures as signed;
use collaboration_central_tokio::test_support::MachineLinkFixture;
use router_clock::{ManualClock, RuntimeClock};
use serde_json::json;
use switchboard_link_lease::DiscoverySource;
use switchboard_machine_core_store::MACHINE_LOCAL_DATABASE;
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, DiscoveryAnswer, EnrollmentBinding,
    EnrollmentContext, EnrollmentId, MachineId, OrganizationId, RefreshAnswer, RefreshCredential,
    RefreshSource, SecretValue, SignInFailure, TokenGrant, decode_discovery,
};
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStore,
    test_support::{KeychainAccess, KeychainAccessError},
};
use switchboard_message_custody::{
    custody_task::{CustodyEventSink, CustodyTaskEvent},
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::mpsc;
use workspace_ids::{RequestId, SessionId};

pub(super) const EPOCH: &str = "2030-03-17T17:46:40Z";

struct FixtureKeychain;

impl KeychainAccess for FixtureKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        Ok(Some(vec![0x71; 32]))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}

pub(super) async fn open_store(
    root: &Path,
    machine_id: MachineId,
    clock: Arc<ManualClock>,
) -> MachineEnrollmentStore {
    MachineEnrollmentStore::open_with_test_keychain(
        &root.join(MACHINE_LOCAL_DATABASE),
        root.join("secrets"),
        machine_id,
        clock,
        Arc::new(FixtureKeychain),
    )
    .await
    .unwrap()
}

pub(super) async fn grant_at_central(
    central: &MachineLinkFixture,
    machine_id: &MachineId,
    approval_request: RequestId,
) -> (EnrollmentId, SecretValue) {
    let enrollment = approve_at_central(central, machine_id, approval_request).await;
    let token = grant_for_enrollment(central, machine_id, enrollment).await;
    (enrollment, token)
}

pub(super) async fn approve_at_central(
    central: &MachineLinkFixture,
    machine_id: &MachineId,
    approval_request: RequestId,
) -> EnrollmentId {
    let organization = central.organization_id.as_str();
    let now = central.clock.utc_now().timestamp();
    let runtime = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let approved = runtime.decide_device_login(&serde_json::from_value(json!({
        "organizationId": organization, "approver": signed::OWNER,
        "permissionChecked": "machine:enroll", "requestId": approval_request,
        "machineId": machine_id.as_str(), "label": "Message unit fixture", "allowedWorkspaces": [],
        "approverSecondFactorAt": now - 60, "codeExpiresAt": now + 600,
    })).unwrap()).await.unwrap();
    let approved = serde_json::to_value(approved).unwrap();
    approved
        .get("enrollmentId")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .parse()
        .unwrap()
}

pub(super) async fn grant_for_enrollment(
    central: &MachineLinkFixture,
    machine_id: &MachineId,
    enrollment: EnrollmentId,
) -> SecretValue {
    let organization = central.organization_id.as_str();
    let now = central.clock.utc_now().timestamp();
    let enrollment = enrollment.to_string();
    let runtime = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let grant = runtime
        .decide_machine_token(
            &serde_json::from_value(json!({
                "organizationId": organization, "enrollmentId": enrollment, "clientId": enrollment,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let grant = serde_json::to_value(grant).unwrap();
    let token = signed::machine_token(
        organization,
        machine_id.as_str(),
        &enrollment,
        now,
        grant
            .get("grantExpiresAt")
            .and_then(serde_json::Value::as_i64)
            .unwrap(),
    )
    .unwrap();
    SecretValue::new(token).unwrap()
}

pub(super) async fn enroll(
    store: &MachineEnrollmentStore,
    machine_id: MachineId,
    organization_id: OrganizationId,
    enrollment_id: EnrollmentId,
) -> EnrollmentContext {
    let context = EnrollmentContext {
        machine_id,
        organization_id,
        deployment: switchboard_machine_enrollment::SignInDeployment::new(
            "https://sign-in.example/".parse().unwrap(),
        )
        .unwrap(),
    };
    assert_eq!(
        store
            .replace(
                BindingGeneration::EMPTY,
                &EnrollmentBinding {
                    context: context.clone(),
                    generation: BindingGeneration::from_persisted(1).unwrap(),
                    credential: RefreshCredential {
                        enrollment_id,
                        refresh_token: SecretValue::new("fixture-first-refresh".to_owned())
                            .unwrap(),
                    },
                }
            )
            .await
            .unwrap(),
        ConditionalWrite::Applied
    );
    context
}

#[derive(Clone)]
pub(super) struct FixtureDiscovery(pub(super) mpsc::UnboundedSender<()>);

impl DiscoverySource for FixtureDiscovery {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        let _ = self.0.send(());
        Ok(decode_discovery(br#"{"client_id":"019f0000-0000-7000-8000-000000000063","link_url":"https://central.example/v1/organizations/{organization_id}/link"}"#).unwrap())
    }
}

#[derive(Clone)]
pub(super) struct FixtureRefresh {
    pub(super) enrollment_id: EnrollmentId,
    pub(super) bearer: SecretValue,
}

impl RefreshSource for FixtureRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Rotated(TokenGrant {
            access_token: self.bearer.clone(),
            expires_in: Duration::from_secs(600),
            credential: RefreshCredential {
                enrollment_id: self.enrollment_id,
                refresh_token: SecretValue::new("fixture-rotated-refresh".to_owned()).unwrap(),
            },
        })
    }
}

pub(super) struct KnownRecipient(pub(super) SessionId);

impl RecipientSessionDirectory for KnownRecipient {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RecipientSessionAnswer> + Send + 'lookup>>
    {
        Box::pin(std::future::ready(if session_id == &self.0 {
            RecipientSessionAnswer::KnownOnce
        } else {
            RecipientSessionAnswer::UnknownSession
        }))
    }
}

pub(super) struct RecordCustody(pub(super) mpsc::UnboundedSender<CustodyTaskEvent>);

impl CustodyEventSink for RecordCustody {
    fn record_event(&self, event: CustodyTaskEvent) {
        let _ = self.0.send(event);
    }
}
