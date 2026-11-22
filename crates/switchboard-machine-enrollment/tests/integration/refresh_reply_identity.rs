use axum::http::StatusCode;
use switchboard_machine_enrollment::{
    AccessRequest, BindingGeneration, ConditionalWrite, CredentialStore, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, InvalidReason, LeaseConfigurationFault, RefreshCredential,
    SecretValue, StoredEnrollment,
};

use super::scripted_sign_in::{
    ENROLLMENT_ID, INITIAL_REFRESH, RECEIVER, RELAY, ROTATED_ACCESS, ROTATED_REFRESH, Reply,
    ScriptStore, SignInScript, machine_id, organization_id,
};

#[tokio::test]
async fn a_success_for_another_enrollment_never_replaces_or_releases_credentials() {
    let mut script = SignInScript::token_responses([Reply::new(
        StatusCode::OK,
        serde_json::json!({
            "access_token": ROTATED_ACCESS, "refresh_token": ROTATED_REFRESH,
            "expires_in": 60, "expires_at": 60,
            "token_type": "Bearer", "scope": "offline_access",
            "machine_client_id": "019a0000-0000-7000-8000-00000000000a",
            "fleet_central": {"receiver": RELAY, "relay": RECEIVER},
        })
        .to_string(),
    )])
    .await;
    let context = EnrollmentContext {
        organization_id: organization_id(),
        deployment: script.client.deployment().clone(),
        machine_id: machine_id(),
    };
    let binding = EnrollmentBinding {
        generation: BindingGeneration::from_persisted(1),
        context,
        credential: RefreshCredential {
            enrollment_id: ENROLLMENT_ID.parse().expect("old enrollment"),
            central: switchboard_machine_enrollment::CentralEndpointIds {
                receiver: RECEIVER.parse().expect("old receiver"),
                relay: RELAY.parse().expect("old relay"),
            },
            refresh_token: SecretValue::new(INITIAL_REFRESH.to_owned()).expect("old secret"),
        },
    };
    let store = ScriptStore::default();
    assert_eq!(
        store.replace(BindingGeneration::EMPTY, &binding).await,
        Ok(ConditionalWrite::Applied)
    );
    let owner = script
        .client
        .refresh_owner(store.clone(), machine_id(), organization_id());

    let expected =
        Err(LeaseConfigurationFault::InvalidAnswer(InvalidReason::WrongEnrollment).into());
    assert_eq!(owner.lease(AccessRequest::Usable).await, expected);
    assert_eq!(store.snapshot(), StoredEnrollment::Active(binding.clone()));
    assert_eq!(owner.lease(AccessRequest::Usable).await, expected);
    script
        .observations
        .try_recv()
        .expect("observed refresh")
        .assert_refresh(INITIAL_REFRESH);
    script.assert_exhausted();
    assert_eq!(
        owner
            .end_enrollment(&binding.key(), EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert!(
        matches!(store.snapshot(), StoredEnrollment::Ended { key, why: EnrollmentEnd::Revoked, .. } if key == binding.key())
    );
}
