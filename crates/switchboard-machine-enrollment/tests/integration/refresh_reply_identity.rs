use axum::http::StatusCode;
use switchboard_machine_enrollment::{
    AccessRequest, BindingGeneration, ConditionalWrite, CredentialStore, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, InvalidReason, LeaseConfigurationFault, LeaseUnavailable,
    RefreshCredential, SecretValue, StoredEnrollment,
};

use super::scripted_sign_in::{
    ENROLLMENT_ID, INITIAL_REFRESH, ROTATED_ACCESS, ROTATED_REFRESH, Reply, ScriptStore,
    SignInScript, machine_id, organization_id,
};

#[tokio::test]
async fn a_success_for_another_enrollment_never_replaces_or_releases_credentials() {
    let mut script = SignInScript::token_responses([Reply::new(
        StatusCode::OK,
        serde_json::json!({
            "access_token": ROTATED_ACCESS, "refresh_token": ROTATED_REFRESH,
            "expires_in": 60, "expires_at": 60,
            "token_type": "Bearer", "scope": "offline_access",
            "machine_client_id": "019a0000-0000-7000-8000-00000000000a"
        })
        .to_string(),
    )])
    .await;
    let (store, binding) = enrolled_store(&script).await;
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

async fn enrolled_store(script: &SignInScript) -> (ScriptStore, EnrollmentBinding) {
    let context = EnrollmentContext {
        organization_id: organization_id(),
        deployment: script.client.deployment().clone(),
        machine_id: machine_id(),
    };
    let binding = EnrollmentBinding {
        generation: BindingGeneration::from_persisted(1).expect("fixture generation"),
        context,
        credential: RefreshCredential {
            enrollment_id: ENROLLMENT_ID.parse().expect("old enrollment"),
            refresh_token: SecretValue::new(INITIAL_REFRESH.to_owned()).expect("old secret"),
        },
    };
    let store = ScriptStore::default();
    assert_eq!(
        store.replace(BindingGeneration::EMPTY, &binding).await,
        Ok(ConditionalWrite::Applied)
    );
    (store, binding)
}

#[tokio::test]
async fn oversized_rotation_reply_leaves_resolution_to_the_next_refresh() {
    let mut script = SignInScript::token_responses([
        Reply::chunked(StatusCode::OK, vec![" ".repeat(65_537)]),
        Reply::failure(StatusCode::BAD_REQUEST, "invalid_grant"),
    ])
    .await;
    let (store, binding) = enrolled_store(&script).await;
    let owner = script
        .client
        .refresh_owner(store.clone(), machine_id(), organization_id());

    assert_eq!(
        owner.lease(AccessRequest::Usable).await,
        Err(LeaseUnavailable::OversizedRefreshReply)
    );
    assert_eq!(store.snapshot(), StoredEnrollment::Active(binding.clone()));
    assert_eq!(
        owner.lease(AccessRequest::Usable).await,
        Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::RefreshRefused
        })
    );
    assert!(
        matches!(store.snapshot(), StoredEnrollment::Ended { key, why: EnrollmentEnd::RefreshRefused, .. } if key == binding.key())
    );
    for _ in 0..2 {
        script
            .observations
            .try_recv()
            .expect("each attempted refresh")
            .assert_refresh(INITIAL_REFRESH);
    }
    script.assert_exhausted();
}
