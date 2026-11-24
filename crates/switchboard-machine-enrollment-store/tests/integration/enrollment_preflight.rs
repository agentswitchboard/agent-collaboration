//! The real sign-in client and SQLite adapter enforce the local scope boundary.

use std::time::Duration;

use router_clock::ClockInstant;
use switchboard_machine_enrollment::{
    ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentEnd, EnrollmentFailure,
    EnrollmentRequest, SignInClient, StoredEnrollment,
};

use super::{ALTERNATE_ENROLLMENT, ENROLLMENT, Fixture, ORGANIZATION, generation, row_columns};

#[expect(
    clippy::expect_used,
    reason = "Fixture failures must name the missing TLS setup step."
)]
#[path = "../support/local_sign_in.rs"]
mod local_sign_in;
use local_sign_in::{LocalSignIn, REFRESH};

const OTHER_ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000005";

#[tokio::test]
async fn ended_scope_refuses_another_organization_before_sign_in() {
    assert_preflight_refuses_scope(Some(EnrollmentEnd::Revoked), ScopeDifference::Organization)
        .await;
}

#[tokio::test]
async fn ended_scope_refuses_another_deployment_before_sign_in() {
    assert_preflight_refuses_scope(Some(EnrollmentEnd::Revoked), ScopeDifference::Deployment).await;
}

#[tokio::test]
async fn active_scope_refuses_another_organization_before_sign_in() {
    assert_preflight_refuses_scope(None, ScopeDifference::Organization).await;
}

enum ScopeDifference {
    Organization,
    Deployment,
}

async fn assert_preflight_refuses_scope(
    ending: Option<EnrollmentEnd>,
    difference: ScopeDifference,
) {
    let service = LocalSignIn::start().await;
    let fixture = Fixture::open().await;
    let client = SignInClient::new_https_fixture(
        service.origin.parse().unwrap(),
        fixture.clock.clone(),
        &service.root_certificate,
    )
    .unwrap();
    let mut saved = fixture.binding(1, ENROLLMENT, REFRESH);
    let mut organization_id = saved.context.organization_id;
    match difference {
        ScopeDifference::Organization => {
            saved.context.deployment = client.deployment().clone();
            organization_id = OTHER_ORGANIZATION.parse().unwrap();
        }
        ScopeDifference::Deployment => {}
    }
    assert_eq!(
        fixture.store.replace(generation(0), &saved).await,
        Ok(ConditionalWrite::Applied)
    );
    if let Some(why) = ending {
        assert_eq!(
            fixture.store.remove(generation(1), why).await,
            Ok(ConditionalWrite::Applied)
        );
    }
    let mut connection = fixture.connection().await;
    let before = row_columns(&mut connection).await;
    let request = EnrollmentRequest {
        machine_id: &fixture.machine,
        machine_label: "Fixture",
        organization_id,
    };
    let mut instructions_shown = false;
    let outcome = tokio::select! {
        biased;
        () = service.next_request() => panic!("a known scope mismatch must send zero sign-in requests"),
        result = client.enroll(request, &fixture.store, |_| {
            instructions_shown = true;
            Ok(())
        }) => result,
    };

    assert_eq!(
        outcome,
        Err(EnrollmentFailure::Store(
            CredentialStoreError::EnrollmentScopeMismatch
        ))
    );
    assert_eq!(service.requests(), 0);
    assert!(!instructions_shown);
    assert_eq!(row_columns(&mut connection).await, before);
}

#[tokio::test]
async fn a_held_first_grant_loses_to_another_organizations_install() {
    let service = LocalSignIn::start().await;
    let fixture = Fixture::open().await;
    let client = SignInClient::new_https_fixture(
        service.origin.parse().unwrap(),
        fixture.clock.clone(),
        &service.root_certificate,
    )
    .unwrap();
    let organization_id = ORGANIZATION.parse().unwrap();
    let request = EnrollmentRequest {
        machine_id: &fixture.machine,
        machine_label: "Fixture",
        organization_id,
    };
    let (reached, release) = service.hold_initial_grant();
    let mut losing_install = std::pin::pin!(client.enroll(request, &fixture.store, |_| Ok(())));
    let first_poll = ClockInstant::ZERO.after(Duration::from_secs(5));
    tokio::select! {
        result = &mut losing_install => panic!("installation finished before its poll: {result:?}"),
        () = fixture.clock.not_due_check_now(first_poll) => {}
    }
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    tokio::select! {
        result = &mut losing_install => panic!("installation finished before the held grant: {result:?}"),
        result = reached => result.unwrap(),
    }
    assert_eq!(fixture.store.read().await, Ok(StoredEnrollment::Empty));
    let mut winner = fixture.binding(1, ALTERNATE_ENROLLMENT, "fictional-winner-refresh");
    winner.context.organization_id = OTHER_ORGANIZATION.parse().unwrap();
    winner.context.deployment = client.deployment().clone();
    assert_eq!(
        fixture.store.replace(generation(0), &winner).await,
        Ok(ConditionalWrite::Applied)
    );
    let mut connection = fixture.connection().await;
    let winning_row = row_columns(&mut connection).await;
    let mut changes = fixture.store.subscribe_enrollment_changes();
    let revision = *changes.borrow_and_update();

    release.send(()).unwrap();
    let outcome = losing_install.await;

    assert_eq!(
        outcome,
        Err(EnrollmentFailure::AfterApproval(
            switchboard_machine_enrollment::ApprovedEnrollmentSaveFailure::NotSaved(
                switchboard_machine_enrollment::EnrollmentSaveFailure::ReplacementConflict,
            ),
        ))
    );
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(winner))
    );
    assert_eq!(row_columns(&mut connection).await, winning_row);
    assert_eq!(*changes.borrow(), revision);
    assert!(!changes.has_changed().unwrap());
    assert_eq!(service.token_requests(), 1);
}
