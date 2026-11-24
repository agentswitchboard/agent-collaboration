use std::{sync::atomic::Ordering::SeqCst, time::Duration};

use router_clock::ClockInstant;
use switchboard_machine_enrollment::{
    AccessRequest, EnrollmentRequest, LeaseUnavailable, RefreshAnswer, RefreshCredential,
    RefreshOwner, RefreshSource, SignInClient,
};

use super::{
    ConditionalWrite, CredentialStore, EnrollmentEnd, Fixture, InvalidReason, StoredEnrollment,
    TestWriteFault, fixture::token_value, generation,
};

#[expect(
    dead_code,
    reason = "The integration target uses the peer's request and grant gates."
)]
#[path = "../tests/support/local_sign_in.rs"]
mod local_sign_in;
use local_sign_in::{LocalSignIn, REFRESH, ROTATED};

#[tokio::test]
async fn public_https_install_rotation_and_explicit_marker_use_real_sqlite() {
    let service = LocalSignIn::start().await;
    let fixture = Fixture::open().await;
    let client = SignInClient::new_https_fixture(
        service.origin.parse().unwrap(),
        fixture.clock.clone(),
        &service.root_certificate,
    )
    .unwrap();
    let ordinary =
        SignInClient::new(service.origin.parse().unwrap(), fixture.clock.clone()).unwrap();
    assert!(
        ordinary.discover().await.is_err(),
        "ordinary client trusted the private fixture root"
    );
    let organization_id = super::fixture::ORGANIZATION.parse().unwrap();

    let request = EnrollmentRequest {
        machine_id: &fixture.machine,
        machine_label: "Fixture",
        organization_id,
    };
    let mut install = std::pin::pin!(client.enroll(request, &fixture.store, |_| Ok(())));
    let first_poll = ClockInstant::ZERO.after(Duration::from_secs(5));
    tokio::select! {
        result = &mut install => panic!("installation finished before its poll: {result:?}"),
        () = fixture.clock.not_due_check_now(first_poll) => {}
    }
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    let receipt = install.await.unwrap();

    let reopened = fixture.another_worker().await;
    let StoredEnrollment::Active(initial) = reopened.read().await.unwrap() else {
        panic!("installation was not persisted");
    };
    assert_eq!(receipt.generation, generation(1));
    assert_eq!(receipt.enrollment_id, initial.credential.enrollment_id);
    assert!(initial.credential.refresh_token == token_value(REFRESH));
    assert_eq!(initial.context.deployment.origin().as_str(), service.origin);

    let owner = client.refresh_owner(
        fixture.store.clone(),
        fixture.machine.clone(),
        organization_id,
    );
    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
    let lease = owner.lease(AccessRequest::Usable).await.unwrap();
    assert_eq!(lease.binding_generation, generation(2));
    assert_eq!(lease.key, initial.key());
    assert_eq!(service.token_requests(), 2);
    let StoredEnrollment::Active(rotated) = reopened.read().await.unwrap() else {
        panic!("rotation was not persisted");
    };
    assert!(rotated.credential.refresh_token == token_value(ROTATED));
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);

    fixture.faults.arm(TestWriteFault::BeforeCommit);
    assert_eq!(
        owner
            .end_enrollment(&lease.key, EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        reopened.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(3),
            why: EnrollmentEnd::Revoked,
            key: lease.key,
            deployment: initial.context.deployment,
        })
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);
}

struct Refused;

impl RefreshSource for Refused {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused,
        }
    }
}

#[tokio::test]
async fn a_refresh_refusal_marks_the_real_row_ended_even_after_lost_acknowledgement() {
    let fixture = Fixture::open().await;
    let binding = fixture.install().await;
    let owner = RefreshOwner::new(
        fixture.store.clone(),
        Refused,
        fixture.clock.clone(),
        binding.context.clone(),
    );
    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);

    assert_eq!(
        owner.lease(AccessRequest::Usable).await,
        Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::RefreshRefused,
        })
    );
    assert_eq!(
        fixture.another_worker().await.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(2),
            why: EnrollmentEnd::RefreshRefused,
            key: binding.key(),
            deployment: binding.context.deployment,
        })
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);
}
