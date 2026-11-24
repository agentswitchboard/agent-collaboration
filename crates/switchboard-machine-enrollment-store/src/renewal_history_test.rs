use std::{collections::VecDeque, sync::Mutex, time::Duration};

use hegel::{TestCase, generators as gs};
use router_clock::RuntimeClock;
use sqlx::Connection;
use switchboard_machine_enrollment::{
    AccessRequest, CredentialStore, EnrollmentBinding, LeaseUnavailable, RefreshAnswer,
    RefreshCredential, RefreshOwner, RefreshSource, SecretValue, StoredEnrollment, TokenGrant,
};

use super::{ENROLLMENT, Fixture, TestWriteFault, generation, row_columns};

struct ScriptedRefresh(Mutex<VecDeque<RefreshAnswer>>);

impl RefreshSource for ScriptedRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected sign-in refresh")
    }
}

fn token(value: &str) -> SecretValue {
    SecretValue::new(value.to_owned()).unwrap()
}

fn grant(binding: &EnrollmentBinding, access: &str, lifetime: Duration) -> RefreshAnswer {
    RefreshAnswer::Rotated(TokenGrant {
        credential: binding.credential.clone(),
        access_token: token(access),
        expires_in: lifetime,
    })
}

#[hegel::test(test_cases = 20)]
fn generated_renewal_histories_reopen_the_exact_committed_rotation(case: TestCase) {
    let retry_after =
        Duration::from_secs(case.draw(gs::integers::<u64>().min_value(1).max_value(8)));
    let lifetime = Duration::from_secs(case.draw(gs::integers::<u64>().min_value(3).max_value(12)));
    let first_fault = if case.draw(gs::booleans()) {
        TestWriteFault::BeforeCommit
    } else {
        TestWriteFault::CommitNotApplied
    };
    let lost_ack_on_second = case.draw(gs::booleans());

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = Fixture::open().await;
            let original = fixture.install().await;
            let rotated = fixture.binding(2, ENROLLMENT, "fixture-rotated-refresh");
            let latest = fixture.binding(3, ENROLLMENT, "fixture-latest-refresh");
            let key = original.key();
            let source = ScriptedRefresh(Mutex::new(VecDeque::from([
                RefreshAnswer::Unavailable {
                    retry_after: Some(retry_after),
                },
                grant(&rotated, "fixture-access-two", lifetime),
                grant(&latest, "fixture-access-three", lifetime),
            ])));
            let owner = RefreshOwner::new(
                fixture.store.clone(),
                source,
                fixture.clock.clone(),
                original.context.clone(),
            );

            for _ in 0..2 {
                assert_eq!(
                    owner.lease_for(&key, AccessRequest::Usable).await,
                    Err(LeaseUnavailable::SignInUnreachable {
                        retry_after: Some(retry_after)
                    })
                );
            }
            assert_eq!(
                fixture.store.read().await,
                Ok(StoredEnrollment::Active(original))
            );
            fixture.clock.advance(retry_after).unwrap();

            fixture.faults.arm(first_fault);
            let first_dispatch = fixture.clock.monotonic_now();
            let retry_at = first_dispatch.after(Duration::from_secs(1));
            let mut lease = std::pin::pin!(owner.lease_for(&key, AccessRequest::Usable));
            tokio::select! {
                result = &mut lease => panic!("renewal skipped its write backoff: {result:?}"),
                () = fixture.clock.not_due_check_now(retry_at) => {}
            }
            assert_eq!(
                fixture.store.read().await.unwrap().generation(),
                generation(1)
            );
            fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
            fixture.clock.advance(Duration::from_secs(1)).unwrap();
            let lease = lease.await.expect("recovered the exact stored rotation");
            assert_eq!(lease.binding_generation, generation(2));
            assert_eq!(lease.token, token("fixture-access-two"));
            assert_eq!(lease.usable_until, first_dispatch.after(lifetime));
            assert_eq!(
                owner.lease_for(&key, AccessRequest::Usable).await,
                Ok(lease.clone())
            );
            assert_eq!(
                fixture.store.read().await,
                Ok(StoredEnrollment::Active(rotated))
            );

            fixture
                .clock
                .advance(
                    lease
                        .usable_until
                        .saturating_duration_since(fixture.clock.monotonic_now()),
                )
                .unwrap();
            if lost_ack_on_second {
                fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
            }
            let lease = owner
                .lease_for(&key, AccessRequest::Usable)
                .await
                .expect("refresh on the expiry boundary");
            assert_eq!(lease.binding_generation, generation(3));
            assert_eq!(lease.token, token("fixture-access-three"));

            let mut database = fixture.connection().await;
            let durable = row_columns(&mut database).await;
            assert_eq!(durable.0, 3);
            database.close().await.unwrap();
            fixture.store.close().await.unwrap();

            let reopened = fixture.another_worker().await;
            assert_eq!(reopened.read().await, Ok(StoredEnrollment::Active(latest)));
            let mut database = fixture.connection().await;
            assert_eq!(row_columns(&mut database).await, durable);
            database.close().await.unwrap();
            reopened.close().await.unwrap();
        });
}
