use hegel::{TestCase, generators as gs};
use sqlx::Connection;
use switchboard_machine_enrollment::{
    ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentEnd, StoredEnrollment,
};

use super::{
    ALTERNATE_ENROLLMENT, ENROLLMENT, Fixture, TOKEN, TestWriteFault, generation, row_columns,
};

#[hegel::test(test_cases = 25)]
fn generated_histories_reopen_each_committed_enrollment_row(tc: TestCase) {
    let steps = tc.draw(gs::vecs(gs::booleans()).min_size(4).max_size(8));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut fixture = Fixture::open().await;
        assert_reopened(&mut fixture, &StoredEnrollment::Empty).await;

        // Every history visits all four legal edges. Additional live rotations
        // and the location of stale requests and lost acknowledgements vary.
        for (offset, lose_acknowledgement) in steps.into_iter().enumerate() {
            let expected = u64::try_from(offset).unwrap();
            if expected == 2 {
                let active = fixture.binding(2, ENROLLMENT, TOKEN);
                let mut database = fixture.connection().await;
                let before = row_columns(&mut database).await;
                assert_eq!(before.0, 2);
                assert_eq!(
                    fixture
                        .store
                        .remove(generation(1), EnrollmentEnd::Revoked)
                        .await,
                    Ok(ConditionalWrite::Superseded),
                    "an ending one generation behind the locked active row is stale"
                );
                assert_eq!(
                    row_columns(&mut database).await,
                    before,
                    "stale ending changed a stored column"
                );
                database.close().await.unwrap();
                if lose_acknowledgement {
                    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
                }
                assert_eq!(
                    fixture
                        .store
                        .remove(generation(2), EnrollmentEnd::Revoked)
                        .await,
                    write_outcome(lose_acknowledgement)
                );
                let ended = StoredEnrollment::Ended {
                    generation: generation(3),
                    key: active.key(),
                    deployment: active.context.deployment,
                    why: EnrollmentEnd::Revoked,
                };
                assert_reopened(&mut fixture, &ended).await;
                continue;
            }
            let enrollment = if expected <= 1 {
                ENROLLMENT
            } else {
                ALTERNATE_ENROLLMENT
            };
            let binding = fixture.binding(expected + 1, enrollment, TOKEN);
            if expected > 0 {
                let stale = fixture.binding(expected, enrollment, "fictional-stale-credential");
                assert_eq!(
                    fixture
                        .store
                        .replace(generation(expected - 1), &stale)
                        .await,
                    Ok(ConditionalWrite::Superseded)
                );
            }
            if lose_acknowledgement {
                fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
            }
            assert_eq!(
                fixture.store.replace(generation(expected), &binding).await,
                write_outcome(lose_acknowledgement)
            );
            assert_reopened(&mut fixture, &StoredEnrollment::Active(binding)).await;
        }
        fixture.store.close().await.unwrap();
    });
}

fn write_outcome(lose_acknowledgement: bool) -> Result<ConditionalWrite, CredentialStoreError> {
    if lose_acknowledgement {
        Err(CredentialStoreError::OutcomeNotKnown)
    } else {
        Ok(ConditionalWrite::Applied)
    }
}

async fn assert_reopened(fixture: &mut Fixture, expected: &StoredEnrollment) {
    fixture.store.close().await.unwrap();
    fixture.store = Fixture::worker(
        &fixture.database,
        fixture.secret_root.clone(),
        fixture.machine.clone(),
        fixture.clock.clone(),
        fixture.keychain.clone(),
        fixture.faults.clone(),
    )
    .await;
    assert_eq!(fixture.store.read().await.as_ref(), Ok(expected));
}
