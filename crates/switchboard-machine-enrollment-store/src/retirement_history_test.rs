use sqlx::Connection;
use switchboard_machine_enrollment::{
    AccessRequest, ConditionalWrite, CredentialStore, EnrollmentBinding, EnrollmentEnd,
    InvalidReason, LeaseUnavailable, RefreshAnswer, RefreshCredential, RefreshOwner, RefreshSource,
    StoredEnrollment,
};

use super::{
    ALTERNATE_ENROLLMENT, ENROLLMENT, Fixture, TOKEN, TestWriteFault, generation, row_columns,
};

struct RefusedRefresh;

impl RefreshSource for RefusedRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RetirementHistory {
    Remove,
    LostAcknowledgement,
    MarkUnusable,
    AlreadyEnded,
    DifferentKey,
    StaleOwner,
    QueuedDefinitive,
}

#[tokio::test]
async fn every_retirement_history_reopens_the_exact_resulting_row() {
    // Each of the 56 combinations opens and reopens its own SQLite fixture.
    for history in [
        RetirementHistory::Remove,
        RetirementHistory::LostAcknowledgement,
        RetirementHistory::MarkUnusable,
        RetirementHistory::AlreadyEnded,
        RetirementHistory::DifferentKey,
        RetirementHistory::StaleOwner,
        RetirementHistory::QueuedDefinitive,
    ] {
        for (why, other_reason) in [
            (EnrollmentEnd::Revoked, EnrollmentEnd::Inactive),
            (EnrollmentEnd::Inactive, EnrollmentEnd::Revoked),
        ] {
            for rotations in 1..=4 {
                run_history(history, why, other_reason, rotations).await;
            }
        }
    }
}

async fn run_history(
    history: RetirementHistory,
    why: EnrollmentEnd,
    other_reason: EnrollmentEnd,
    rotations: u64,
) {
    use RetirementHistory::*;

    let fixture = Fixture::open().await;
    let first = fixture.install().await;
    let key = first.key();
    let owner = RefreshOwner::new(
        fixture.store.clone(),
        RefusedRefresh,
        fixture.clock.clone(),
        first.context.clone(),
    );
    owner.supervise_retirement();
    let fence = owner.retirement_fence();

    let expected = match history {
        Remove | LostAcknowledgement | MarkUnusable => {
            match history {
                Remove => {}
                LostAcknowledgement => fixture.faults.arm(TestWriteFault::LoseAcknowledgement),
                MarkUnusable => fixture.faults.arm(TestWriteFault::CommitNotApplied),
                _ => {}
            }
            assert_eq!(
                owner.end_enrollment(&key, why).await,
                Ok(ConditionalWrite::Applied)
            );
            assert_eq!(
                owner.end_enrollment(&key, why).await,
                Ok(ConditionalWrite::Applied)
            );
            assert_eq!(
                owner.end_enrollment(&key, other_reason).await,
                Ok(ConditionalWrite::Superseded),
                "{history:?}, {why:?}, {rotations} rotations"
            );
            ended(&first, 2, why)
        }
        AlreadyEnded => {
            assert_eq!(
                fixture.store.remove(generation(1), other_reason).await,
                Ok(ConditionalWrite::Applied)
            );
            assert_eq!(
                owner.end_enrollment(&key, why).await,
                Ok(ConditionalWrite::Applied)
            );
            assert_eq!(
                owner.end_enrollment(&key, why).await,
                Ok(ConditionalWrite::Superseded)
            );
            assert_eq!(
                owner.end_enrollment(&key, other_reason).await,
                Ok(ConditionalWrite::Applied)
            );
            ended(&first, 2, other_reason)
        }
        DifferentKey => {
            let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
            assert_eq!(
                fixture.store.replace(generation(1), &replacement).await,
                Ok(ConditionalWrite::Applied)
            );
            assert_eq!(
                owner.end_enrollment(&key, why).await,
                Ok(ConditionalWrite::Superseded)
            );
            StoredEnrollment::Active(replacement)
        }
        StaleOwner | QueuedDefinitive => {
            assert_eq!(
                owner.lease(AccessRequest::Usable).await,
                Err(LeaseUnavailable::Ended {
                    why: EnrollmentEnd::RefreshRefused
                })
            );
            assert!(fence.is_fenced(&key));
            let last = rotations + 1;
            for version in 2..=last {
                let binding = fixture.binding(version, ENROLLMENT, TOKEN);
                assert_eq!(
                    fixture
                        .store
                        .replace(generation(version - 1), &binding)
                        .await,
                    Ok(ConditionalWrite::Applied)
                );
            }
            if matches!(history, QueuedDefinitive) {
                fence.request_definitive(&key, why);
                fence.request_definitive(&key, other_reason);
                assert_eq!(
                    owner.finish_retirement(&key).await,
                    Ok(ConditionalWrite::Applied),
                    "{history:?}, {why:?}, {rotations} rotations"
                );
                assert!(fence.is_fenced(&key));
                ended(&first, last + 1, why)
            } else {
                assert_eq!(
                    owner.finish_retirement(&key).await,
                    Ok(ConditionalWrite::Superseded)
                );
                assert!(!fence.is_fenced(&key));
                StoredEnrollment::Active(fixture.binding(last, ENROLLMENT, TOKEN))
            }
        }
    };
    assert!(fence.pending_keys().is_empty());
    assert_eq!(fixture.store.read().await.as_ref(), Ok(&expected));
    let mut database = fixture.connection().await;
    let columns = row_columns(&mut database).await;
    assert_eq!(
        columns.0,
        i64::try_from(expected.generation().get()).unwrap()
    );
    assert_eq!(
        columns.2.is_none(),
        matches!(expected, StoredEnrollment::Ended { .. })
    );
    database.close().await.unwrap();

    fixture.store.close().await.unwrap();
    let reopened = fixture.another_worker().await;
    assert_eq!(reopened.read().await, Ok(expected));
    let mut database = fixture.connection().await;
    assert_eq!(row_columns(&mut database).await, columns);
    database.close().await.unwrap();
    reopened.close().await.unwrap();
}

fn ended(binding: &EnrollmentBinding, version: u64, why: EnrollmentEnd) -> StoredEnrollment {
    StoredEnrollment::Ended {
        generation: generation(version),
        why,
        key: binding.key(),
        deployment: binding.context.deployment.clone(),
    }
}
