use hegel::{TestCase, generators as gs};
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
enum History {
    Remove,
    LostAcknowledgement,
    MarkUnusable,
    AlreadyEnded,
    DifferentKey,
    StaleOwner,
    QueuedDefinitive,
}

// Seven explicit histories guarantee branch coverage. The 56 generated cases
// give eight draws per branch on average while opening one fixture per draw.
// TooSlow measures elapsed time across real SQLite commits and reopen I/O.
#[hegel::test(test_cases = 56, suppress_health_check = [hegel::HealthCheck::TooSlow])]
#[hegel::explicit_test_case(history = History::Remove, revoke = true, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::LostAcknowledgement, revoke = false, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::MarkUnusable, revoke = true, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::AlreadyEnded, revoke = false, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::DifferentKey, revoke = true, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::StaleOwner, revoke = false, rotations = 1_u64)]
#[hegel::explicit_test_case(history = History::QueuedDefinitive, revoke = true, rotations = 4_u64)]
fn generated_retirement_histories_reopen_the_exact_resulting_row(case: TestCase) {
    let history = case.draw(gs::sampled_from(vec![
        History::Remove,
        History::LostAcknowledgement,
        History::MarkUnusable,
        History::AlreadyEnded,
        History::DifferentKey,
        History::StaleOwner,
        History::QueuedDefinitive,
    ]));
    let revoke = case.draw(gs::booleans());
    let rotations = case.draw(gs::integers::<u64>().min_value(1).max_value(4));
    case.note(&format!("retirement history: {history:?}"));
    let why = if revoke {
        EnrollmentEnd::Revoked
    } else {
        EnrollmentEnd::Inactive
    };
    let other_reason = if why == EnrollmentEnd::Revoked {
        EnrollmentEnd::Inactive
    } else {
        EnrollmentEnd::Revoked
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_history(history, why, other_reason, rotations));
}

async fn run_history(
    history: History,
    why: EnrollmentEnd,
    other_reason: EnrollmentEnd,
    rotations: u64,
) {
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
        History::Remove | History::LostAcknowledgement | History::MarkUnusable => {
            match history {
                History::Remove => {}
                History::LostAcknowledgement => {
                    fixture.faults.arm(TestWriteFault::LoseAcknowledgement)
                }
                History::MarkUnusable => fixture.faults.arm(TestWriteFault::CommitNotApplied),
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
                Ok(ConditionalWrite::Superseded)
            );
            ended(&first, 2, why)
        }
        History::AlreadyEnded => {
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
        History::DifferentKey => {
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
        History::StaleOwner | History::QueuedDefinitive => {
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
            if matches!(history, History::QueuedDefinitive) {
                fence.request_definitive(&key, why);
                fence.request_definitive(&key, other_reason);
                assert_eq!(
                    owner.finish_retirement(&key).await,
                    Ok(ConditionalWrite::Applied)
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
