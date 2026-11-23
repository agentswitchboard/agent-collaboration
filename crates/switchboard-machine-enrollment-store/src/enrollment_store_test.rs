use std::{
    future::Future,
    pin::Pin,
    sync::atomic::Ordering::SeqCst,
    task::{Context, Waker},
    time::Duration,
};

use sqlx::{Connection, SqliteConnection};
use switchboard_machine_core_store::EnrollmentRowCorruption;
use switchboard_machine_enrollment::{InvalidReason, StoreDurability};

use super::enrollment_worker::{TestWriteFault, TestWriteFaults};
use super::*;

#[path = "enrollment_store_fixture.rs"]
mod fixture;
use fixture::{ALTERNATE_ENROLLMENT, ENROLLMENT, Fixture, TOKEN, generation};

#[cfg(debug_assertions)]
#[path = "enrollment_tls_fixture.rs"]
mod tls;

#[path = "enrollment_process_test.rs"]
mod process;

fn assert_queued<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

type RowColumns = (i64, Option<String>, Option<Vec<u8>>, i64, i64);

async fn row_columns(connection: &mut SqliteConnection) -> RowColumns {
    sqlx::query_as("SELECT enrollment_generation, enrollment_json, enrollment_sealed, created_at, updated_at FROM machine_identity")
        .fetch_one(connection)
        .await
        .expect("stored row")
}

#[tokio::test]
async fn adapter_checks_generation_and_phase_and_updates_only_the_enrollment_and_timestamp() {
    let fixture = Fixture::open().await;
    let mut connection = fixture.connection().await;
    let before = row_columns(&mut connection).await;
    assert_eq!(fixture.store.durability(), StoreDurability::Durable);
    assert!(!format!("{:?}", fixture.binding(1, ENROLLMENT, TOKEN)).contains(TOKEN));
    assert_eq!(fixture.store.read().await, Ok(StoredEnrollment::Empty));
    assert_eq!(fixture.keychain.reads.load(SeqCst), 0);

    let bad_successor = fixture.binding(2, ENROLLMENT, TOKEN);
    assert_eq!(
        fixture.store.replace(generation(0), &bad_successor).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(
        fixture
            .store
            .remove(generation(0), EnrollmentEnd::Denied)
            .await,
        Err(CredentialStoreError::Remove)
    );
    assert_eq!(
        fixture
            .store
            .mark_unusable(generation(0), EnrollmentEnd::Denied)
            .await,
        Err(CredentialStoreError::Remove)
    );
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::Denied)
            .await,
        Ok(ConditionalWrite::Superseded)
    );
    assert_eq!(row_columns(&mut connection).await, before);
    assert_eq!(fixture.keychain.reads.load(SeqCst), 0);

    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let binding = fixture.install().await;
    let after = row_columns(&mut connection).await;
    assert_eq!(after.0, 1);
    assert_eq!(after.3, before.3);
    assert_eq!(after.4, fixture.clock.utc_now().timestamp_millis());
    assert!(
        after
            .1
            .as_deref()
            .is_some_and(|json| json.contains("\"state\":\"active\"") && !json.contains(TOKEN))
    );
    let sealed = after.2.as_deref().expect("sealed refresh token");
    assert_eq!(sealed.first(), Some(&1));
    assert!(
        !sealed
            .windows(TOKEN.len())
            .any(|window| window == TOKEN.as_bytes())
    );
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(binding.clone()))
    );
    for path in [
        &fixture.database,
        &fixture.database.with_extension("sqlite-wal"),
    ] {
        let bytes = std::fs::read(path).expect("credential custody file");
        assert!(
            !bytes
                .windows(TOKEN.len())
                .any(|window| window == TOKEN.as_bytes())
        );
    }

    assert_eq!(
        fixture.store.replace(generation(0), &binding).await,
        Ok(ConditionalWrite::Superseded)
    );
    let mut foreign = fixture.binding(2, ENROLLMENT, TOKEN);
    foreign.context.machine_id = "019a0000-0000-7000-8000-000000000099".parse().unwrap();
    assert_eq!(
        fixture.store.replace(generation(1), &foreign).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, after);
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::EndedByCentral)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        fixture
            .store
            .mark_unusable(generation(2), EnrollmentEnd::Revoked)
            .await,
        Err(CredentialStoreError::Remove)
    );
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(2),
            why: EnrollmentEnd::EndedByCentral,
            key: binding.key(),
        })
    );
    let ended = row_columns(&mut connection).await;
    assert_eq!(ended.0, 2);
    assert!(ended.2.is_none());
    assert!(
        ended
            .1
            .is_some_and(|json| json.contains("\"kind\":\"endedByCentral\""))
    );
}

#[tokio::test]
async fn typed_corruption_is_preserved_while_unreadable_active_bytes_can_be_ended() {
    let fixture = Fixture::open().await;
    let mut connection = fixture.connection().await;
    let binding = fixture.install().await;
    let first = row_columns(&mut connection).await;
    let next = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
    fixture.keychain.unavailable.store(true, SeqCst);

    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::KeyUnavailable)
    ));
    assert_eq!(
        fixture.store.replace(generation(1), &next).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, first);
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(2),
            why: EnrollmentEnd::Revoked,
            key: binding.key(),
        })
    );
    fixture.keychain.unavailable.store(false, SeqCst);

    // The old bytes authenticate generation 1, so setting generation 3 cannot
    // produce a usable token. This row still has valid ciphertext framing.
    sqlx::query("UPDATE machine_identity SET enrollment_generation=3, enrollment_json=?, enrollment_sealed=?")
        .bind(first.1)
        .bind(first.2)
        .execute(&mut connection).await.unwrap();
    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::SealedBytesUnreadable)
    ));
    assert_eq!(
        fixture
            .store
            .mark_unusable(generation(3), EnrollmentEnd::LocalCredentialLost)
            .await,
        Ok(ConditionalWrite::Applied)
    );

    sqlx::query("UPDATE machine_identity SET enrollment_json='{'")
        .execute(&mut connection)
        .await
        .unwrap();
    let malformed = row_columns(&mut connection).await;
    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::Corrupt(EnrollmentRowCorruption::Json))
    ));
    assert_eq!(fixture.store.read().await, Err(CredentialStoreError::Read));
    assert_eq!(
        fixture
            .store
            .remove(generation(0), EnrollmentEnd::Revoked)
            .await,
        Err(CredentialStoreError::Remove)
    );
    assert_eq!(row_columns(&mut connection).await, malformed);
    assert!(!format!("{:?}", fixture.detail().await).contains(TOKEN));

    sqlx::query("DELETE FROM machine_identity")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::Corrupt(
            EnrollmentRowCorruption::MissingMachine
        ))
    ));
    assert_eq!(
        fixture.store.replace(generation(0), &next).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(
        fixture
            .store
            .remove(generation(0), EnrollmentEnd::Revoked)
            .await,
        Err(CredentialStoreError::Remove)
    );
    assert_eq!(
        fixture
            .store
            .mark_unusable(generation(0), EnrollmentEnd::Revoked)
            .await,
        Err(CredentialStoreError::Remove)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM machine_identity")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn started_replace_survives_dropped_waiter_and_queued_ending_is_discarded() {
    let fixture = Fixture::open().await;
    fixture.install().await;
    let mut outside = fixture.connection().await;
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
    let (at_keychain, release) = fixture.keychain.hold_next_read();

    let mut started = Box::pin(fixture.store.replace(generation(1), &replacement));
    assert_queued(started.as_mut());
    at_keychain.await.expect("write admitted");
    assert_eq!(row_columns(&mut outside).await.0, 1);

    let mut queued = Box::pin(
        fixture
            .store
            .mark_unusable(generation(2), EnrollmentEnd::Revoked),
    );
    assert_queued(queued.as_mut());
    drop(queued);
    drop(started);
    let mut ordered_read = Box::pin(fixture.store.read());
    assert_queued(ordered_read.as_mut());

    release.release();
    assert_eq!(
        ordered_read.await,
        Ok(StoredEnrollment::Active(replacement))
    );
    assert_eq!(row_columns(&mut outside).await.0, 2);
}

#[tokio::test]
async fn active_read_keeps_its_snapshot_while_another_worker_changes_the_row() {
    for ending in [false, true] {
        let fixture = Fixture::open().await;
        let first = fixture.install().await;
        let other = fixture.another_worker().await;
        let (at_keychain, release) = fixture.keychain.hold_next_read();
        let mut read = Box::pin(fixture.store.read());
        assert_queued(read.as_mut());
        at_keychain.await.expect("read got its snapshot");

        let next = fixture.binding(2, ALTERNATE_ENROLLMENT, "test-replacement-refresh");
        let expected = if ending {
            assert_eq!(
                other.remove(generation(1), EnrollmentEnd::Inactive).await,
                Ok(ConditionalWrite::Applied)
            );
            StoredEnrollment::Ended {
                generation: generation(2),
                why: EnrollmentEnd::Inactive,
                key: first.key(),
            }
        } else {
            assert_eq!(
                other.replace(generation(1), &next).await,
                Ok(ConditionalWrite::Applied)
            );
            StoredEnrollment::Active(next)
        };
        release.release();

        assert_eq!(read.await, Ok(StoredEnrollment::Active(first)));
        assert_eq!(fixture.store.read().await, Ok(expected));
    }
}

#[tokio::test]
async fn a_held_keychain_does_not_hold_the_sqlite_writer_and_busy_is_definite() {
    let fixture = Fixture::open().await;
    fixture.install().await;
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
    let mut outside = fixture.connection().await;
    let (at_keychain, release) = fixture.keychain.hold_next_read();
    let mut replace = Box::pin(fixture.store.replace(generation(1), &replacement));
    assert_queued(replace.as_mut());
    at_keychain.await.expect("preflight reached Keychain");

    sqlx::query("PRAGMA busy_timeout=0")
        .execute(&mut outside)
        .await
        .unwrap();
    let held_writer = outside.begin_with("BEGIN IMMEDIATE").await.unwrap();
    release.release();

    assert_eq!(replace.await, Err(CredentialStoreError::Replace));
    held_writer.rollback().await.unwrap();
    assert_eq!(row_columns(&mut outside).await.0, 1);
    assert_eq!(fixture.faults.probes.load(SeqCst), 0);
    assert_eq!(
        fixture.store.replace(generation(1), &replacement).await,
        Ok(ConditionalWrite::Applied)
    );
}

#[tokio::test]
async fn issued_commit_errors_probe_once_without_manufacturing_applied() {
    let fixture = Fixture::open().await;
    let binding = fixture.binding(1, ENROLLMENT, TOKEN);
    fixture.faults.arm(TestWriteFault::BeforeCommit);
    assert_eq!(
        fixture.store.replace(generation(0), &binding).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 0);

    fixture.faults.arm(TestWriteFault::CommitNotApplied);
    assert_eq!(
        fixture.store.replace(generation(0), &binding).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 1);
    assert_eq!(fixture.store.read().await, Ok(StoredEnrollment::Empty));

    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
    assert_eq!(
        fixture.store.replace(generation(0), &binding).await,
        Err(CredentialStoreError::OutcomeNotKnown)
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 2);
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(binding.clone()))
    );

    fixture.faults.arm(TestWriteFault::LoseAcknowledgement);
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::EndedByCentral)
            .await,
        Err(CredentialStoreError::OutcomeNotKnown)
    );
    assert_eq!(fixture.faults.probes.load(SeqCst), 3);
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Ended {
            generation: generation(2),
            why: EnrollmentEnd::EndedByCentral,
            key: binding.key(),
        })
    );
}

#[tokio::test]
async fn the_last_active_generation_can_end_but_the_last_ending_cannot_replace() {
    let fixture = Fixture::open().await;
    let mut connection = fixture.connection().await;
    let last_active_predecessor = u64::try_from(i64::MAX).unwrap() - 2;
    let binding = fixture.install().await;
    assert_eq!(
        fixture
            .store
            .remove(binding.generation, EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    sqlx::query("UPDATE machine_identity SET enrollment_generation=?")
        .bind(i64::try_from(last_active_predecessor).unwrap())
        .execute(&mut connection)
        .await
        .unwrap();

    let binding = fixture.binding(last_active_predecessor + 1, ENROLLMENT, TOKEN);
    assert_eq!(
        fixture
            .store
            .replace(generation(last_active_predecessor), &binding)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        fixture
            .store
            .remove(binding.generation, EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    let final_generation = generation(last_active_predecessor + 2);
    let last = row_columns(&mut connection).await;
    assert_eq!(last.0, i64::MAX);
    assert_eq!(
        fixture.store.replace(final_generation, &binding).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, last);
}

#[tokio::test]
async fn every_ending_reason_round_trips_through_the_adapter() {
    let fixture = Fixture::open().await;
    let invalid = [
        InvalidReason::GrantRefused,
        InvalidReason::InvalidClient,
        InvalidReason::InvalidRequest,
        InvalidReason::UnknownError,
        InvalidReason::MalformedAnswer,
        InvalidReason::UnexpectedStatus,
        InvalidReason::WrongEnrollment,
        InvalidReason::OversizedAnswer,
    ];
    let reasons = [
        EnrollmentEnd::Denied,
        EnrollmentEnd::Expired,
        EnrollmentEnd::RefreshRefused,
        EnrollmentEnd::RefreshClientDisabled,
        EnrollmentEnd::EndedByCentral,
        EnrollmentEnd::LocalCredentialLost,
        EnrollmentEnd::Revoked,
        EnrollmentEnd::Inactive,
    ]
    .into_iter()
    .chain(invalid.into_iter().map(EnrollmentEnd::Invalid));
    for (index, why) in reasons.enumerate() {
        let before = u64::try_from(index).unwrap() * 2;
        let active = fixture.binding(before + 1, ENROLLMENT, TOKEN);
        assert_eq!(
            fixture.store.replace(generation(before), &active).await,
            Ok(ConditionalWrite::Applied)
        );
        assert_eq!(
            fixture.store.remove(active.generation, why).await,
            Ok(ConditionalWrite::Applied)
        );
        assert_eq!(
            fixture.store.read().await,
            Ok(StoredEnrollment::Ended {
                generation: generation(before + 2),
                key: active.key(),
                why,
            })
        );
    }
}

#[tokio::test]
async fn unsealed_witness_cannot_rekey_a_concurrently_published_enrollment() {
    use std::fs::{OpenOptions, TryLockError};

    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    assert_eq!(
        fixture
            .store
            .remove(original.generation, EnrollmentEnd::Inactive)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    let other = fixture.another_worker().await;
    let attempt = fixture.binding(3, ENROLLMENT, TOKEN);
    let competitor = fixture.binding(3, ALTERNATE_ENROLLMENT, "fictional-competitor");
    let (at_witness, release) = fixture.faults.hold_replacement_witness();
    let mut replacing = Box::pin(fixture.store.replace(generation(2), &attempt));
    assert_queued(replacing.as_mut());
    at_witness.await.unwrap();

    let boundary = OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.secret_root.join(".store.lock"))
        .unwrap();
    match boundary.try_lock() {
        Err(TryLockError::WouldBlock) => {
            release.send(()).unwrap();
            assert_eq!(replacing.await, Ok(ConditionalWrite::Applied));
            assert_eq!(
                other.replace(generation(2), &competitor).await,
                Ok(ConditionalWrite::Superseded)
            );
        }
        Ok(()) => {
            // If the witness was taken without the lock, force the ordering
            // that leaves a sealed row and no Keychain item.
            boundary.unlock().unwrap();
            assert_eq!(
                other.replace(generation(2), &competitor).await,
                Ok(ConditionalWrite::Applied)
            );
            fixture.keychain.delete_item();
            release.send(()).unwrap();
            assert_eq!(replacing.await, Ok(ConditionalWrite::Superseded));
        }
        Err(error) => panic!("cannot inspect enrollment coordination lock: {error}"),
    }
    assert_eq!(
        fixture.keychain.additions.load(SeqCst),
        1,
        "a stale unsealed witness created a new key"
    );
    assert!(matches!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(_))
    ));
}

#[tokio::test]
async fn replace_preserves_active_ciphertext_that_fails_authentication() {
    let fixture = Fixture::open().await;
    let installed = fixture.install().await;
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(installed))
    );
    let mut connection = fixture.connection().await;
    let mut damaged = row_columns(&mut connection).await.2.unwrap();
    *damaged.last_mut().unwrap() ^= 1;
    sqlx::query("UPDATE machine_identity SET enrollment_sealed = ?")
        .bind(damaged)
        .execute(&mut connection)
        .await
        .unwrap();
    let unreadable = row_columns(&mut connection).await;
    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::SealedBytesUnreadable)
    ));

    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, "fictional-replacement-refresh");
    assert_eq!(
        fixture.store.replace(generation(1), &replacement).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, unreadable);

    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    assert!(row_columns(&mut connection).await.2.is_none());
}

#[tokio::test]
async fn lost_worker_replies_are_definite_for_preflight_and_queued_requests() {
    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    let faults = Arc::new(TestWriteFaults::default());
    let (store, worker) = fixture.another_tracked_worker(faults.clone()).await;
    let next = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
    let (at_witness, release) = faults.hold_replacement_witness();
    let mut preflight = Box::pin(store.replace(generation(1), &next));
    assert_queued(preflight.as_mut());
    at_witness.await.unwrap();

    let mut queued = Box::pin(store.remove(generation(1), EnrollmentEnd::Revoked));
    assert_queued(queued.as_mut());
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    drop(release);

    assert_eq!(
        (preflight.await, queued.await),
        (
            Err(CredentialStoreError::Replace),
            Err(CredentialStoreError::Remove)
        )
    );
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(original))
    );
}
