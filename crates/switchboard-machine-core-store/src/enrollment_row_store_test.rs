use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};

use super::*;
use crate::{MACHINE_LOCAL_DATABASE, StoredEnrollmentEnd, StoredInvalidEnrollmentReason};

const MACHINE: &str = "019a0000-0000-7000-8000-000000000005";
const OTHER_MACHINE: &str = "019a0000-0000-7000-8000-000000000006";
const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000007";
const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000008";
const DEPLOYMENT: &str = "https://signin.example/";
const CREATED: i64 = 1_795_000_000_123;

struct Fixture {
    root: tempfile::TempDir,
    store: MachineLocalStore,
    machine: MachineId,
}

impl Fixture {
    async fn open() -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = MachineLocalStore::open(&root.path().join(MACHINE_LOCAL_DATABASE))
            .await
            .unwrap();
        Self {
            root,
            store,
            machine: MACHINE.parse().unwrap(),
        }
    }

    async fn insert_machine(&mut self, id: &str) {
        sqlx::query(
            "INSERT INTO machine_identity (machine_id, created_at, updated_at) VALUES (?, ?, ?)",
        )
        .bind(id)
        .bind(CREATED)
        .bind(CREATED)
        .execute(&mut self.store.connection)
        .await
        .unwrap();
    }

    async fn raw_row(&mut self) -> (i64, Option<String>, Option<Vec<u8>>, i64, i64) {
        sqlx::query_as(
            "SELECT enrollment_generation, enrollment_json, enrollment_sealed, created_at, updated_at
               FROM machine_identity",
        )
        .fetch_one(&mut self.store.connection)
        .await
        .unwrap()
    }

    async fn set_columns(&mut self, generation: i64, json: Option<&str>, sealed: Option<&[u8]>) {
        sqlx::query(
            "UPDATE machine_identity SET enrollment_generation = ?, enrollment_json = ?, enrollment_sealed = ?",
        )
        .bind(generation)
        .bind(json)
        .bind(sealed)
        .execute(&mut self.store.connection)
        .await
        .unwrap();
    }

    async fn read(&mut self) -> Result<StoredEnrollmentRow, EnrollmentRowStoreError> {
        self.store.read_enrollment_row(&self.machine).await
    }
}

fn active(generation: i64) -> StoredEnrollmentRow {
    StoredEnrollmentRow::Active {
        generation,
        organization_id: ORGANIZATION.to_owned(),
        deployment: DEPLOYMENT.to_owned(),
        enrollment_id: ENROLLMENT.to_owned(),
        // Deliberately unauthentic ciphertext with the correct v1 framing.
        sealed: [vec![1], vec![0x42; 29]].concat(),
    }
}

fn active_metadata() -> Value {
    json!({
        "state": "active",
        "organizationId": ORGANIZATION,
        "deployment": DEPLOYMENT,
        "enrollmentId": ENROLLMENT,
    })
}

fn ended(generation: i64) -> StoredEnrollmentRow {
    StoredEnrollmentRow::Ended {
        generation,
        organization_id: ORGANIZATION.to_owned(),
        deployment: DEPLOYMENT.to_owned(),
        enrollment_id: ENROLLMENT.to_owned(),
        why: StoredEnrollmentEnd::Invalid {
            reason: StoredInvalidEnrollmentReason::WrongEnrollment,
        },
    }
}

#[tokio::test]
async fn writing_enrollment_keeps_creation_time_and_stores_the_pinned_json() {
    let mut fixture = Fixture::open().await;
    fixture.insert_machine(MACHINE).await;
    let empty = fixture.read().await.unwrap();
    assert_eq!(empty, StoredEnrollmentRow::Empty);
    let mut commits = fixture.store.subscribe_commits();

    let applied = fixture
        .store
        .write_enrollment_row(&fixture.machine, 0, CREATED + 10, |row| {
            assert_eq!(row, StoredEnrollmentRow::Empty);
            EnrollmentRowChange::<()>::Write(active(1))
        })
        .await
        .unwrap();

    assert_eq!(applied, EnrollmentRowWrite::Applied);
    assert!(commits.has_changed().unwrap());
    commits.borrow_and_update();
    let (generation, metadata, sealed, created, updated) = fixture.raw_row().await;
    let metadata: Value = serde_json::from_str(&metadata.unwrap()).unwrap();
    assert_eq!(metadata, active_metadata());
    assert_eq!((generation, created, updated), (1, CREATED, CREATED + 10));
    assert_eq!(sealed, Some([vec![1], vec![0x42; 29]].concat()));
    assert_eq!(fixture.read().await.unwrap(), active(1));
    assert!(!commits.has_changed().unwrap());

    let applied = fixture
        .store
        .write_enrollment_row(&fixture.machine, 1, CREATED + 20, |row| {
            assert_eq!(row, active(1));
            EnrollmentRowChange::<()>::Write(ended(2))
        })
        .await
        .unwrap();

    assert_eq!(applied, EnrollmentRowWrite::Applied);
    let (generation, metadata, sealed, created, updated) = fixture.raw_row().await;
    let metadata: Value = serde_json::from_str(&metadata.unwrap()).unwrap();
    assert_eq!(
        metadata,
        json!({
            "state": "ended",
            "organizationId": ORGANIZATION,
            "deployment": DEPLOYMENT,
            "enrollmentId": ENROLLMENT,
            "why": {"kind": "invalid", "reason": "wrongEnrollment"},
        })
    );
    assert_eq!((generation, created, updated), (2, CREATED, CREATED + 20));
    assert_eq!(sealed, None);
    assert_eq!(fixture.read().await.unwrap(), ended(2));
}

#[tokio::test]
async fn generation_and_domain_validation_fence_a_core_write() {
    let mut fixture = Fixture::open().await;
    fixture.insert_machine(MACHINE).await;
    let before = fixture.raw_row().await;

    let stale = fixture
        .store
        .write_enrollment_row(&fixture.machine, 1, CREATED + 10, |_| {
            EnrollmentRowChange::Abort("wrong state")
        })
        .await
        .unwrap();
    assert_eq!(stale, EnrollmentRowWrite::Superseded);
    let wrong_state = fixture
        .store
        .write_enrollment_row(&fixture.machine, 0, CREATED + 20, |_| {
            EnrollmentRowChange::Abort("wrong state")
        })
        .await
        .unwrap();
    assert_eq!(wrong_state, EnrollmentRowWrite::Aborted("wrong state"));
    let invalid_domain = fixture
        .store
        .write_enrollment_row(&fixture.machine, 7, CREATED + 30, |_| {
            EnrollmentRowChange::<()>::Corrupt(EnrollmentRowCorruption::Deployment)
        })
        .await;
    assert!(matches!(
        invalid_domain,
        Err(EnrollmentRowStoreError::Corrupt(
            EnrollmentRowCorruption::Deployment
        ))
    ));
    let skipped_generation = fixture
        .store
        .write_enrollment_row(&fixture.machine, 0, CREATED + 40, |_| {
            EnrollmentRowChange::<()>::Write(active(2))
        })
        .await;
    assert!(matches!(
        skipped_generation,
        Err(EnrollmentRowStoreError::InvalidTransition)
    ));
    assert_eq!(fixture.raw_row().await, before);
}

#[tokio::test]
async fn corrupt_enrollment_is_preserved_and_refused_before_cas() {
    let mut fixture = Fixture::open().await;
    fixture.insert_machine(MACHINE).await;
    let metadata = active_metadata().to_string();
    let with_extra = {
        let mut value = active_metadata();
        value["refreshToken"] = Value::Null;
        value.to_string()
    };
    let bad_id = {
        let mut value = active_metadata();
        value["organizationId"] = Value::String("not-a-uuid".to_owned());
        value.to_string()
    };
    let unsupported_reason = json!({
        "state": "ended", "organizationId": ORGANIZATION, "enrollmentId": ENROLLMENT,
        "deployment": DEPLOYMENT,
        "why": {"kind": "invalid", "reason": "novelReason"},
    })
    .to_string();
    let extra_reason_field = json!({
        "state": "ended", "organizationId": ORGANIZATION, "enrollmentId": ENROLLMENT,
        "deployment": DEPLOYMENT,
        "why": {"kind": "revoked", "extra": true},
    })
    .to_string();
    let ended_json = json!({
        "state": "ended", "organizationId": ORGANIZATION, "enrollmentId": ENROLLMENT,
        "deployment": DEPLOYMENT,
        "why": {"kind": "revoked"},
    })
    .to_string();
    let mut sealed = [0u8; 30];
    sealed[0] = 1;
    let cases = [
        (
            0,
            Some(metadata.as_str()),
            Some(sealed.as_slice()),
            EnrollmentRowCorruption::Shape,
        ),
        (
            1,
            Some(metadata.as_str()),
            None,
            EnrollmentRowCorruption::Shape,
        ),
        (
            1,
            Some(ended_json.as_str()),
            Some(sealed.as_slice()),
            EnrollmentRowCorruption::Shape,
        ),
        (
            1,
            Some(with_extra.as_str()),
            Some(sealed.as_slice()),
            EnrollmentRowCorruption::Json,
        ),
        (
            1,
            Some(bad_id.as_str()),
            Some(sealed.as_slice()),
            EnrollmentRowCorruption::EnrollmentId,
        ),
        (
            1,
            Some(unsupported_reason.as_str()),
            None,
            EnrollmentRowCorruption::Json,
        ),
        (
            1,
            Some(extra_reason_field.as_str()),
            None,
            EnrollmentRowCorruption::Json,
        ),
        (1, Some("{"), None, EnrollmentRowCorruption::Json),
        (
            1,
            Some(metadata.as_str()),
            Some(&[1][..]),
            EnrollmentRowCorruption::SealedFormat,
        ),
        (
            1,
            Some(metadata.as_str()),
            Some(&[2; 30][..]),
            EnrollmentRowCorruption::SealedFormat,
        ),
        (
            i64::MAX,
            Some(metadata.as_str()),
            Some(sealed.as_slice()),
            EnrollmentRowCorruption::Generation,
        ),
    ];
    for (generation, json, sealed, expected) in cases {
        fixture.set_columns(generation, json, sealed).await;
        let before = fixture.raw_row().await;

        assert!(
            matches!(fixture.read().await, Err(EnrollmentRowStoreError::Corrupt(found)) if found == expected)
        );
        let called = AtomicBool::new(false);
        let result = fixture
            .store
            .write_enrollment_row(&fixture.machine, 71, CREATED + 1, |_| {
                called.store(true, Ordering::SeqCst);
                EnrollmentRowChange::<()>::Write(active(72))
            })
            .await;
        assert!(
            matches!(result, Err(EnrollmentRowStoreError::Corrupt(found)) if found == expected)
        );
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(fixture.raw_row().await, before);
    }
}

#[tokio::test]
async fn enrollment_refuses_missing_wrong_or_multiple_machine_rows_without_repair() {
    let mut fixture = Fixture::open().await;
    let missing = fixture
        .store
        .write_enrollment_row(&fixture.machine, 0, CREATED, |_| {
            EnrollmentRowChange::<()>::Write(active(1))
        })
        .await;
    assert!(matches!(
        missing,
        Err(EnrollmentRowStoreError::Corrupt(
            EnrollmentRowCorruption::MissingMachine
        ))
    ));
    assert!(matches!(
        fixture.read().await,
        Err(EnrollmentRowStoreError::Corrupt(
            EnrollmentRowCorruption::MissingMachine
        ))
    ));

    fixture.insert_machine(OTHER_MACHINE).await;
    assert!(matches!(
        fixture.read().await,
        Err(EnrollmentRowStoreError::Corrupt(
            EnrollmentRowCorruption::WrongMachine
        ))
    ));
    sqlx::query("DROP INDEX machine_identity_singleton")
        .execute(&mut fixture.store.connection)
        .await
        .unwrap();
    fixture.insert_machine(MACHINE).await;
    assert!(matches!(
        fixture.read().await,
        Err(EnrollmentRowStoreError::Corrupt(
            EnrollmentRowCorruption::MultipleMachines
        ))
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM machine_identity WHERE enrollment_generation = 0")
            .fetch_one(&mut fixture.store.connection)
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn central_ending_round_trips_as_its_own_kind() {
    let mut fixture = Fixture::open().await;
    fixture.insert_machine(MACHINE).await;
    let metadata = active_metadata().to_string();
    let mut opaque_sealed = [0; 30];
    opaque_sealed[0] = 1;
    fixture
        .set_columns(1, Some(&metadata), Some(&opaque_sealed))
        .await;

    let stored = StoredEnrollmentRow::Ended {
        generation: 2,
        organization_id: ORGANIZATION.to_owned(),
        deployment: DEPLOYMENT.to_owned(),
        enrollment_id: ENROLLMENT.to_owned(),
        why: StoredEnrollmentEnd::EndedByCentral {},
    };
    let result = fixture
        .store
        .write_enrollment_row(&fixture.machine, 1, CREATED + 1, |_| {
            EnrollmentRowChange::<()>::Write(stored.clone())
        })
        .await
        .unwrap();

    assert_eq!(result, EnrollmentRowWrite::Applied);
    let (_, encoded, sealed, _, _) = fixture.raw_row().await;
    let encoded: Value = serde_json::from_str(&encoded.unwrap()).unwrap();
    assert_eq!(encoded["why"], json!({"kind": "endedByCentral"}));
    assert_eq!(sealed, None);
    assert_eq!(fixture.read().await.unwrap(), stored);
}

#[tokio::test]
async fn ordered_queue_can_take_a_row_snapshot_without_reserving_the_sqlite_writer() {
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

    let mut fixture = Fixture::open().await;
    fixture.insert_machine(MACHINE).await;
    let options =
        SqliteConnectOptions::new().filename(fixture.root.path().join(MACHINE_LOCAL_DATABASE));
    let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("PRAGMA busy_timeout = 0")
        .execute(&mut fixture.store.connection)
        .await
        .unwrap();
    let mut transaction = writer.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE machine_identity SET enrollment_generation = 1, enrollment_json = ?, enrollment_sealed = ?")
        .bind(active_metadata().to_string())
        .bind([vec![1], vec![0x42; 29]].concat())
        .execute(&mut *transaction)
        .await
        .unwrap();

    assert_eq!(fixture.read().await.unwrap(), StoredEnrollmentRow::Empty);

    transaction.commit().await.unwrap();
    assert_eq!(fixture.read().await.unwrap(), active(1));
}
