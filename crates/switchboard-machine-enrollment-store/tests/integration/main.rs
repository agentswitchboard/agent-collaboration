#![cfg(debug_assertions)]
#![expect(
    clippy::unwrap_used,
    reason = "The SQLite and keychain fixtures must be available."
)]

use std::{sync::Arc, time::Duration};

use router_clock::{ManualClock, RuntimeClock};
use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, MachineLocalStore};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, MachineId, RefreshCredential, SecretValue, SignInDeployment,
    StoredEnrollment,
};
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStore,
    test_support::{KeychainAccess, KeychainAccessError},
};

mod enrollment_preflight;
mod store_lifecycle;

const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000001";
const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000002";
const ALTERNATE_ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000003";
const TOKEN: &str = "fictional-enrollment-row-fixture-token";

struct FixtureKeychain;

impl KeychainAccess for FixtureKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        Ok(Some(vec![0x71; 32]))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}

struct Fixture {
    store: MachineEnrollmentStore,
    clock: Arc<ManualClock>,
    machine: MachineId,
    directory: tempfile::TempDir,
}

impl Fixture {
    async fn open() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join(MACHINE_LOCAL_DATABASE);
        let clock = Arc::new(ManualClock::new("2026-11-16T00:00:00Z".parse().unwrap()));
        let mut core = MachineLocalStore::open(&database).await.unwrap();
        let machine = core
            .load_or_create_machine_id(clock.utc_now().timestamp_millis())
            .await
            .unwrap();
        core.close().await.unwrap();
        let store = MachineEnrollmentStore::open_with_test_keychain(
            &database,
            directory.path().join("secrets"),
            machine.clone(),
            clock.clone(),
            Arc::new(FixtureKeychain),
        )
        .await
        .unwrap();
        Self {
            store,
            clock,
            machine,
            directory,
        }
    }

    async fn connection(&self) -> SqliteConnection {
        let options = SqliteConnectOptions::new()
            .filename(self.directory.path().join(MACHINE_LOCAL_DATABASE))
            .journal_mode(SqliteJournalMode::Wal);
        SqliteConnection::connect_with(&options).await.unwrap()
    }

    fn binding(&self, version: u64, enrollment: &str, token: &str) -> EnrollmentBinding {
        EnrollmentBinding {
            generation: generation(version),
            context: EnrollmentContext {
                machine_id: self.machine.clone(),
                organization_id: ORGANIZATION.parse().unwrap(),
                deployment: SignInDeployment::new("https://signin.example/".parse().unwrap())
                    .unwrap(),
            },
            credential: RefreshCredential {
                enrollment_id: enrollment.parse().unwrap(),
                refresh_token: SecretValue::new(token.to_owned()).unwrap(),
            },
        }
    }

    async fn install(&self) -> EnrollmentBinding {
        let binding = self.binding(1, ENROLLMENT, TOKEN);
        assert_eq!(
            self.store.replace(generation(0), &binding).await,
            Ok(ConditionalWrite::Applied)
        );
        binding
    }
}

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value).unwrap()
}

type RowColumns = (i64, Option<String>, Option<Vec<u8>>, i64, i64, String);

async fn row_columns(connection: &mut SqliteConnection) -> RowColumns {
    sqlx::query_as("SELECT enrollment_generation, enrollment_json, enrollment_sealed, created_at, updated_at, machine_id FROM machine_identity")
        .fetch_one(connection)
        .await
        .unwrap()
}

const OTHER_ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000004";
const OTHER_DEPLOYMENT: &str = "https://other-signin.example/";

async fn assert_scope_refused(phase: Option<EnrollmentEnd>, replacement_scope: ReplacementScope) {
    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    let generation_before = if let Some(why) = phase {
        assert_eq!(
            fixture.store.remove(original.generation, why).await,
            Ok(ConditionalWrite::Applied)
        );
        generation(2)
    } else {
        original.generation
    };
    let mut connection = fixture.connection().await;
    let original_row = row_columns(&mut connection).await;
    let mut signal = fixture.store.subscribe_enrollment_changes();
    let original_signal = *signal.borrow_and_update();
    let mut replacement = fixture.binding(generation_before.get() + 1, ALTERNATE_ENROLLMENT, TOKEN);
    match replacement_scope {
        ReplacementScope::OtherOrganization => {
            replacement.context.organization_id = OTHER_ORGANIZATION.parse().unwrap();
        }
        ReplacementScope::OtherDeployment => {
            replacement.context.deployment =
                SignInDeployment::new(OTHER_DEPLOYMENT.parse().unwrap()).unwrap();
        }
    }
    fixture.clock.advance(Duration::from_secs(1)).unwrap();

    let result = fixture.store.replace(generation_before, &replacement).await;

    assert_eq!(result, Err(CredentialStoreError::EnrollmentScopeMismatch));
    assert_eq!(row_columns(&mut connection).await, original_row);
    assert_eq!(
        fixture.store.read().await.unwrap().generation(),
        generation_before
    );
    assert_eq!(*signal.borrow(), original_signal);
    assert!(!signal.has_changed().unwrap());
}

enum ReplacementScope {
    OtherOrganization,
    OtherDeployment,
}

#[tokio::test]
async fn active_enrollment_refuses_another_organization() {
    assert_scope_refused(None, ReplacementScope::OtherOrganization).await;
}

#[tokio::test]
async fn active_enrollment_refuses_another_deployment() {
    assert_scope_refused(None, ReplacementScope::OtherDeployment).await;
}

#[tokio::test]
async fn ended_enrollment_refuses_another_organization() {
    assert_scope_refused(
        Some(EnrollmentEnd::Revoked),
        ReplacementScope::OtherOrganization,
    )
    .await;
}

#[tokio::test]
async fn ended_enrollment_refuses_another_deployment() {
    assert_scope_refused(
        Some(EnrollmentEnd::Revoked),
        ReplacementScope::OtherDeployment,
    )
    .await;
}

#[tokio::test]
async fn ended_enrollment_accepts_a_new_id_in_its_original_scope() {
    let fixture = Fixture::open().await;
    let mut original = fixture.binding(1, ENROLLMENT, TOKEN);
    original.context.deployment = SignInDeployment::new(OTHER_DEPLOYMENT.parse().unwrap()).unwrap();
    assert_eq!(
        fixture.store.replace(generation(0), &original).await,
        Ok(ConditionalWrite::Applied)
    );
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    let mut replacement = fixture.binding(3, ALTERNATE_ENROLLMENT, TOKEN);
    replacement.context = original.context;

    let result = fixture.store.replace(generation(2), &replacement).await;

    assert_eq!(result, Ok(ConditionalWrite::Applied));
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(replacement))
    );
}

#[tokio::test]
async fn empty_enrollment_accepts_a_new_organization_and_deployment() {
    let fixture = Fixture::open().await;
    let mut binding = fixture.binding(1, ENROLLMENT, TOKEN);
    binding.context.organization_id = OTHER_ORGANIZATION.parse().unwrap();
    binding.context.deployment = SignInDeployment::new(OTHER_DEPLOYMENT.parse().unwrap()).unwrap();

    let result = fixture.store.replace(generation(0), &binding).await;

    assert_eq!(result, Ok(ConditionalWrite::Applied));
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(binding))
    );
}
