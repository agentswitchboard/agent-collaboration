use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc as blocking,
    },
};

use codex_router_secret_store::keychain_data_key::{
    KeychainAccess, KeychainAccessError, ROUTER_KEYCHAIN_SERVICE,
};
use router_clock::{ManualClock, RuntimeClock};
use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use switchboard_machine_enrollment::{
    EnrollmentContext, RefreshCredential, SecretValue, SignInDeployment,
};
use tokio::sync::oneshot;

use super::*;

const START: i64 = 1_795_000_000_123;
pub(super) const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000001";
pub(super) const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000002";
pub(super) const ALTERNATE_ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000003";
pub(super) const TOKEN: &str = "fictional-enrollment-row-fixture-token";

pub(super) fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value).expect("fixture generation")
}

pub(super) struct Fixture {
    pub(super) store: MachineEnrollmentStore,
    pub(super) faults: Arc<TestWriteFaults>,
    pub(super) keychain: Arc<FixtureKeychain>,
    pub(super) clock: Arc<ManualClock>,
    pub(super) machine: MachineId,
    pub(super) database: PathBuf,
    pub(super) secret_root: PathBuf,
    _temp: tempfile::TempDir,
}

impl Fixture {
    pub(super) async fn open() -> Self {
        Self::with_keychain(Arc::new(FixtureKeychain::default())).await
    }

    pub(super) async fn with_keychain(keychain: Arc<FixtureKeychain>) -> Self {
        let root = tempfile::tempdir().expect("fixture directory");
        let database = root.path().join("machine_local.sqlite");
        let secret_root = root.path().join("secrets");
        let mut core = MachineLocalStore::open(&database)
            .await
            .expect("isolated SQLite");
        let machine = core
            .load_or_create_machine_id(START)
            .await
            .expect("machine");
        core.close().await.expect("close fixture initializer");
        let clock = Arc::new(ManualClock::new(
            "2026-11-16T00:00:00Z".parse().expect("fixture epoch"),
        ));
        let faults = Arc::new(TestWriteFaults::default());
        let store = Self::worker(
            &database,
            secret_root.clone(),
            machine.clone(),
            clock.clone(),
            keychain.clone(),
            faults.clone(),
        )
        .await;
        Self {
            store,
            faults,
            keychain,
            clock,
            machine,
            database,
            secret_root,
            _temp: root,
        }
    }

    pub(super) async fn worker(
        database: &std::path::Path,
        secret_root: PathBuf,
        machine: MachineId,
        clock: Arc<ManualClock>,
        keychain: Arc<dyn KeychainAccess>,
        faults: Arc<TestWriteFaults>,
    ) -> MachineEnrollmentStore {
        Self::tracked_worker(database, secret_root, machine, clock, keychain, faults)
            .await
            .0
    }

    async fn tracked_worker(
        database: &std::path::Path,
        secret_root: PathBuf,
        machine: MachineId,
        clock: Arc<ManualClock>,
        keychain: Arc<dyn KeychainAccess>,
        faults: Arc<TestWriteFaults>,
    ) -> (MachineEnrollmentStore, tokio::task::JoinHandle<()>) {
        let core = MachineLocalStore::open(database)
            .await
            .expect("dedicated fixture connection");
        let clock: Arc<dyn RuntimeClock + Send + Sync> = clock;
        let mut worker = EnrollmentWorker::new(core, secret_root, machine, clock);
        worker.inject_test_dependencies(keychain, faults);
        MachineEnrollmentStore::spawn_worker(worker)
    }

    pub(super) async fn another_tracked_worker(
        &self,
        faults: Arc<TestWriteFaults>,
    ) -> (MachineEnrollmentStore, tokio::task::JoinHandle<()>) {
        Self::tracked_worker(
            &self.database,
            self.secret_root.clone(),
            self.machine.clone(),
            self.clock.clone(),
            self.keychain.clone(),
            faults,
        )
        .await
    }

    pub(super) async fn another_worker(&self) -> MachineEnrollmentStore {
        Self::worker(
            &self.database,
            self.secret_root.clone(),
            self.machine.clone(),
            self.clock.clone(),
            self.keychain.clone(),
            Arc::new(TestWriteFaults::default()),
        )
        .await
    }

    pub(super) async fn connection(&self) -> SqliteConnection {
        let options = SqliteConnectOptions::new()
            .filename(&self.database)
            .journal_mode(SqliteJournalMode::Wal);
        SqliteConnection::connect_with(&options)
            .await
            .expect("independent SQLite connection")
    }

    pub(super) fn binding(&self, version: u64, enrollment: &str, token: &str) -> EnrollmentBinding {
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
                refresh_token: token_value(token),
            },
        }
    }

    pub(super) async fn install(&self) -> EnrollmentBinding {
        let binding = self.binding(1, ENROLLMENT, TOKEN);
        assert_eq!(
            self.store.replace(generation(0), &binding).await,
            Ok(ConditionalWrite::Applied)
        );
        binding
    }

    pub(super) async fn detail(&self) -> Result<StoredEnrollment, ReadFailure> {
        let (reply, result) = oneshot::channel();
        self.store
            .requests
            .send(EnrollmentRequest::Read(reply))
            .unwrap_or_else(|_| panic!("fixture worker closed"));
        result.await.expect("typed reply")
    }
}

pub(super) fn token_value(token: &str) -> SecretValue {
    SecretValue::new(token.to_owned()).expect("fixture credential")
}

#[derive(Default)]
pub(super) struct FixtureKeychain {
    item: Mutex<Option<Vec<u8>>>,
    pub(super) unavailable: AtomicBool,
    pub(super) reads: AtomicUsize,
    pub(super) additions: AtomicUsize,
    pause: Mutex<Option<KeychainPause>>,
    add_pause: Mutex<Option<KeychainPause>>,
}

impl FixtureKeychain {
    pub(super) fn delete_item(&self) {
        *self.item.lock().expect("fixture Keychain item") = None;
    }

    pub(super) fn fixed(key: [u8; 32]) -> Self {
        Self {
            item: Mutex::new(Some(key.to_vec())),
            ..Self::default()
        }
    }

    pub(super) fn hold_next_read(&self) -> (oneshot::Receiver<()>, KeychainRelease) {
        Self::hold(&self.pause)
    }

    pub(super) fn hold_next_add(&self) -> (oneshot::Receiver<()>, KeychainRelease) {
        Self::hold(&self.add_pause)
    }

    fn hold(slot: &Mutex<Option<KeychainPause>>) -> (oneshot::Receiver<()>, KeychainRelease) {
        let (arrived, at_gate) = oneshot::channel();
        let (released, wait) = blocking::channel();
        *slot.lock().expect("key pause") = Some(KeychainPause { arrived, wait });
        (at_gate, KeychainRelease(released))
    }

    fn wait_at(slot: &Mutex<Option<KeychainPause>>) -> Result<(), KeychainAccessError> {
        let pause = slot
            .lock()
            .map_err(|_| KeychainAccessError::Unavailable)?
            .take();
        if let Some(KeychainPause { arrived, wait }) = pause {
            let _ = arrived.send(());
            let _ = wait.recv();
        }
        Ok(())
    }
}

pub(super) struct KeychainRelease(blocking::Sender<()>);

impl KeychainRelease {
    pub(super) fn release(self) {
        let _ = self.0.send(());
    }
}

struct KeychainPause {
    arrived: oneshot::Sender<()>,
    wait: blocking::Receiver<()>,
}

impl KeychainAccess for FixtureKeychain {
    fn read_secret(
        &self,
        service: &str,
        _account: &str,
    ) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        if service != ROUTER_KEYCHAIN_SERVICE {
            return Err(KeychainAccessError::ServiceRejected);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        Self::wait_at(&self.pause)?;
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(KeychainAccessError::Unavailable);
        }
        Ok(self
            .item
            .lock()
            .map_err(|_| KeychainAccessError::Unavailable)?
            .clone())
    }

    fn add_secret(
        &self,
        service: &str,
        _account: &str,
        secret: &[u8],
    ) -> Result<(), KeychainAccessError> {
        if service != ROUTER_KEYCHAIN_SERVICE {
            return Err(KeychainAccessError::ServiceRejected);
        }
        Self::wait_at(&self.add_pause)?;
        let mut item = self
            .item
            .lock()
            .map_err(|_| KeychainAccessError::Unavailable)?;
        if self.unavailable.load(Ordering::SeqCst) || item.is_some() {
            return Err(KeychainAccessError::Unavailable);
        }
        *item = Some(secret.to_vec());
        self.additions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
