use std::{
    io::{Read, Write},
    path::PathBuf,
    process::Stdio,
};

use codex_router_secret_store::keychain_data_key::{KeychainAccess, KeychainAccessError};
use router_clock::ManualClock;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
};

use super::{
    fixture::{FixtureKeychain, ORGANIZATION, token_value},
    *,
};
use switchboard_machine_enrollment::{EnrollmentContext, RefreshCredential, SignInDeployment};

const CHILD_TEST: &str = "tests::process::enrollment_process_actor";
const BARRIER: &str = "ENROLLMENT_PROCESS_READY";
const DB_ENV: &str = "ROUTER_ENROLLMENT_TEST_DB";
const ROOT_ENV: &str = "ROUTER_ENROLLMENT_TEST_SECRET_ROOT";
const MACHINE_ENV: &str = "ROUTER_ENROLLMENT_TEST_MACHINE";
const WHICH_ENV: &str = "ROUTER_ENROLLMENT_TEST_WRITER";
const ITEMS_ENV: &str = "ROUTER_ENROLLMENT_TEST_KEYCHAIN";
const FIXED_KEY: [u8; 32] = [0x6c; 32];

fn wait_for_release() -> Result<(), KeychainAccessError> {
    let mut output = std::io::stdout().lock();
    writeln!(output, "\n{BARRIER}").map_err(|_| KeychainAccessError::Unavailable)?;
    output
        .flush()
        .map_err(|_| KeychainAccessError::Unavailable)?;
    let mut release = [0];
    std::io::stdin()
        .read_exact(&mut release)
        .map_err(|_| KeychainAccessError::Unavailable)?;
    if release != [1] {
        return Err(KeychainAccessError::Unavailable);
    }
    Ok(())
}
struct ProcessKeychain(Option<PathBuf>);

impl KeychainAccess for ProcessKeychain {
    fn read_secret(&self, _: &str, account: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        let Some(directory) = &self.0 else {
            return Ok(Some(FIXED_KEY.to_vec()));
        };
        match std::fs::read(directory.join(account)) {
            Ok(secret) => Ok(Some(secret)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(KeychainAccessError::Unavailable),
        }
    }

    fn add_secret(&self, _: &str, account: &str, secret: &[u8]) -> Result<(), KeychainAccessError> {
        let Some(directory) = &self.0 else {
            return Err(KeychainAccessError::Unavailable);
        };
        let staged = directory.join(format!(".item-{}", std::process::id()));
        std::fs::write(&staged, secret).map_err(|_| KeychainAccessError::Unavailable)?;
        let added = std::fs::hard_link(&staged, directory.join(account));
        let _ = std::fs::remove_file(staged);
        added.map_err(|_| KeychainAccessError::Unavailable)
    }
}

#[tokio::test]
#[ignore = "started by the parent process proof with an isolated test database"]
async fn enrollment_process_actor() {
    let database = std::env::var_os(DB_ENV).expect("fixture database");
    let secret_root = std::env::var_os(ROOT_ENV).expect("fixture key identity");
    let machine: MachineId = std::env::var(MACHINE_ENV).unwrap().parse().unwrap();
    let writer = std::env::var(WHICH_ENV).expect("writer label");
    let shared_items = std::env::var_os(ITEMS_ENV).map(PathBuf::from);
    let expected = u64::from(shared_items.is_none());
    let clock = Arc::new(ManualClock::new(Default::default()));
    let store = Fixture::worker(
        std::path::Path::new(&database),
        secret_root.into(),
        machine.clone(),
        clock,
        Arc::new(ProcessKeychain(shared_items)),
        Arc::new(TestWriteFaults::default()),
    )
    .await;
    wait_for_release().expect("start both processes before the replacement lock");
    let pending = writer_binding(machine, &writer, expected + 1);
    let result = store.replace(generation(expected), &pending).await;
    if expected == 0 {
        let Ok(StoredEnrollment::Active(active)) = store.read().await else {
            panic!("first enrollment must be readable by either process");
        };
        assert_eq!(active.generation, generation(1));
    }
    match result {
        Ok(ConditionalWrite::Applied) => println!("ENROLLMENT_PROCESS_APPLIED"),
        Ok(ConditionalWrite::Superseded) => println!("ENROLLMENT_PROCESS_SUPERSEDED"),
        Err(error) => panic!("child write failed: {error}"),
    }
}

fn writer_binding(machine: MachineId, writer: &str, version: u64) -> EnrollmentBinding {
    let enrollment = if writer == "alpha" {
        ENROLLMENT
    } else {
        ALTERNATE_ENROLLMENT
    };
    EnrollmentBinding {
        generation: generation(version),
        context: EnrollmentContext {
            machine_id: machine,
            organization_id: ORGANIZATION.parse().unwrap(),
            deployment: SignInDeployment::new("https://signin.example/".parse().unwrap()).unwrap(),
        },
        credential: RefreshCredential {
            enrollment_id: enrollment.parse().unwrap(),
            refresh_token: token_value(&format!("fictional-{writer}-credential")),
        },
    }
}

struct ChildWriter {
    child: Child,
    output: Lines<BufReader<ChildStdout>>,
}

impl ChildWriter {
    fn spawn(fixture: &Fixture, writer: &str) -> Self {
        Self::start(fixture, writer, None)
    }

    fn start(fixture: &Fixture, writer: &str, items: Option<&PathBuf>) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
            .env(DB_ENV, &fixture.database)
            .env(ROOT_ENV, &fixture.secret_root)
            .env(MACHINE_ENV, fixture.machine.as_str())
            .env(WHICH_ENV, writer)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        if let Some(items) = items {
            command.env(ITEMS_ENV, items);
        }
        let mut child = command.spawn().expect("second signed process");
        let output = BufReader::new(child.stdout.take().expect("child receipt channel")).lines();
        Self { child, output }
    }

    async fn wait_until_ready(&mut self) {
        while let Some(line) = self.output.next_line().await.unwrap() {
            if line.trim() == BARRIER {
                return;
            }
        }
        panic!("child exited before the replacement barrier");
    }

    async fn release(&mut self) {
        self.child
            .stdin
            .take()
            .unwrap()
            .write_all(&[1])
            .await
            .unwrap();
    }

    async fn applied(mut self) -> bool {
        let mut outcome = None;
        while let Some(line) = self.output.next_line().await.unwrap() {
            match line.trim() {
                "ENROLLMENT_PROCESS_APPLIED" => outcome = Some(true),
                "ENROLLMENT_PROCESS_SUPERSEDED" => outcome = Some(false),
                _ => {}
            }
        }
        assert!(self.child.wait().await.unwrap().success());
        outcome.expect("child supplied a result")
    }
}

#[tokio::test]
async fn two_processes_submit_at_one_generation_and_the_row_decrypts_to_the_winner() {
    let fixture = Fixture::with_keychain(Arc::new(FixtureKeychain::fixed(FIXED_KEY))).await;
    fixture.install().await;
    let mut alpha = ChildWriter::spawn(&fixture, "alpha");
    let mut beta = ChildWriter::spawn(&fixture, "beta");
    tokio::join!(alpha.wait_until_ready(), beta.wait_until_ready());

    tokio::join!(alpha.release(), beta.release());
    let (alpha_wrote, beta_wrote) = tokio::join!(alpha.applied(), beta.applied());

    assert_ne!(alpha_wrote, beta_wrote);
    let winner = if alpha_wrote { "alpha" } else { "beta" };
    let expected = writer_binding(fixture.machine, winner, 2);
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(expected))
    );
}

#[tokio::test]
async fn two_first_enrollments_share_one_marker_and_one_readable_keychain_item() {
    let fixture = Fixture::open().await;
    let items = fixture.database.with_file_name("fixture-keychain");
    std::fs::create_dir(&items).unwrap();
    let mut alpha = ChildWriter::start(&fixture, "alpha", Some(&items));
    let mut beta = ChildWriter::start(&fixture, "beta", Some(&items));
    tokio::join!(alpha.wait_until_ready(), beta.wait_until_ready());

    tokio::join!(alpha.release(), beta.release());
    let (alpha_wrote, beta_wrote) = tokio::join!(alpha.applied(), beta.applied());

    assert_ne!(alpha_wrote, beta_wrote);
    let identity = std::fs::read_to_string(fixture.secret_root.join("store-id")).unwrap();
    let item_names: Vec<_> = std::fs::read_dir(&items)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        item_names,
        [std::ffi::OsString::from(format!(
            "pooled-credential-key:{identity}"
        ))]
    );
    let observer = Fixture::worker(
        &fixture.database,
        fixture.secret_root.clone(),
        fixture.machine.clone(),
        fixture.clock.clone(),
        Arc::new(ProcessKeychain(Some(items))),
        Arc::new(TestWriteFaults::default()),
    )
    .await;
    let winner = if alpha_wrote { "alpha" } else { "beta" };
    assert_eq!(
        observer.read().await,
        Ok(StoredEnrollment::Active(writer_binding(
            fixture.machine,
            winner,
            1
        )))
    );
}
