use std::{
    io::{Read, Write},
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
struct ProcessKeychain;

impl KeychainAccess for ProcessKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        Ok(Some(FIXED_KEY.to_vec()))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}

#[tokio::test]
#[ignore = "started by the parent process proof with an isolated test database"]
async fn enrollment_process_actor() {
    let database = std::env::var_os(DB_ENV).expect("fixture database");
    let secret_root = std::env::var_os(ROOT_ENV).expect("fixture key identity");
    let machine: MachineId = std::env::var(MACHINE_ENV).unwrap().parse().unwrap();
    let writer = std::env::var(WHICH_ENV).expect("writer label");
    let clock = Arc::new(ManualClock::new(Default::default()));
    let store = Fixture::worker(
        std::path::Path::new(&database),
        secret_root.into(),
        machine.clone(),
        clock,
        Arc::new(ProcessKeychain),
        Arc::new(TestWriteFaults::default()),
    )
    .await;
    wait_for_release().expect("start both processes before the replacement lock");
    let pending = writer_binding(machine, &writer);
    let result = store.replace(generation(1), &pending).await;
    match result {
        Ok(ConditionalWrite::Applied) => println!("ENROLLMENT_PROCESS_APPLIED"),
        Ok(ConditionalWrite::Superseded) => println!("ENROLLMENT_PROCESS_SUPERSEDED"),
        Err(error) => panic!("child write failed: {error}"),
    }
}

fn writer_binding(machine: MachineId, writer: &str) -> EnrollmentBinding {
    let enrollment = if writer == "alpha" {
        ENROLLMENT
    } else {
        ALTERNATE_ENROLLMENT
    };
    EnrollmentBinding {
        generation: generation(2),
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
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
            .env(DB_ENV, &fixture.database)
            .env(ROOT_ENV, &fixture.secret_root)
            .env(MACHINE_ENV, fixture.machine.as_str())
            .env(WHICH_ENV, writer)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("second signed process");
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
    let expected = writer_binding(fixture.machine, winner);
    assert_eq!(
        fixture.store.read().await,
        Ok(StoredEnrollment::Active(expected))
    );
}
