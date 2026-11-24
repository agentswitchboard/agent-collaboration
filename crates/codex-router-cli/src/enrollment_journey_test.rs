use std::{
    future::ready,
    io::{self, Write},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use router_clock::{ClockInstant, ManualClock};
use serde_json::{Value, json};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentEnd, EnrollmentKey, EnrollmentMetadata, KeyedEnrollment, MachineId, OrganizationId,
    SignInClient, SignInDeployment, StoreDurability, StoredEnrollment,
};
use switchboard_machine_enrollment_store::EnrollmentStoreCloseError;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

use super::{EnrollBindings, EnrollCommandError, run_enroll};

const MACHINE: &str = "019a0000-0000-7000-8000-000000000003";
const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000004";
const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000005";
const LABEL: &str = "Skybook";
const USER_CODE: &str = "SHOP-LION";

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScriptedSave {
    Applied,
    Rejected,
    LostAcknowledgement,
}

struct ScriptedEnrollmentStore {
    saved: Mutex<StoredEnrollment>,
    save: ScriptedSave,
    write_started: AtomicBool,
    reconciliation_read: AtomicBool,
}

impl ScriptedEnrollmentStore {
    fn new(saved: StoredEnrollment, save: ScriptedSave) -> Self {
        Self {
            saved: Mutex::new(saved),
            save,
            write_started: AtomicBool::new(false),
            reconciliation_read: AtomicBool::new(false),
        }
    }

    fn snapshot(&self) -> StoredEnrollment {
        self.saved.lock().expect("scripted enrollment").clone()
    }
}

impl CredentialStore for ScriptedEnrollmentStore {
    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        if self.save == ScriptedSave::LostAcknowledgement
            && self.write_started.load(Ordering::SeqCst)
        {
            self.reconciliation_read.store(true, Ordering::SeqCst);
            return Err(CredentialStoreError::Read);
        }
        Ok(self.snapshot())
    }

    async fn read_for_key(
        &self,
        key: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        Ok(KeyedEnrollment::from_snapshot(self.snapshot(), key))
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        Ok(self.snapshot().into())
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        assert!(
            !self.write_started.swap(true, Ordering::SeqCst),
            "only one approved save is expected"
        );
        let mut saved = self.saved.lock().expect("scripted enrollment");
        assert_eq!(saved.generation(), expected);
        if self.save == ScriptedSave::Rejected {
            return Err(CredentialStoreError::Replace);
        }
        *saved = StoredEnrollment::Active(binding.clone());
        if self.save == ScriptedSave::LostAcknowledgement {
            return Err(CredentialStoreError::OutcomeNotKnown);
        }
        Ok(ConditionalWrite::Applied)
    }

    async fn remove(
        &self,
        _: BindingGeneration,
        _: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        panic!("enroll does not retire a binding")
    }

    async fn mark_unusable(
        &self,
        _: BindingGeneration,
        _: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        panic!("enroll does not mark a binding unusable")
    }
}

struct EnrollmentOutput<'store> {
    store: &'store ScriptedEnrollmentStore,
    captured: Vec<u8>,
    success_before_install: bool,
    instructions_after_install: bool,
}

impl Write for EnrollmentOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.captured.extend_from_slice(bytes);
        if self.captured.ends_with(b"This Mac is enrolled.\n")
            && !matches!(self.store.snapshot(), StoredEnrollment::Active(_))
        {
            self.success_before_install = true;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.instructions_after_install |=
            !matches!(self.store.snapshot(), StoredEnrollment::Empty);
        Ok(())
    }
}

#[tokio::test]
async fn approved_enrollment_displays_instructions_then_installs_and_reports_success() {
    let (terminal, result) = run_approved_enrollment(ScriptedSave::Applied, Ok(())).await;
    result.expect("enrollment installed");
    assert!(terminal.ends_with("Code SHOP-LION\nThis Mac is enrolled.\n"));
}

#[tokio::test]
async fn enrollment_close_failure_points_to_status_after_install() {
    let (terminal, result) = run_approved_enrollment(
        ScriptedSave::Applied,
        Err(EnrollmentStoreCloseError::WorkerStopped),
    )
    .await;
    assert!(terminal.ends_with("Code SHOP-LION\n"));
    assert_eq!(
        result.expect_err("the store could not close").to_string(),
        "Enrollment was saved. The enrollment store could not close. Run enrollment status before trying again."
    );
}

#[tokio::test]
async fn approved_enrollment_reports_a_definite_local_save_failure() {
    let (terminal, result) = run_approved_enrollment(ScriptedSave::Rejected, Ok(())).await;
    assert!(terminal.ends_with("Code SHOP-LION\n"));
    assert!(!terminal.contains("This Mac is enrolled."));
    assert_eq!(
        result.expect_err("local save was refused").to_string(),
        "Sign-in approved this request. The save on this Mac failed. Check codex-router enrollment status before starting again."
    );
}

#[tokio::test]
async fn approved_enrollment_reports_an_unknown_local_save_outcome() {
    let (terminal, result) =
        run_approved_enrollment(ScriptedSave::LostAcknowledgement, Ok(())).await;
    assert!(terminal.ends_with("Code SHOP-LION\n"));
    assert!(!terminal.contains("This Mac is enrolled."));
    assert_eq!(
        result
            .expect_err("local save acknowledgement and reconciliation were lost")
            .to_string(),
        "Sign-in approved this request. It is not known whether this Mac saved it. Check codex-router enrollment status and the enrollment at central before asking for another approval. Local status may show an older enrollment for the same organization."
    );
}

async fn run_approved_enrollment(
    save: ScriptedSave,
    closing: Result<(), EnrollmentStoreCloseError>,
) -> (String, Result<(), EnrollCommandError>) {
    let clock = fixture_clock();
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("loopback sign-in");
    let origin = format!(
        "http://{}/",
        listener.local_addr().expect("loopback address")
    );
    let client = SignInClient::new_loopback_for_test(
        origin.parse().expect("loopback origin"),
        clock.clone(),
        |_| {},
    )
    .expect("fixture client");
    let script = tokio::spawn(serve_approval(listener, origin.clone()));
    let machine_id: MachineId = MACHINE.parse().expect("fixture machine ID");
    let store = ScriptedEnrollmentStore::new(StoredEnrollment::Empty, save);
    let mut output = EnrollmentOutput {
        store: &store,
        captured: Vec::new(),
        success_before_install: false,
        instructions_after_install: false,
    };
    let result = {
        let mut enrollment = pin!(run_enroll(
            organization_id(),
            EnrollBindings {
                machine_id: &machine_id,
                machine_label: LABEL,
                store: &store
            },
            &client,
            ready(closing),
            &mut output,
        ));
        let interval = Duration::from_secs(5);
        tokio::select! {
            result = &mut enrollment => panic!("enrollment ended before the device poll: {result:?}"),
            () = clock.not_due_check_now(ClockInstant::ZERO.after(interval)) => {}
        }
        clock
            .advance(interval)
            .expect("advance the device polling clock");
        enrollment.await
    };
    script.await.expect("loopback sign-in completed");
    assert!(
        !output.success_before_install,
        "reported success before install"
    );
    assert!(
        !output.instructions_after_install,
        "displayed device instructions after install"
    );
    assert!(store.write_started.load(Ordering::SeqCst));
    assert_eq!(
        store.reconciliation_read.load(Ordering::SeqCst),
        save == ScriptedSave::LostAcknowledgement
    );
    if save == ScriptedSave::Rejected {
        assert!(matches!(store.snapshot(), StoredEnrollment::Empty));
    } else {
        let StoredEnrollment::Active(binding) = store.snapshot() else {
            panic!("the approved binding was not installed");
        };
        assert_eq!(binding.context.machine_id, machine_id);
        assert_eq!(binding.context.organization_id, organization_id());
        assert_eq!(binding.credential.enrollment_id.to_string(), ENROLLMENT);
    }
    let captured = String::from_utf8(output.captured).expect("UTF-8 terminal");
    assert!(captured.starts_with(&format!("Open {origin}device?user_code={USER_CODE}\n")));
    (captured, result)
}

#[tokio::test]
async fn existing_scope_refusal_reports_remove_first_without_contacting_sign_in() {
    let client = SignInClient::new_loopback_for_test(
        "http://127.0.0.1:1/".parse().expect("loopback origin"),
        fixture_clock(),
        |_| panic!("scope refusal contacted sign-in"),
    )
    .expect("fixture client");
    let machine_id: MachineId = MACHINE.parse().expect("fixture machine ID");
    let store = ScriptedEnrollmentStore::new(
        StoredEnrollment::Ended {
            generation: BindingGeneration::from_persisted(1).expect("fixture generation"),
            key: EnrollmentKey {
                machine_id: machine_id.clone(),
                organization_id: organization_id(),
                enrollment_id: ENROLLMENT.parse().expect("fixture enrollment ID"),
            },
            why: EnrollmentEnd::Revoked,
            deployment: SignInDeployment::new(
                "https://other.example/".parse().expect("fixture origin"),
            )
            .expect("fixture deployment"),
        },
        ScriptedSave::Applied,
    );
    let mut output = Vec::new();
    let result = run_enroll(
        organization_id(),
        EnrollBindings {
            machine_id: &machine_id,
            machine_label: LABEL,
            store: &store,
        },
        &client,
        ready(Err(EnrollmentStoreCloseError::WorkerStopped)),
        &mut output,
    )
    .await;
    assert_eq!(
        result.expect_err("scope refusal").to_string(),
        "Remove this machine first, then enroll it in the new organization or sign-in deployment."
    );
    assert!(output.is_empty());
}

fn organization_id() -> OrganizationId {
    ORGANIZATION.parse().expect("fixture organization ID")
}

fn fixture_clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(
        "2026-11-23T00:00:00Z".parse().expect("fixture time"),
    ))
}

async fn serve_approval(listener: TcpListener, origin: String) {
    let discovery = json!({
        "client_id": "019a0000-0000-7000-8000-000000000006",
        "link_url": "https://central.example/v1/organizations/{organization_id}/link"
    });
    reply(
        &listener,
        "GET /.well-known/fleet-login HTTP/1.1",
        discovery,
    )
    .await;
    let device = json!({
        "device_code": "fixture-device-code",
        "user_code": USER_CODE,
        "verification_uri": format!("{origin}device"),
        "verification_uri_complete": format!("{origin}device?user_code={USER_CODE}"),
        "expires_in": 120,
        "interval": 5
    });
    let body = reply(&listener, "POST /api/auth/device/code HTTP/1.1", device).await;
    assert!(body.contains(&format!("machine_id={MACHINE}")));
    assert!(body.contains(&format!("organization_id={ORGANIZATION}")));
    assert!(body.contains(&format!("machine_label={LABEL}")));
    let grant = json!({
        "access_token": "fixture-access",
        "refresh_token": "fixture-refresh",
        "expires_in": 60,
        "expires_at": 60,
        "token_type": "Bearer",
        "scope": "offline_access",
        "machine_client_id": ENROLLMENT
    });
    reply(&listener, "POST /api/auth/oauth2/token HTTP/1.1", grant).await;
}

async fn reply(listener: &TcpListener, request_line: &str, response: Value) -> String {
    let (stream, _) = listener.accept().await.expect("fixture request");
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    stream
        .read_line(&mut line)
        .await
        .expect("HTTP request line");
    assert_eq!(line.trim_end(), request_line);
    let mut content_length = 0;
    loop {
        line.clear();
        assert_ne!(stream.read_line(&mut line).await.expect("HTTP header"), 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, length)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = length.trim().parse().expect("HTTP request length");
        }
    }
    let mut body = vec![0; content_length];
    stream
        .read_exact(&mut body)
        .await
        .expect("HTTP request body");
    let response = response.to_string();
    let message = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
        response.len()
    );
    stream
        .get_mut()
        .write_all(message.as_bytes())
        .await
        .expect("fixture response");
    String::from_utf8(body).expect("UTF-8 form")
}
