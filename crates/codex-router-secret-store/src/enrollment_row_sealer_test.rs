use std::{fs, path::Path, sync::Mutex};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::{
    encrypted_credential_store::{EncryptedCredentialStore, decrypt_envelope, encrypt_envelope},
    keychain_data_key::{KeychainAccessError, ROUTER_KEYCHAIN_SERVICE},
    model::SecretKey,
};

const MACHINE_ID: &str = "019a0000-0000-7000-8000-000000000011";
const ORGANIZATION_ID: &str = "019a0000-0000-7000-8000-000000000022";
const ENROLLMENT_ID: &str = "019a0000-0000-7000-8000-000000000033";
const OTHER_ID: &str = "019a0000-0000-7000-8000-000000000099";
const FIRST_FORMAT: u8 = 1;
const TEST_KEY: [u8; 32] = [0x54; 32];

#[derive(Default)]
struct KeychainState {
    item: Option<(String, Zeroizing<Vec<u8>>)>,
    add_attempts: usize,
    install_on_add: bool,
    temporarily_unavailable: bool,
}

#[derive(Default)]
struct MemoryKeychain(Mutex<KeychainState>);

impl KeychainAccess for MemoryKeychain {
    fn read_secret(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        if service != ROUTER_KEYCHAIN_SERVICE {
            return Err(KeychainAccessError::ServiceRejected);
        }
        let state = self
            .0
            .lock()
            .map_err(|_| KeychainAccessError::Unavailable)?;
        if state.temporarily_unavailable {
            return Err(KeychainAccessError::Unavailable);
        }
        Ok(state
            .item
            .as_ref()
            .filter(|(stored_account, _)| stored_account == account)
            .map(|(_, secret)| secret.to_vec()))
    }

    fn add_secret(
        &self,
        service: &str,
        account: &str,
        secret: &[u8],
    ) -> Result<(), KeychainAccessError> {
        if service != ROUTER_KEYCHAIN_SERVICE {
            return Err(KeychainAccessError::ServiceRejected);
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| KeychainAccessError::Unavailable)?;
        state.add_attempts += 1;
        if state.install_on_add {
            state.item = Some((account.to_owned(), Zeroizing::new(TEST_KEY.to_vec())));
            return Err(KeychainAccessError::Unavailable);
        }
        if state.item.is_some() {
            return Err(KeychainAccessError::Unavailable);
        }
        state.item = Some((account.to_owned(), Zeroizing::new(secret.to_vec())));
        Ok(())
    }
}

fn context() -> EnrollmentRowContext {
    EnrollmentRowContext::new(
        MACHINE_ID,
        ORGANIZATION_ID,
        ENROLLMENT_ID,
        "https://signin.example/",
        0x0102_0304_0506_0708,
    )
    .expect("canonical fixture metadata")
}

fn fixture_token() -> EnrollmentRowCredential {
    EnrollmentRowCredential::copy_from("refresh-fixture-æ").expect("nonempty fixture")
}

fn fixture_sealer() -> EnrollmentRowSealer {
    EnrollmentRowSealer(PooledCredentialDataKey::from_bytes(TEST_KEY))
}

fn enroll(path: &Path, keychain: &MemoryKeychain) -> Vec<u8> {
    EnrollmentRowSealer::acquire_with_keychain(path, false, keychain)
        .expect("key for first installation")
        .seal(&context(), &fixture_token())
        .expect("seal")
}

#[test]
fn enrollment_associated_data_matches_the_exact_byte_fixture() {
    let expected = b"switchboard.enrollment-row\x00\x01\
        \x00\x00\x00\x24019a0000-0000-7000-8000-000000000011\
        \x00\x00\x00\x24019a0000-0000-7000-8000-000000000022\
        \x00\x00\x00\x24019a0000-0000-7000-8000-000000000033\
        \x00\x00\x00\x17https://signin.example/\
        \x01\x02\x03\x04\x05\x06\x07\x08";

    assert_eq!(associated_data(&context()).expect("encode"), expected);
}

#[test]
fn enrollment_row_authenticates_each_context_field() {
    let sealer = fixture_sealer();
    let sealed = sealer.seal(&context(), &fixture_token()).expect("seal");
    let variants = [
        EnrollmentRowContext {
            machine_id: OTHER_ID.parse().expect("other machine"),
            ..context()
        },
        EnrollmentRowContext {
            organization_id: OTHER_ID.parse().expect("other organization"),
            ..context()
        },
        EnrollmentRowContext {
            enrollment_id: OTHER_ID.parse().expect("other enrollment"),
            ..context()
        },
        EnrollmentRowContext {
            deployment_origin: "https://other.example/".to_owned(),
            ..context()
        },
        EnrollmentRowContext {
            generation: context().generation + 1,
            ..context()
        },
    ];

    assert!(
        sealer
            .open(&context(), &sealed)
            .expect("open")
            .expose_bytes()
            == fixture_token().expose_bytes()
    );
    for variant in variants {
        assert!(matches!(
            sealer.open(&variant, &sealed),
            Err(SecretStoreError::CredentialAuthenticationFailed)
        ));
    }
}

#[test]
fn enrollment_row_stores_version_nonce_and_ciphertext_and_rejects_other_shapes() {
    let sealer = fixture_sealer();
    let expected_token = fixture_token();
    let sealed = sealer.seal(&context(), &expected_token).expect("seal");
    let second = sealer
        .seal(&context(), &expected_token)
        .expect("seal again");
    assert_eq!(sealed[0], FIRST_FORMAT);
    assert_eq!(
        sealed.len(),
        1 + 12 + 16 + expected_token.expose_bytes().len()
    );
    assert_ne!(sealed[1..13], second[1..13]);
    let mut wrong_version = sealed.clone();
    wrong_version[0] = FIRST_FORMAT + 1;
    for corrupted in [&wrong_version[..], &sealed[..29]] {
        assert!(matches!(
            sealer.open(&context(), corrupted),
            Err(SecretStoreError::InvalidCredentialEnvelope)
        ));
    }
}

#[test]
fn file_and_enrollment_formats_cannot_authenticate_each_others_ciphertext() {
    let sealer = fixture_sealer();
    let file_key = SecretKey::new("fixture-file-key").expect("file key");
    let active_context = context();
    let file_envelope = encrypt_envelope(&sealer.0, &file_key, fixture_token().expose_bytes())
        .expect("file envelope");
    let encoded: serde_json::Value = serde_json::from_slice(&file_envelope).expect("file JSON");
    let mut file_payload_in_row_format = vec![FIRST_FORMAT];
    for field in ["nonce", "ciphertext"] {
        file_payload_in_row_format.extend(
            STANDARD
                .decode(encoded[field].as_str().expect("base64 field"))
                .expect("decode"),
        );
    }
    assert!(matches!(
        sealer.open(&active_context, &file_payload_in_row_format),
        Err(SecretStoreError::CredentialAuthenticationFailed)
    ));

    let row_blob = sealer
        .seal(&active_context, &fixture_token())
        .expect("row blob");
    let row_payload_in_file_format = json!({
        "format": 2,
        "nonce": STANDARD.encode(&row_blob[1..13]),
        "ciphertext": STANDARD.encode(&row_blob[13..]),
    });
    assert!(matches!(
        decrypt_envelope(
            &sealer.0,
            &file_key,
            &serde_json::to_vec(&row_payload_in_file_format).expect("encode file JSON")
        ),
        Err(SecretStoreError::CredentialAuthenticationFailed)
    ));
}

#[test]
fn enrollment_row_with_sealed_bytes_fails_closed_when_its_key_disappears() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    let sealed = enroll(root.path(), &keychain);
    let store_id_path = root.path().join("store-id");
    let original_store_id = fs::read(&store_id_path).expect("store id");
    keychain.0.lock().expect("test keychain").item = None;

    assert!(matches!(
        EnrollmentRowSealer::acquire_with_keychain(root.path(), !sealed.is_empty(), &keychain),
        Err(SecretStoreError::KeyMissing)
    ));
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 1);
    assert_eq!(
        fs::read(&store_id_path).expect("same identity"),
        original_store_id
    );
}

#[test]
fn enrollment_row_with_sealed_bytes_does_not_recreate_a_missing_store_identity() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    let sealed = enroll(root.path(), &keychain);
    let store_id_path = root.path().join("store-id");
    fs::remove_file(&store_id_path).expect("simulate missing store identity");

    assert!(matches!(
        EnrollmentRowSealer::acquire_with_keychain(root.path(), !sealed.is_empty(), &keychain),
        Err(SecretStoreError::KeyMissing)
    ));
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 1);
    assert!(!store_id_path.exists());
}

#[test]
fn first_enrollment_creates_the_shared_key_once_and_reopens_its_token() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    let sealed = enroll(root.path(), &keychain);

    let reopened = EnrollmentRowSealer::acquire_with_keychain(root.path(), true, &keychain)
        .expect("reopen without creating a key");
    assert!(
        reopened
            .open(&context(), &sealed)
            .expect("open token")
            .expose_bytes()
            == fixture_token().expose_bytes()
    );
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 1);
}

#[test]
fn enrollment_key_provisioning_reads_back_the_duplicate_add_winner() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain(Mutex::new(KeychainState {
        install_on_add: true,
        ..KeychainState::default()
    }));
    let sealed = enroll(root.path(), &keychain);

    let decrypted = fixture_sealer()
        .open(&context(), &sealed)
        .expect("winner's key");
    assert!(decrypted.expose_bytes() == fixture_token().expose_bytes());
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 1);
}

#[test]
fn enrollment_does_not_create_a_key_when_existing_file_envelopes_need_it() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    fs::write(root.path().join("opaque-envelope.v2"), b"opaque test bytes").expect("fixture");

    assert!(matches!(
        EnrollmentRowSealer::acquire_with_keychain(root.path(), false, &keychain),
        Err(SecretStoreError::KeyMissing)
    ));
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 0);
    assert!(!root.path().join("store-id").exists());
}

#[test]
fn enrollment_retries_key_acquisition_after_the_keychain_unlocks() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    let sealed = enroll(root.path(), &keychain);
    keychain
        .0
        .lock()
        .expect("test keychain")
        .temporarily_unavailable = true;

    assert!(matches!(
        EnrollmentRowSealer::acquire_with_keychain(root.path(), true, &keychain),
        Err(SecretStoreError::KeyUnavailable)
    ));
    keychain
        .0
        .lock()
        .expect("test keychain")
        .temporarily_unavailable = false;
    let available = EnrollmentRowSealer::acquire_with_keychain(root.path(), true, &keychain)
        .expect("retry after unlock");
    assert!(
        available
            .open(&context(), &sealed)
            .expect("open")
            .expose_bytes()
            == fixture_token().expose_bytes()
    );
    assert_eq!(keychain.0.lock().expect("test keychain").add_attempts, 1);
}

#[test]
#[ignore = "Host wiring must gate the legacy file opener with the enrollment row witness"]
fn host_wiring_must_prevent_the_file_opener_from_rekeying_an_active_enrollment() {
    let root = TempDir::new().expect("temporary root");
    let keychain = MemoryKeychain::default();
    let sealed = enroll(root.path(), &keychain);
    assert!(!sealed.is_empty());
    keychain.0.lock().expect("test keychain").item = None;

    let _ = EncryptedCredentialStore::open_for_process_with_keychain(root.path(), &keychain);

    assert_eq!(
        keychain.0.lock().expect("test keychain").add_attempts,
        1,
        "the old opener attempts a second add without the Host guard"
    );
}

#[test]
fn fixed_enrollment_ciphertext_matches_the_version_one_byte_fixture() {
    const NONCE: [u8; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    const V1_BLOB: &[u8] = b"\x01\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\
        \xe9\x7d\x08\x27\xe3\x21\x0d\x61\x54\x1e\x21\x23\xd3\xa5\x3a\x91\x2a\x80\
        \x87\xae\xc1\x74\x20\x1e\x2e\xbd\x16\x7b\x33\x98\x21\x1c\x24\xd9";
    let context = context();
    let sealer = fixture_sealer();
    let credential = fixture_token();
    let ciphertext = credential_cipher::encrypt_for_fixture(
        &sealer.0,
        &associated_data(&context).expect("fixture AAD"),
        NONCE,
        credential.expose_bytes(),
    )
    .expect("fixture ciphertext");

    assert!(encode_sealed(&NONCE, &ciphertext) == V1_BLOB);
    assert!(
        sealer
            .open(&context, V1_BLOB)
            .expect("open v1 fixture")
            .expose_bytes()
            == credential.expose_bytes()
    );
}

#[test]
fn enrollment_context_refuses_an_empty_field_and_noncanonical_ids() {
    let invalid_contexts = [
        (
            "",
            ORGANIZATION_ID,
            ENROLLMENT_ID,
            "https://signin.example/",
        ),
        (MACHINE_ID, "", ENROLLMENT_ID, "https://signin.example/"),
        (MACHINE_ID, ORGANIZATION_ID, "", "https://signin.example/"),
        (MACHINE_ID, ORGANIZATION_ID, ENROLLMENT_ID, ""),
        (
            "019A0000-0000-7000-8000-000000000011",
            ORGANIZATION_ID,
            ENROLLMENT_ID,
            "https://signin.example/",
        ),
    ];
    for (machine, org, enrollment, deployment) in invalid_contexts {
        assert!(matches!(
            EnrollmentRowContext::new(machine, org, enrollment, deployment, 1),
            Err(SecretStoreError::InvalidEnrollmentRowContext)
        ));
    }
}

#[test]
fn enrollment_credential_rejects_empty_and_redacts_debug() {
    assert!(EnrollmentRowCredential::copy_from("").is_none());
    assert_eq!(format!("{:?}", fixture_token()), "[redacted]");
}

#[path = "enrollment_identity_process_test.rs"]
mod identity_process;
