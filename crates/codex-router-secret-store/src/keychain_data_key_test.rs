#[cfg(target_os = "macos")]
#[test]
fn keychain_failure_metadata_preserves_returned_os_status() {
    for (os_status, expected_class) in [
        (-61, "no_storage_access"),
        (-25293, "platform_failure"),
        (-25308, "platform_failure"),
        (-128, "platform_failure"),
    ] {
        let error = apple_native_keyring_store::keychain::decode_error(
            security_framework::base::Error::from_code(os_status),
        );
        assert_eq!(
            super::keychain_failure_metadata(&error),
            (expected_class, Some(os_status))
        );
    }
    assert_eq!(
        super::keychain_failure_metadata(&keyring_core::Error::NoEntry),
        ("no_entry", None)
    );
}

#[cfg(target_os = "macos")]
#[test]
fn keychain_failure_metadata_never_formats_backend_payloads() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PrivateBackendFailure(Arc<AtomicBool>);
    impl std::fmt::Display for PrivateBackendFailure {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.store(true, Ordering::SeqCst);
            formatter.write_str("PRIVATE_BACKEND_CANARY")
        }
    }
    impl std::fmt::Debug for PrivateBackendFailure {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(self, formatter)
        }
    }
    impl std::error::Error for PrivateBackendFailure {}
    let formatted = Arc::new(AtomicBool::new(false));
    for (error, expected_class) in [
        (
            keyring_core::Error::PlatformFailure(Box::new(PrivateBackendFailure(Arc::clone(
                &formatted,
            )))),
            "platform_failure",
        ),
        (
            keyring_core::Error::BadDataFormat(
                b"PRIVATE_SECRET_CANARY".to_vec(),
                Box::new(PrivateBackendFailure(Arc::clone(&formatted))),
            ),
            "bad_data_format",
        ),
        (
            keyring_core::Error::Invalid("PRIVATE_ACCOUNT".into(), "PRIVATE_PATH".into()),
            "invalid_attribute",
        ),
    ] {
        assert_eq!(
            super::keychain_failure_metadata(&error),
            (expected_class, None)
        );
        assert!(!formatted.load(Ordering::SeqCst));
    }
}

use std::collections::HashMap;
use std::sync::Mutex;

use super::KeychainAccess;
use super::KeychainAccessError;
use super::PooledCredentialDataKey;
use super::ROUTER_KEYCHAIN_SERVICE;
use super::ensure_router_keychain_service;
use super::with_platform_keychain_for_production;

type AfterKeyMiss = Box<dyn FnOnce(&MemoryKeychainAccess) + Send>;

#[derive(Default)]
struct MemoryKeychainAccess {
    items: Mutex<HashMap<(String, String), Vec<u8>>>,
    read_count: Mutex<usize>,
    add_count: Mutex<usize>,
    after_key_miss: Mutex<Option<AfterKeyMiss>>,
}

impl KeychainAccess for MemoryKeychainAccess {
    #[expect(
        clippy::unwrap_in_result,
        reason = "This test double must fail on broken fixture invariants instead of reporting a product error."
    )]
    fn read_secret(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        ensure_router_keychain_service(service)?;
        *self.read_count.lock().expect("read count lock") += 1;
        let existing = self
            .items
            .lock()
            .expect("items lock")
            .get(&(service.to_owned(), account.to_owned()))
            .cloned();
        if existing.is_none() {
            let publish = self.after_key_miss.lock().expect("key miss pause").take();
            if let Some(publish) = publish {
                publish(self);
            }
        }
        Ok(existing)
    }

    #[expect(
        clippy::unwrap_in_result,
        reason = "This test double must fail on broken fixture invariants instead of reporting a product error."
    )]
    fn add_secret(
        &self,
        service: &str,
        account: &str,
        secret: &[u8],
    ) -> Result<(), KeychainAccessError> {
        ensure_router_keychain_service(service)?;
        *self.add_count.lock().expect("add count lock") += 1;
        let mut items = self.items.lock().expect("items lock");
        if items.contains_key(&(service.to_owned(), account.to_owned())) {
            return Err(KeychainAccessError::Unavailable);
        }
        items.insert((service.to_owned(), account.to_owned()), secret.to_vec());
        Ok(())
    }
}

#[test]
fn production_keychain_service_guard_accepts_only_router_service() {
    assert_eq!(
        ensure_router_keychain_service(ROUTER_KEYCHAIN_SERVICE),
        Ok(())
    );
    assert_eq!(
        ensure_router_keychain_service("another-application"),
        Err(KeychainAccessError::ServiceRejected)
    );
}

#[test]
fn production_store_opener_fails_loudly_in_a_test_build() {
    let result =
        crate::encrypted_credential_store::EncryptedCredentialStore::open_production_for_process(
            std::env::temp_dir().join("codex-router-production-keychain-test-guard"),
        );

    assert!(matches!(
        result,
        Err(crate::model::SecretStoreError::TestKeychainAccessForbidden)
    ));
}

#[test]
fn production_migration_entrypoint_fails_loudly_in_a_test_build() {
    let result = crate::credential_migration::migrate_pooled_credentials_at_production_startup(
        std::env::temp_dir().join("codex-router-production-migration-test-guard"),
    );

    assert!(matches!(
        result,
        Err(crate::model::SecretStoreError::TestKeychainAccessForbidden)
    ));
}

#[test]
fn platform_keychain_access_is_unreachable_from_test_builds() {
    let was_called = std::cell::Cell::new(false);

    let result = with_platform_keychain_for_production(|_keychain| {
        was_called.set(true);
        Ok(())
    });

    assert!(matches!(
        result,
        Err(crate::model::SecretStoreError::TestKeychainAccessForbidden)
    ));
    assert!(
        !was_called.get(),
        "test builds must not construct the platform keychain"
    );
}

#[test]
fn data_key_debug_output_is_redacted() {
    let data_key = PooledCredentialDataKey::from_bytes([0x5a; 32]);

    assert_eq!(
        format!("{data_key:?}"),
        "PooledCredentialDataKey(\"[REDACTED]\")"
    );
}

#[test]
fn key_is_created_once_and_reused_for_the_same_store_root() {
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::file_backend::FileSecretStore;
    use crate::keychain_data_key::load_or_create_pooled_credential_data_key;

    let directory = TempDir::new().expect("temporary root");
    let file_store = FileSecretStore::open(directory.path()).expect("secret store");
    let keychain = Arc::new(MemoryKeychainAccess::default());

    let first_key = load_or_create_pooled_credential_data_key(&file_store, keychain.as_ref())
        .expect("first opener creates key");
    let second_key = load_or_create_pooled_credential_data_key(&file_store, keychain.as_ref())
        .expect("next opener reads key");

    assert_eq!(first_key.bytes(), second_key.bytes());
    assert_eq!(*keychain.read_count.lock().expect("read count lock"), 2);
}

type StoreKeyOpener = fn(
    &crate::file_backend::FileSecretStore,
    &dyn KeychainAccess,
) -> Result<PooledCredentialDataKey, crate::model::SecretStoreError>;

fn open_enrollment_key(
    store: &crate::file_backend::FileSecretStore,
    keychain: &dyn KeychainAccess,
) -> Result<PooledCredentialDataKey, crate::model::SecretStoreError> {
    super::load_enrollment_row_data_key(store, keychain, false)
}

fn assert_store_id_publication_order(wins: StoreKeyOpener, loses: StoreKeyOpener) {
    use crate::file_backend::{FileSecretStore, FileWriteTrace};
    use std::{
        fs,
        sync::{Arc, mpsc},
    };

    let root = tempfile::tempdir().expect("temporary secret root");
    let keychain = Arc::new(MemoryKeychainAccess::default());
    let trace = FileWriteTrace::default();
    let winner_store = FileSecretStore::open(root.path()).expect("winning store");
    let winner_keychain = Arc::clone(&keychain);
    let (published, publication) = mpsc::channel();
    trace.before_store_id_publication(move || {
        assert_eq!(
            winner_store
                .read_store_id_file()
                .expect("read marker before the winner"),
            None
        );
        let key = wins(&winner_store, winner_keychain.as_ref()).expect("winner opens");
        let marker = winner_store
            .read_store_id_file()
            .expect("read winner marker")
            .expect("winner published");
        published
            .send((marker, key))
            .expect("winner publication witness");
    });
    let loser_store =
        FileSecretStore::open_with_write_trace(root.path(), trace).expect("losing store");

    // Reenter through the other real opener after the loser's absent read
    // and staged write, but before the loser publishes.
    let adopted = loses(&loser_store, keychain.as_ref()).expect("loser adopts the winning item");

    let (marker, winning_key) = publication.try_recv().expect("forced winning publication");
    assert_eq!(
        loser_store.read_store_id_file().expect("read final marker"),
        Some(marker)
    );
    assert!(
        adopted.bytes() == winning_key.bytes(),
        "both paths must open the same data key"
    );
    assert_eq!(keychain.items.lock().expect("memory keychain").len(), 1);
    assert_eq!(
        fs::read_dir(root.path()).expect("published files").count(),
        1
    );
}

#[test]
fn legacy_store_opener_adopts_enrollment_publication_after_its_absent_read() {
    assert_store_id_publication_order(
        open_enrollment_key,
        super::load_or_create_pooled_credential_data_key,
    );
}

#[test]
fn legacy_store_opener_winner_is_adopted_by_enrollment_after_its_absent_read() {
    assert_store_id_publication_order(
        super::load_or_create_pooled_credential_data_key,
        open_enrollment_key,
    );
}

#[test]
fn legacy_store_opener_uses_the_key_for_an_existing_marker() {
    use crate::file_backend::{FileSecretStore, FileWriteTrace};

    let root = tempfile::tempdir().expect("temporary secret root");
    let marker = "019a0000-0000-7000-8000-000000000033";
    let trace = FileWriteTrace::default();
    let store =
        FileSecretStore::open_with_write_trace(root.path(), trace.clone()).expect("file store");
    std::fs::write(root.path().join("store-id"), marker).expect("existing marker");
    let keychain = MemoryKeychainAccess::default();
    let winner_key = [0x57; 32];
    keychain
        .add_secret(
            ROUTER_KEYCHAIN_SERVICE,
            &format!("{}{marker}", super::POOLED_KEY_ACCOUNT_PREFIX),
            &winner_key,
        )
        .expect("existing item");

    let key = super::load_or_create_pooled_credential_data_key(&store, &keychain)
        .expect("open existing item");

    assert!(
        key.bytes() == &winner_key,
        "must use the existing marker's key"
    );
    assert_eq!(
        store
            .read_store_id_file()
            .expect("read unchanged marker")
            .as_deref(),
        Some(marker)
    );
    assert!(
        trace.events().is_empty(),
        "must not write a replacement marker"
    );
    assert_eq!(keychain.items.lock().expect("memory keychain").len(), 1);
}

#[test]
fn legacy_store_opener_refuses_sealed_files_without_a_marker_before_writing() {
    use crate::{
        encrypted_credential_store::encrypt_envelope,
        file_backend::{FileReadTrace, FileSecretStore, FileWriteTrace},
        model::{SecretKey, SecretStoreError},
    };

    let root = tempfile::tempdir().expect("temporary secret root");
    let envelope_store = FileSecretStore::open(root.path()).expect("envelope store");
    let credential = SecretKey::new("openai_credential_bundle.fixture.1").expect("fixture key");
    let envelope = encrypt_envelope(
        &PooledCredentialDataKey::from_bytes([0x57; 32]),
        &credential,
        b"synthetic fixture",
    )
    .expect("sealed envelope");
    envelope_store
        .write_credential_envelope(&credential, &envelope)
        .expect("write fixture envelope");
    let trace = FileWriteTrace::default();
    let reads = FileReadTrace::default();
    let store =
        FileSecretStore::open_with_read_and_write_traces(root.path(), reads.clone(), trace.clone())
            .expect("file store");
    let keychain = MemoryKeychainAccess::default();

    let result = super::load_or_create_pooled_credential_data_key(&store, &keychain);

    assert!(matches!(result, Err(SecretStoreError::KeyMissing)));
    assert_eq!(
        marker_read_count(&reads, root.path()),
        2,
        "persistent marker absence is rechecked once"
    );
    assert_eq!(store.read_store_id_file().expect("no marker write"), None);
    assert_eq!(
        store
            .read_credential_envelope(&credential)
            .expect("unchanged envelope"),
        envelope
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .expect("fixture files")
            .count(),
        1
    );
    assert!(
        trace.events().is_empty(),
        "no marker staging or other write"
    );
    assert_eq!(*keychain.read_count.lock().expect("memory keychain"), 0);
    assert!(keychain.items.lock().expect("memory keychain").is_empty());
}

const SEEDED_MARKER: &str = "019a0000-0000-7000-8000-000000000033";
const SEALED_FIXTURE: &str = "synthetic sealed fixture";

fn envelope_key() -> crate::model::SecretKey {
    crate::model::SecretKey::new("openai_credential_bundle.fixture.1").expect("fixture key")
}

fn write_winning_envelope(root: &std::path::Path, keychain: &MemoryKeychainAccess) {
    use crate::{SecretStore, encrypted_credential_store::EncryptedCredentialStore};
    use codex_router_core::redaction::SecretString;

    let winner = EncryptedCredentialStore::open_for_process_with_keychain(root, keychain)
        .expect("the pooled winner opens");
    winner
        .write_secret(&envelope_key(), &SecretString::new(SEALED_FIXTURE))
        .expect("the pooled winner seals a real envelope");
}

fn assert_opened_winning_envelope(
    store: &crate::file_backend::FileSecretStore,
    key: &PooledCredentialDataKey,
) {
    let credential = envelope_key();
    let envelope = store
        .read_credential_envelope(&credential)
        .expect("winning ciphertext");
    let plaintext =
        crate::encrypted_credential_store::decrypt_envelope(key, &credential, &envelope)
            .expect("use the existing winner's data key");
    assert!(plaintext.expose_secret() == SEALED_FIXTURE);
}

fn marker_read_count(trace: &crate::file_backend::FileReadTrace, root: &std::path::Path) -> usize {
    use crate::file_backend::FileReadTraceEvent;

    let marker = root.join("store-id");
    trace.events().iter().filter(|event| {
        matches!(event, FileReadTraceEvent::FileOpenAttempted { path } if path == &marker)
    }).count()
}

#[test]
fn store_id_recheck_adopts_a_marker_published_after_the_first_absent_read() {
    use crate::file_backend::{FileReadTrace, FileSecretStore, FileWriteTrace};
    use std::sync::Arc;

    let root = tempfile::tempdir().expect("temporary secret root");
    let reads = FileReadTrace::default();
    let writes = FileWriteTrace::default();
    let keychain = Arc::new(MemoryKeychainAccess::default());
    let winner_keychain = Arc::clone(&keychain);
    let winner_root = root.path().to_path_buf();
    reads.after_store_id_miss(move || write_winning_envelope(&winner_root, &winner_keychain));
    let waiting = FileSecretStore::open_with_read_and_write_traces(
        root.path(),
        reads.clone(),
        writes.clone(),
    )
    .expect("waiting store");

    let key = open_enrollment_key(&waiting, keychain.as_ref())
        .expect("read the marker published after the first absent read");

    assert_opened_winning_envelope(&waiting, &key);
    assert_eq!(marker_read_count(&reads, root.path()), 2);
    assert!(
        writes.events().is_empty(),
        "the waiting opener must not publish"
    );
    assert_eq!(
        *keychain.add_count.lock().expect("add count"),
        1,
        "only the winner adds"
    );
}

#[test]
fn store_id_recheck_adopts_a_key_published_after_the_first_absent_read() {
    use crate::file_backend::{FileReadTrace, FileSecretStore, FileWriteTrace};

    for opener in [
        open_enrollment_key as StoreKeyOpener,
        super::load_or_create_pooled_credential_data_key,
    ] {
        let root = tempfile::tempdir().expect("temporary secret root");
        let reads = FileReadTrace::default();
        let writes = FileWriteTrace::default();
        let waiting = FileSecretStore::open_with_read_and_write_traces(
            root.path(),
            reads.clone(),
            writes.clone(),
        )
        .expect("waiting store");
        std::fs::write(root.path().join("store-id"), SEEDED_MARKER).expect("pre-existing marker");
        let keychain = MemoryKeychainAccess::default();
        let winner_root = root.path().to_path_buf();
        *keychain.after_key_miss.lock().expect("key miss pause") =
            Some(Box::new(move |keychain| {
                write_winning_envelope(&winner_root, keychain)
            }));

        let key = opener(&waiting, &keychain)
            .expect("read the key published after the first absent read");

        assert_opened_winning_envelope(&waiting, &key);
        assert_eq!(
            marker_read_count(&reads, root.path()),
            1,
            "reuse the resolved marker"
        );
        assert_eq!(
            *keychain.read_count.lock().expect("read count"),
            3,
            "two waiting reads and one winning read"
        );
        assert_eq!(
            *keychain.add_count.lock().expect("add count"),
            1,
            "only the winner adds"
        );
        assert!(
            writes.events().is_empty(),
            "the waiting opener must not publish"
        );
    }
}

#[test]
fn store_id_recheck_refuses_a_persistently_missing_key_when_an_envelope_exists() {
    use crate::{
        encrypted_credential_store::encrypt_envelope,
        file_backend::{FileSecretStore, FileWriteTrace},
        model::SecretStoreError,
    };

    for opener in [
        open_enrollment_key as StoreKeyOpener,
        super::load_or_create_pooled_credential_data_key,
    ] {
        let root = tempfile::tempdir().expect("temporary secret root");
        let trace = FileWriteTrace::default();
        let store = FileSecretStore::open_with_write_trace(root.path(), trace.clone())
            .expect("waiting store");
        let fixture = FileSecretStore::open(root.path()).expect("fixture store");
        fixture
            .publish_store_id_if_absent(SEEDED_MARKER)
            .expect("pre-existing marker");
        let envelope = encrypt_envelope(
            &PooledCredentialDataKey::from_bytes([0x57; 32]),
            &envelope_key(),
            SEALED_FIXTURE.as_bytes(),
        )
        .expect("fixture ciphertext");
        fixture
            .write_credential_envelope(&envelope_key(), &envelope)
            .expect("fixture envelope");
        let keychain = MemoryKeychainAccess::default();

        let result = opener(&store, &keychain);

        assert!(matches!(result, Err(SecretStoreError::KeyMissing)));
        assert_eq!(
            *keychain.read_count.lock().expect("read count"),
            2,
            "persistent key absence is rechecked once"
        );
        assert_eq!(*keychain.add_count.lock().expect("add count"), 0);
        assert_eq!(
            store
                .read_store_id_file()
                .expect("unchanged marker")
                .as_deref(),
            Some(SEEDED_MARKER)
        );
        assert_eq!(
            store
                .read_credential_envelope(&envelope_key())
                .expect("unchanged envelope"),
            envelope
        );
        assert!(
            trace.events().is_empty(),
            "failed acquisition does not write"
        );
    }
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires the opt-in unsandboxed temporary-Keychain test command"]
fn keychain_access_uses_an_explicit_temporary_keychain_file() {
    use security_framework::os::macos::keychain::SecKeychain;

    use super::temporary_keychain_tests::TemporaryKeychainAccess;
    use super::temporary_keychain_tests::acquire_temporary_keychain_test_lock;

    assert!(matches!(
        std::env::var("CODEX_ROUTER_KEYCHAIN_TESTS").as_deref(),
        Ok("1")
    ));
    let _keychain_test_lock = acquire_temporary_keychain_test_lock();
    let keychain = TemporaryKeychainAccess::new().expect("temporary Keychain");
    let account = "pooled-credential-key:temporary-keychain-test";

    assert!(keychain.path().is_absolute());
    assert!(!SecKeychain::user_interaction_allowed().expect("query test interaction state"));
    assert_eq!(
        keychain
            .read_secret(ROUTER_KEYCHAIN_SERVICE, account)
            .expect("read temporary item"),
        None
    );
    keychain
        .add_secret(ROUTER_KEYCHAIN_SERVICE, account, &[0x37; 32])
        .expect("write temporary item");
    assert_eq!(
        keychain
            .read_secret(ROUTER_KEYCHAIN_SERVICE, account)
            .expect("read temporary item"),
        Some(vec![0x37; 32])
    );
}
