use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use switchboard_machine_enrollment::{
    AccessRequest, LeaseUnavailable, RefreshAnswer, RefreshCredential, RefreshOwner, RefreshSource,
    SecretValue, TokenGrant,
};
use tokio::sync::OnceCell;

use super::{fixture::FixtureKeychain, *};

struct NoRefresh;

impl RefreshSource for NoRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        panic!("a replacement key must never reach Sign-in");
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReplaceAt {
    CredentialRead,
    UncertainRotation,
}

struct ReplacementStore {
    reader: MachineEnrollmentStore,
    other_writer: MachineEnrollmentStore,
    keychain: Arc<FixtureKeychain>,
    replacement: EnrollmentBinding,
    reads_after_replacement: Arc<OnceCell<usize>>,
    replace_at: ReplaceAt,
    rotation_writes: Arc<AtomicUsize>,
}

impl ReplacementStore {
    async fn install_replacement(&self) {
        self.reads_after_replacement
            .get_or_init(|| async {
                assert_eq!(
                    self.other_writer
                        .replace(generation(1), &self.replacement)
                        .await,
                    Ok(ConditionalWrite::Applied)
                );
                self.keychain.unavailable.store(true, SeqCst);
                self.keychain.reads.load(SeqCst)
            })
            .await;
    }
}

impl CredentialStore for ReplacementStore {
    fn durability(&self) -> StoreDurability {
        self.reader.durability()
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        if self.replace_at == ReplaceAt::CredentialRead {
            self.install_replacement().await;
        }
        self.reader.read().await
    }

    async fn read_for_key(
        &self,
        requested: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        if self.replace_at == ReplaceAt::CredentialRead {
            self.install_replacement().await;
        }
        self.reader.read_for_key(requested).await
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        self.reader.read_metadata().await
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> WriteResult {
        self.rotation_writes.fetch_add(1, SeqCst);
        if self.replace_at == ReplaceAt::UncertainRotation {
            self.install_replacement().await;
            return Err(CredentialStoreError::OutcomeNotKnown);
        }
        self.reader.replace(expected, binding).await
    }

    async fn remove(&self, expected: BindingGeneration, why: EnrollmentEnd) -> WriteResult {
        self.reader.remove(expected, why).await
    }

    async fn mark_unusable(&self, expected: BindingGeneration, why: EnrollmentEnd) -> WriteResult {
        self.reader.mark_unusable(expected, why).await
    }
}

#[tokio::test]
async fn an_exact_key_read_rejects_a_replacement_before_opening_its_credential() {
    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    let other_writer = fixture.another_worker().await;
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, "fictional-replacement");
    let reads_after_replacement = Arc::new(OnceCell::new());
    let reader = ReplacementStore {
        reader: fixture.store.clone(),
        other_writer: other_writer.clone(),
        keychain: fixture.keychain.clone(),
        replacement,
        reads_after_replacement: reads_after_replacement.clone(),
        replace_at: ReplaceAt::CredentialRead,
        rotation_writes: Arc::new(AtomicUsize::default()),
    };
    let original_key = original.key();
    let owner = RefreshOwner::new(reader, NoRefresh, fixture.clock.clone(), original.context);
    assert_eq!(
        owner.lease_for(&original_key, AccessRequest::Usable).await,
        Err(LeaseUnavailable::DifferentEnrollment)
    );
    assert_eq!(
        fixture.keychain.reads.load(SeqCst),
        *reads_after_replacement.get().unwrap(),
        "a replacement credential must not reach the Keychain"
    );
    fixture.store.close().await.unwrap();
    other_writer.close().await.unwrap();
}

struct RotateRefreshCredential;

impl RefreshSource for RotateRefreshCredential {
    async fn refresh(&self, previous: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Rotated(TokenGrant {
            credential: RefreshCredential {
                enrollment_id: previous.enrollment_id,
                refresh_token: SecretValue::new("fictional-rotated-refresh".into()).unwrap(),
            },
            access_token: SecretValue::new("fictional-rotation-access".into()).unwrap(),
            expires_in: Duration::from_secs(60),
        })
    }
}

#[tokio::test]
async fn uncertain_rotation_recovery_does_not_open_or_write_a_replacement_key() {
    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    let other_writer = fixture.another_worker().await;
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, "fictional-replacement");
    let reads_after_replacement = Arc::new(OnceCell::new());
    let rotation_writes = Arc::new(AtomicUsize::default());
    let store = ReplacementStore {
        reader: fixture.store.clone(),
        other_writer: other_writer.clone(),
        keychain: fixture.keychain.clone(),
        replacement: replacement.clone(),
        reads_after_replacement: reads_after_replacement.clone(),
        replace_at: ReplaceAt::UncertainRotation,
        rotation_writes: rotation_writes.clone(),
    };
    let original_key = original.key();
    let owner = RefreshOwner::new(
        store,
        RotateRefreshCredential,
        fixture.clock.clone(),
        original.context,
    );

    assert_eq!(
        owner.lease_for(&original_key, AccessRequest::Usable).await,
        Err(LeaseUnavailable::DifferentEnrollment)
    );
    assert_eq!(
        fixture.keychain.reads.load(SeqCst),
        *reads_after_replacement.get().unwrap()
    );
    assert_eq!(rotation_writes.load(SeqCst), 1);
    assert_eq!(
        fixture.store.read_metadata().await,
        Ok(EnrollmentMetadata::Active {
            key: replacement.key(),
            generation: replacement.generation,
            deployment: replacement.context.deployment,
        })
    );
    fixture.store.close().await.unwrap();
    other_writer.close().await.unwrap();
}

#[tokio::test]
async fn ended_enrollment_without_a_deployment_is_corrupt() {
    let fixture = Fixture::open().await;
    let mut connection = fixture.connection().await;
    let document = serde_json::json!({
        "state": "ended", "organizationId": fixture::ORGANIZATION,
        "enrollmentId": ENROLLMENT, "why": {"kind": "revoked"},
    });
    sqlx::query("UPDATE machine_identity SET enrollment_generation = 1, enrollment_json = ?")
        .bind(document.to_string())
        .execute(&mut connection)
        .await
        .unwrap();
    let before = row_columns(&mut connection).await;
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);

    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::Corrupt(EnrollmentRowCorruption::Json))
    ));
    assert_eq!(
        fixture.store.replace(generation(1), &replacement).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, before);
}

#[tokio::test]
async fn ended_enrollment_refuses_a_noncanonical_stored_deployment() {
    let fixture = Fixture::open().await;
    let mut connection = fixture.connection().await;
    let document = serde_json::json!({
        "state": "ended", "organizationId": fixture::ORGANIZATION,
        "deployment": "https://signin.example", "enrollmentId": ENROLLMENT,
        "why": {"kind": "revoked"},
    });
    sqlx::query("UPDATE machine_identity SET enrollment_generation = 1, enrollment_json = ?")
        .bind(document.to_string())
        .execute(&mut connection)
        .await
        .unwrap();
    let before = row_columns(&mut connection).await;
    let key = fixture.binding(2, ENROLLMENT, TOKEN).key();
    let replacement = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);

    assert!(matches!(
        fixture.detail().await,
        Err(ReadFailure::Corrupt(EnrollmentRowCorruption::Deployment))
    ));
    assert_eq!(
        fixture.store.read_for_key(&key).await,
        Err(CredentialStoreError::Read)
    );
    assert_eq!(
        fixture.store.read_metadata().await,
        Err(CredentialStoreError::Read)
    );
    assert_eq!(
        fixture.store.replace(generation(1), &replacement).await,
        Err(CredentialStoreError::Replace)
    );
    assert_eq!(row_columns(&mut connection).await, before);
}

#[tokio::test]
async fn replace_compares_the_organization_in_the_locked_snapshot() {
    const OTHER_ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000004";
    let fixture = Fixture::open().await;
    fixture.install().await;
    assert_eq!(
        fixture
            .store
            .remove(generation(1), EnrollmentEnd::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    let replacement = fixture.binding(3, ALTERNATE_ENROLLMENT, TOKEN);
    let (prepared, release) = fixture.faults.hold_prepared_replacement();
    let mut replace = Box::pin(fixture.store.replace(generation(2), &replacement));
    assert_queued(replace.as_mut());
    prepared.await.unwrap();
    let mut connection = fixture.connection().await;
    sqlx::query("UPDATE machine_identity SET enrollment_json = json_set(enrollment_json, '$.organizationId', ?)")
        .bind(OTHER_ORGANIZATION)
        .execute(&mut connection).await.unwrap();
    let changed_row = row_columns(&mut connection).await;
    let changes = fixture.store.subscribe_enrollment_changes();

    release.send(()).unwrap();
    let result = replace.await;

    assert_eq!(result, Err(CredentialStoreError::EnrollmentScopeMismatch));
    assert_eq!(row_columns(&mut connection).await, changed_row);
    assert!(!changes.has_changed().unwrap());
}
