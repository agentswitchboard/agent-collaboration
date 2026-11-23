use std::sync::atomic::Ordering::SeqCst;

use switchboard_machine_enrollment::{
    AccessRequest, LeaseUnavailable, RefreshAnswer, RefreshCredential, RefreshOwner, RefreshSource,
};
use tokio::sync::OnceCell;

use super::{fixture::FixtureKeychain, *};

struct NoRefresh;

impl RefreshSource for NoRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        panic!("a replacement key must never reach Sign-in");
    }
}

struct ReplaceBeforeReadingCredential {
    reader: MachineEnrollmentStore,
    other_writer: MachineEnrollmentStore,
    keychain: Arc<FixtureKeychain>,
    replacement: EnrollmentBinding,
    reads_after_replacement: Arc<OnceCell<usize>>,
}

impl ReplaceBeforeReadingCredential {
    async fn replace_before_read(&self) {
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

impl CredentialStore for ReplaceBeforeReadingCredential {
    fn durability(&self) -> StoreDurability {
        self.reader.durability()
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        self.replace_before_read().await;
        self.reader.read().await
    }

    async fn read_for_key(
        &self,
        requested: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        self.replace_before_read().await;
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
    let reader = ReplaceBeforeReadingCredential {
        reader: fixture.store.clone(),
        other_writer: other_writer.clone(),
        keychain: fixture.keychain.clone(),
        replacement,
        reads_after_replacement: reads_after_replacement.clone(),
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
