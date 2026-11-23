use std::future::Future;

use crate::{
    BindingGeneration, EnrollmentBinding, EnrollmentEnd, EnrollmentKey, EnrollmentMetadata,
    StoredEnrollment,
};

/// A credential store adapter reports failed operations without carrying secret-bearing errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("the enrollment credential could not be read")]
    Read,
    #[error("the enrollment credential could not be replaced")]
    Replace,
    #[error("the enrollment credential could not be removed")]
    Remove,
    #[error("the enrollment cleanup deadline elapsed")]
    CleanupTimedOut,
    #[error("the enrollment write outcome is unknown")]
    OutcomeNotKnown,
}

/// The store reports whether the expected generation still owned a mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalWrite {
    Applied,
    Superseded,
}

/// An exact-key credential read can classify a replacement without opening its credential.
#[derive(Debug, Eq, PartialEq)]
pub enum KeyedEnrollment {
    Snapshot(StoredEnrollment),
    DifferentEnrollment,
}

impl KeyedEnrollment {
    /// Classifies an already-open, coherent snapshot from an in-memory store.
    pub fn from_snapshot(snapshot: StoredEnrollment, requested: &EnrollmentKey) -> Self {
        if snapshot.key().is_some_and(|key| &key != requested) {
            return Self::DifferentEnrollment;
        }
        Self::Snapshot(snapshot)
    }
}

/// The store reports whether writing can produce a credential another process can read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreDurability {
    Durable,
    MemoryOnly,
}

/// Composition supplies this coherent, generation-fenced store for one Mac.
///
/// Admitted mutations run to completion in the store's own task. Cancelling
/// or timing out a caller only stops it waiting. A following read orders after
/// the admitted mutation and must succeed before the caller acts on an unknown
/// outcome. A mutation error other than `OutcomeNotKnown` means the write did
/// not commit.
pub trait CredentialStore: Send + Sync {
    /// Whether an interactive command can report a durable enrollment.
    fn durability(&self) -> StoreDurability;

    fn read(&self) -> impl Future<Output = Result<StoredEnrollment, CredentialStoreError>> + Send;

    /// Compares the requested key in the same snapshot before opening its credential.
    fn read_for_key(
        &self,
        requested: &EnrollmentKey,
    ) -> impl Future<Output = Result<KeyedEnrollment, CredentialStoreError>> + Send;

    /// Reads enrollment identity and phase without accessing a refresh credential.
    fn read_metadata(
        &self,
    ) -> impl Future<Output = Result<EnrollmentMetadata, CredentialStoreError>> + Send;

    /// Atomically commits the complete binding at expected + 1 if expected is current.
    fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Removes the current secret and records an ending at expected + 1.
    fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Records an ending at expected + 1 if physical secret removal failed.
    /// A reader of the marker must never return the old binding.
    fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;
}
