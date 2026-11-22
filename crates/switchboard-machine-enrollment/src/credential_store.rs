use std::future::Future;

use crate::{BindingGeneration, EnrollmentBinding, EnrollmentEnd, StoredEnrollment};

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
}

/// The store reports whether the expected generation still owned a mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionalWrite {
    Applied,
    Superseded,
}

/// The store reports whether writing can produce a credential another process can read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreDurability {
    Durable,
    MemoryOnly,
}

/// Composition supplies this coherent, generation-fenced store for one Mac.
pub trait CredentialStore: Send + Sync {
    /// Whether an interactive command can report a durable enrollment.
    fn durability(&self) -> StoreDurability;

    fn read(&self) -> impl Future<Output = Result<StoredEnrollment, CredentialStoreError>> + Send;

    /// Atomically commits the complete binding at expected + 1 if expected is current.
    /// On error or cancellation, no uncommitted write may become visible later.
    fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Removes the current secret and records an ending at expected + 1.
    /// If cancelled before commit, no mutation may become visible later. If
    /// commit won, the next read must already expose the durable ending.
    fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;

    /// Records an ending at expected + 1 if physical secret removal failed.
    /// A reader of the marker must never return the old binding. Cancellation
    /// has the same commit and visibility contract as removal.
    fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> impl Future<Output = Result<ConditionalWrite, CredentialStoreError>> + Send;
}
