use std::{
    future::Future,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use codex_router_secret_store::keychain_data_key::KeychainAccess;

use super::{EnrollmentRowStoreError, EnrollmentRowWrite};

pub(crate) struct TestDependencies {
    pub(crate) keychain: Arc<dyn KeychainAccess>,
    pub(crate) faults: Arc<TestWriteFaults>,
}

#[derive(Clone, Copy)]
pub(crate) enum TestWriteFault {
    BeforeCommit,
    CommitNotApplied,
    LoseAcknowledgement,
}

#[derive(Default)]
pub(crate) struct TestWriteFaults {
    next: Mutex<Option<TestWriteFault>>,
    pub(crate) probes: AtomicUsize,
}

impl TestWriteFaults {
    pub(crate) fn arm(&self, fault: TestWriteFault) {
        self.next.lock().expect("test write fault").replace(fault);
    }

    pub(crate) async fn apply(
        &self,
        write: impl Future<Output = Result<EnrollmentRowWrite<()>, EnrollmentRowStoreError>>,
    ) -> Result<EnrollmentRowWrite<()>, EnrollmentRowStoreError> {
        let fault = self.next.lock().ok().and_then(|mut next| next.take());
        match fault {
            Some(TestWriteFault::BeforeCommit) => {
                Err(EnrollmentRowStoreError::Database(sqlx::Error::RowNotFound))
            }
            Some(TestWriteFault::CommitNotApplied) => {
                Err(EnrollmentRowStoreError::Commit(sqlx::Error::RowNotFound))
            }
            Some(TestWriteFault::LoseAcknowledgement) => match write.await? {
                EnrollmentRowWrite::Applied => {
                    Err(EnrollmentRowStoreError::Commit(sqlx::Error::RowNotFound))
                }
                other => Ok(other),
            },
            None => write.await,
        }
    }
}
