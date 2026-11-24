use std::{
    future::Future,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use tokio::sync::oneshot;

use super::{EnrollmentRowStoreError, EnrollmentRowWrite};

pub(crate) struct TestDependencies {
    pub(crate) faults: Arc<TestWriteFaults>,
}

#[derive(Clone, Copy)]
pub(crate) enum TestWriteFault {
    BeforeCommit,
    CommitNotApplied,
    LoseAcknowledgement,
    PanicBeforeCommit,
}

#[derive(Default)]
pub(crate) struct TestWriteFaults {
    next: Mutex<Option<TestWriteFault>>,
    pub(crate) probes: AtomicUsize,
    replacement_witness: Mutex<Option<ReplacementWitnessGate>>,
    prepared_replacement: Mutex<Option<ReplacementWitnessGate>>,
    resource_cleanup: Mutex<Option<ReplacementWitnessGate>>,
}

struct ReplacementWitnessGate {
    reached: oneshot::Sender<()>,
    continue_after: oneshot::Receiver<()>,
}

impl TestWriteFaults {
    pub(crate) fn hold_replacement_witness(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::hold(&self.replacement_witness)
    }

    pub(crate) fn hold_prepared_replacement(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::hold(&self.prepared_replacement)
    }

    pub(crate) fn hold_resource_cleanup(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::hold(&self.resource_cleanup)
    }

    fn hold(
        slot: &Mutex<Option<ReplacementWitnessGate>>,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, at_witness) = oneshot::channel();
        let (release, continue_after) = oneshot::channel();
        *slot.lock().expect("enrollment test gate") = Some(ReplacementWitnessGate {
            reached,
            continue_after,
        });
        (at_witness, release)
    }

    pub(crate) async fn at_replacement_witness(&self) {
        Self::wait(&self.replacement_witness).await;
    }

    pub(crate) async fn at_prepared_replacement(&self) {
        Self::wait(&self.prepared_replacement).await;
    }

    pub(crate) async fn at_resource_cleanup(&self) {
        Self::wait(&self.resource_cleanup).await;
    }

    async fn wait(slot: &Mutex<Option<ReplacementWitnessGate>>) {
        let gate = slot.lock().ok().and_then(|mut slot| slot.take());
        if let Some(gate) = gate {
            let _ = gate.reached.send(());
            let _ = gate.continue_after.await;
        }
    }

    pub(crate) fn arm(&self, fault: TestWriteFault) {
        self.next.lock().expect("test write fault").replace(fault);
    }

    pub(crate) async fn apply<Abort>(
        &self,
        write: impl Future<Output = Result<EnrollmentRowWrite<Abort>, EnrollmentRowStoreError>>,
    ) -> Result<EnrollmentRowWrite<Abort>, EnrollmentRowStoreError> {
        let fault = self.next.lock().ok().and_then(|mut next| next.take());
        match fault {
            Some(TestWriteFault::BeforeCommit) => {
                Err(EnrollmentRowStoreError::Database(sqlx::Error::RowNotFound))
            }
            Some(TestWriteFault::CommitNotApplied) => {
                Err(EnrollmentRowStoreError::Commit(sqlx::Error::RowNotFound))
            }
            Some(TestWriteFault::PanicBeforeCommit) => panic!("injected enrollment worker panic"),
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
