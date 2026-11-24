use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use router_clock::RuntimeClock;
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::{CredentialStore, EnrollmentMetadata};
use switchboard_machine_enrollment_store::{
    EnrollmentJoin, EnrollmentObservationError, MachineEnrollmentStore,
};
use switchboard_message_custody::recipient_sessions::RecipientSessionDirectory;

use crate::message_front::FrontPublication;
use crate::{
    ActiveEnrollment, FrontRefusal, MessageFront, MessageUnit, MessageUnitBuildError,
    MessageUnitEvents, MessageUnitStopReport, PieceStop,
};

/// Owns startup's enrollment store and the single unit, if one started.
#[must_use]
pub struct MessageRuntime {
    front: MessageFront,
    state: Option<MessageRuntimeState>,
    enrollment_store: Option<MachineEnrollmentStore>,
}

enum MessageRuntimeState {
    Running(MessageUnit),
    Refusing(FrontPublication),
}

#[derive(Debug)]
pub struct MessageRuntimeStopReport {
    pub unit: Option<MessageUnitStopReport>,
    pub enrollment_store: Option<Result<EnrollmentJoin, EnrollmentObservationError>>,
}

impl MessageRuntimeStopReport {
    fn joined_cleanly(&self) -> bool {
        let unit_joined = self.unit.as_ref().is_none_or(|unit| {
            unit.driver == PieceStop::Joined(Ok(()))
                && unit.custody == PieceStop::Joined(Ok(()))
                && unit.settings == PieceStop::Joined(Ok(()))
                && matches!(&unit.lease, PieceStop::Joined(join) if join.worker.is_ok() && join.unsettled_retirements.is_empty())
        });
        unit_joined
            && self
                .enrollment_store
                .as_ref()
                .is_none_or(|store| store.as_ref().is_ok_and(|join| join.outcome.is_ok()))
    }
}

impl MessageRuntime {
    pub async fn start(
        state_root: &Path,
        secret_root: PathBuf,
        clock: Arc<dyn RuntimeClock>,
        recipients: Arc<dyn RecipientSessionDirectory>,
        events: MessageUnitEvents,
    ) -> Self {
        let Ok(machine_id) =
            load_or_create_machine_id(state_root, clock.utc_now().timestamp_millis()).await
        else {
            return Self::build_failed(None, MessageUnitBuildError::MachineIdentity);
        };
        let Ok(store) = MachineEnrollmentStore::open(
            &state_root.join(MACHINE_LOCAL_DATABASE),
            secret_root,
            machine_id.clone(),
            clock.clone(),
        )
        .await
        else {
            return Self::build_failed(None, MessageUnitBuildError::EnrollmentStore);
        };
        let unit_store = store.clone();
        Self::start_for_enrollment(store, |enrollment| {
            MessageUnit::build(
                state_root,
                machine_id,
                &unit_store,
                enrollment,
                clock,
                recipients,
                events,
            )
        })
        .await
    }

    /// Reads startup metadata once. The store remains owned through shutdown.
    pub(crate) async fn start_for_enrollment<
        Build: Future<Output = Result<MessageUnit, MessageUnitBuildError>>,
    >(
        store: MachineEnrollmentStore,
        build_unit: impl FnOnce(ActiveEnrollment) -> Build,
    ) -> Self {
        Self::start_from_metadata(store.read_metadata().await, store, build_unit).await
    }

    async fn start_from_metadata<
        Build: Future<Output = Result<MessageUnit, MessageUnitBuildError>>,
    >(
        metadata: Result<EnrollmentMetadata, switchboard_machine_enrollment::CredentialStoreError>,
        store: MachineEnrollmentStore,
        build_unit: impl FnOnce(ActiveEnrollment) -> Build,
    ) -> Self {
        let enrollment = match metadata {
            Ok(EnrollmentMetadata::Active {
                key, deployment, ..
            }) => ActiveEnrollment { key, deployment },
            Ok(EnrollmentMetadata::Empty) => {
                return Self::refusing(Some(store), FrontRefusal::NotEnrolled);
            }
            Ok(EnrollmentMetadata::Ended { .. }) => {
                return Self::refusing(Some(store), FrontRefusal::EnrollmentEnded);
            }
            Err(_) => return Self::refusing(Some(store), FrontRefusal::MetadataUnreadable),
        };
        match build_unit(enrollment).await {
            Ok(unit) => Self {
                front: unit.front(),
                state: Some(MessageRuntimeState::Running(unit)),
                enrollment_store: Some(store),
            },
            Err(error) => Self::build_failed(Some(store), error),
        }
    }

    fn build_failed(store: Option<MachineEnrollmentStore>, error: MessageUnitBuildError) -> Self {
        tracing::warn!(class = ?error, "Mac message link could not start");
        Self::refusing(store, FrontRefusal::CouldNotStart)
    }

    fn refusing(store: Option<MachineEnrollmentStore>, refusal: FrontRefusal) -> Self {
        let (publication, front) = MessageFront::refusing(refusal);
        Self {
            front,
            state: Some(MessageRuntimeState::Refusing(publication)),
            enrollment_store: store,
        }
    }

    pub fn without_router_root() -> Self {
        Self::refusing(None, FrontRefusal::NotEnrolled)
    }

    #[must_use]
    pub fn front(&self) -> MessageFront {
        self.front.clone()
    }

    pub async fn stop(&mut self) -> MessageRuntimeStopReport {
        let unit = match self.state.take() {
            Some(MessageRuntimeState::Running(unit)) => Some(unit.stop().await),
            Some(MessageRuntimeState::Refusing(front)) => {
                let _ = front.send_replace(Err(FrontRefusal::ShuttingDown));
                None
            }
            None => None,
        };
        let enrollment_store = match self.enrollment_store.take() {
            Some(store) => {
                store.request_close();
                Some(store.join().await)
            }
            None => None,
        };
        let report = MessageRuntimeStopReport {
            unit,
            enrollment_store,
        };
        if !report.joined_cleanly() {
            tracing::warn!("Mac message runtime did not stop cleanly");
        }
        report
    }
}

#[cfg(test)]
#[path = "message_runtime_test.rs"]
mod tests;
