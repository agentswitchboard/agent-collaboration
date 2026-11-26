//! Deterministic controls for Host session owner tests.

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

#[derive(Default)]
pub(crate) struct PreparationFixture {
    restore_pause: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    restore_completion_queued: Mutex<Option<oneshot::Sender<()>>>,
    #[cfg(test)]
    harness_loss_pause: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    create_pause: Mutex<Option<PreparationPause>>,
    delivery_ack: Mutex<Option<DeliveryAckPause>>,
    #[cfg(test)]
    recovery_pause: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    named_steer_take: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    exact_steer_admission: Mutex<Option<ExactSteerAdmissionPause>>,
    #[cfg(test)]
    refuse_replacement: AtomicBool,
    restore_fault: AtomicBool,
}

struct PreparationPause {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) type SampledExactSteer = (
    collaboration_protocol::OperationId,
    crate::external_provider_supervisor::ExactTurnSteer,
);

#[cfg(test)]
struct ExactSteerAdmissionPause {
    reached: oneshot::Sender<SampledExactSteer>,
    release: oneshot::Receiver<()>,
}

pub(crate) struct DeliveryAckPause {
    reached: oneshot::Sender<DeliveryAcknowledgement>,
    release: oneshot::Receiver<()>,
    pub(crate) finished: oneshot::Sender<()>,
    pub(crate) lose_ack: bool,
}

pub struct DeliveryAckControl {
    pub reached: oneshot::Receiver<DeliveryAcknowledgement>,
    pub release: oneshot::Sender<()>,
    pub finished: oneshot::Receiver<()>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum DeliveryAcknowledgement {
    Accepted,
    Refused,
    OwnerStopped,
}

impl DeliveryAckPause {
    pub(crate) async fn resolve(
        self,
        admission: Result<(), crate::session_owner_door::OwnerDoorError>,
    ) -> (
        Result<(), crate::session_owner_door::OwnerDoorError>,
        Option<oneshot::Sender<()>>,
    ) {
        let observed = match &admission {
            Ok(()) => DeliveryAcknowledgement::Accepted,
            Err(crate::session_owner_door::OwnerDoorError::OwnerStopped) => {
                DeliveryAcknowledgement::OwnerStopped
            }
            Err(_) => DeliveryAcknowledgement::Refused,
        };
        let _test_withdrew_pause = self.reached.send(observed);
        let _test_withdrew_resume = self.release.await;
        let admission = if self.lose_ack {
            Err(crate::session_owner_door::OwnerDoorError::OwnerStopped)
        } else {
            admission
        };
        (admission, Some(self.finished))
    }
}

impl PreparationFixture {
    pub(crate) fn pause_delivery_ack(&self, lose_ack: bool) -> DeliveryAckControl {
        let (entered, reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (finished, complete) = oneshot::channel();
        let previous = self
            .delivery_ack
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(DeliveryAckPause {
                reached: entered,
                release: released,
                finished,
                lose_ack,
            });
        assert!(previous.is_none(), "prior delivery pause was not reached");
        DeliveryAckControl {
            reached,
            release,
            finished: complete,
        }
    }

    pub(crate) fn take_delivery_ack_pause(&self) -> Option<DeliveryAckPause> {
        self.delivery_ack
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    #[cfg(test)]
    pub(crate) fn pause_recovery(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::arm(&self.recovery_pause)
    }

    #[cfg(test)]
    pub(crate) fn refuse_next_replacement(&self) {
        self.refuse_replacement.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn take_replacement_refusal(&self) -> bool {
        self.refuse_replacement.swap(false, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(crate) fn hold_recovery(&self) -> Option<oneshot::Receiver<()>> {
        Self::enter(&self.recovery_pause)
    }

    pub(crate) fn pause_restore(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::arm(&self.restore_pause)
    }

    #[cfg(test)]
    pub(crate) fn pause_harness_loss(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::arm(&self.harness_loss_pause)
    }

    #[cfg(test)]
    pub(crate) fn hold_harness_loss(&self) -> Option<oneshot::Receiver<()>> {
        Self::enter(&self.harness_loss_pause)
    }

    #[cfg(test)]
    pub(crate) fn observe_restore_completion_queued(&self) -> oneshot::Receiver<()> {
        let (queued, observed) = oneshot::channel();
        let old = self
            .restore_completion_queued
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(queued);
        assert!(
            old.is_none(),
            "a previous restore completion witness is still armed"
        );
        observed
    }

    #[cfg(test)]
    pub(crate) fn witness_restore_completion_queued(&self) {
        if let Some(queued) = self
            .restore_completion_queued
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _fixture_stopped_waiting = queued.send(());
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_create(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::arm(&self.create_pause)
    }

    pub(crate) fn fault_restore(&self) {
        self.restore_fault.store(true, Ordering::Release);
    }

    pub(crate) fn fault_pending(&self) -> bool {
        self.restore_fault.load(Ordering::Acquire)
    }

    pub(crate) fn take_restore_fault(&self) -> bool {
        self.restore_fault.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn hold_restore(&self) -> Option<oneshot::Receiver<()>> {
        Self::enter(&self.restore_pause)
    }

    #[cfg(test)]
    pub(crate) fn hold_create(&self) -> Option<oneshot::Receiver<()>> {
        Self::enter(&self.create_pause)
    }

    #[cfg(test)]
    pub(crate) fn pause_named_steer_take(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        Self::arm(&self.named_steer_take)
    }

    #[cfg(test)]
    pub(crate) fn hold_named_steer_take(&self) -> Option<oneshot::Receiver<()>> {
        Self::enter(&self.named_steer_take)
    }

    #[cfg(test)]
    pub(crate) fn pause_exact_steer_admission(
        &self,
    ) -> (oneshot::Receiver<SampledExactSteer>, oneshot::Sender<()>) {
        let (reached, sample) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let previous = self
            .exact_steer_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(ExactSteerAdmissionPause {
                reached,
                release: released,
            });
        assert!(
            previous.is_none(),
            "the previous steer admission pause was never reached"
        );
        (sample, release)
    }

    #[cfg(test)]
    pub(crate) fn hold_exact_steer_admission(
        &self,
        operation: collaboration_protocol::OperationId,
        steer: crate::external_provider_supervisor::ExactTurnSteer,
    ) -> Option<oneshot::Receiver<()>> {
        let pause = self
            .exact_steer_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()?;
        let _test_withdrew = pause.reached.send((operation, steer));
        Some(pause.release)
    }

    fn arm(slot: &Mutex<Option<PreparationPause>>) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, reaching) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let previous = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(PreparationPause {
                entered,
                release: released,
            });
        assert!(previous.is_none(), "the prior pause was never reached");
        (reaching, release)
    }

    fn enter(slot: &Mutex<Option<PreparationPause>>) -> Option<oneshot::Receiver<()>> {
        let pause = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()?;
        let _test_withdrew_pause = pause.entered.send(());
        Some(pause.release)
    }
}
