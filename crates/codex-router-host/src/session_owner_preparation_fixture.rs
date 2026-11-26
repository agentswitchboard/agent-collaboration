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
    create_pause: Mutex<Option<PreparationPause>>,
    delivery_ack: Mutex<Option<DeliveryAckPause>>,
    #[cfg(test)]
    recovery_pause: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    named_steer_take: Mutex<Option<PreparationPause>>,
    #[cfg(test)]
    exact_steer_admission: Mutex<Option<ExactSteerAdmissionPause>>,
    #[cfg(test)]
    terminal_publication: Mutex<Option<BlockingEffectPause<()>>>,
    #[cfg(test)]
    dispatch_mark: Mutex<Option<BlockingEffectPause<router_clock::ClockInstant>>>,
    #[cfg(test)]
    dispatch_expiry: Mutex<Option<BlockingEffectPause<()>>>,
    #[cfg(test)]
    replacement_installed: Mutex<Option<oneshot::Sender<()>>>,
    #[cfg(test)]
    refuse_replacement: AtomicBool,
    restore_fault: AtomicBool,
}

struct PreparationPause {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
struct BlockingEffectPause<T> {
    reached: oneshot::Sender<T>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
impl<T> BlockingEffectPause<T> {
    fn arm(slot: &Mutex<Option<Self>>) -> (oneshot::Receiver<T>, std::sync::mpsc::Sender<()>) {
        let (send, reached) = oneshot::channel();
        let (release, receive) = std::sync::mpsc::channel();
        let prior = slot.lock().expect("effect fixture").replace(Self {
            reached: send,
            release: receive,
        });
        assert!(prior.is_none(), "the preceding effect fixture did not fire");
        (reached, release)
    }

    fn hold(slot: &Mutex<Option<Self>>, value: T) {
        let pause = slot.lock().expect("effect fixture").take();
        if let Some(pause) = pause
            && pause.reached.send(value).is_ok()
        {
            let _released = pause.release.recv();
        }
    }
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
    #[cfg(test)]
    pub(crate) fn observe_replacement_installation(&self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let previous = self
            .replacement_installed
            .lock()
            .expect("replacement fixture")
            .replace(sender);
        assert!(previous.is_none());
        receiver
    }

    #[cfg(test)]
    pub(crate) fn observe_installed_replacement(&self) {
        if let Some(observer) = self
            .replacement_installed
            .lock()
            .expect("replacement fixture")
            .take()
        {
            let _test_stopped = observer.send(());
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_dispatch_expiry(
        &self,
    ) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        BlockingEffectPause::arm(&self.dispatch_expiry)
    }

    #[cfg(test)]
    pub(crate) fn before_store_offer(
        &self,
        request: &provider_session_machines::session_owner::SessionStoreRequest,
    ) {
        use provider_session_machines::session_owner::{
            RemoteEffect, SessionOwnerReply, SessionStoreRequest,
        };
        if matches!(request, SessionStoreRequest::UndoDispatch(_))
            || matches!(request, SessionStoreRequest::RecordTerminal { reply, .. } if matches!(
                reply.as_ref(),
                SessionOwnerReply::InputFinished { remote: RemoteEffect::None, .. }
                | SessionOwnerReply::InputUnsent { .. }
            ))
        {
            BlockingEffectPause::hold(&self.dispatch_expiry, ());
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_terminal_publication(
        &self,
    ) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        BlockingEffectPause::arm(&self.terminal_publication)
    }

    #[cfg(test)]
    pub(crate) fn hold_terminal_publication(&self) {
        BlockingEffectPause::hold(&self.terminal_publication, ());
    }

    #[cfg(test)]
    pub(crate) fn pause_dispatch_mark(
        &self,
    ) -> (
        oneshot::Receiver<router_clock::ClockInstant>,
        std::sync::mpsc::Sender<()>,
    ) {
        BlockingEffectPause::arm(&self.dispatch_mark)
    }

    #[cfg(test)]
    pub(crate) fn hold_dispatch_mark(&self, deadline: router_clock::ClockInstant) {
        BlockingEffectPause::hold(&self.dispatch_mark, deadline);
    }

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

#[cfg(test)]
pub(crate) fn publishes_turn_ending(
    batch: &provider_session_machines::session_owner::PublicationBatch,
) -> bool {
    use provider_session_machines::session_owner::{
        PublicationBatch, PublicationFact, SessionPublication,
    };
    matches!(batch, PublicationBatch::Commit { facts, .. } if facts.iter().any(|fact| {
        matches!(fact, PublicationFact::Live(SessionPublication::TurnEnded { .. }))
    }))
}
