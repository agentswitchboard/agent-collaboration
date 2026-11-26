//! Offer and settle a close against one exact routed opening.

use super::{
    Arc, Command, Effect, LifecycleRefusal, LifecycleReply, OpeningSlot, OwnerOpenings, RunAttempt,
    lock,
};
#[cfg(any(test, feature = "test-observation"))]
use crate::{owner_wire::OpeningPausePoint, owner_wire::OwnerAcpWire};
use provider_session_machines::{
    harness_lifecycle::CloseFact, session_lifecycle_machine::CloseAttempt,
};
use switchboard_harness_contracts::{CloseUncertainty, ReservedReply};
use tokio::sync::oneshot;

impl OwnerOpenings {
    pub(in super::super) fn close(
        &self,
        close: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal> {
        self.observe_health()?;
        let slot = OpeningSlot::Run(close.generation);
        let (instance, admitted, session) = {
            let mut attempts = lock(&self.attempts);
            if let Some(refusal) = attempts.port_refusal {
                return Err(refusal);
            }
            let Some(row) = attempts.get(slot) else {
                return Err(if let Some(create) = attempts.creates.values().next() {
                    create.phase().refusal()
                } else if attempts.runs.is_empty() {
                    self.idle_phase().refusal()
                } else {
                    LifecycleRefusal::AttemptMismatch
                });
            };
            let session = self
                .session
                .get()
                .ok_or_else(|| row.phase().refusal())?
                .clone();
            let instance = row.instance().clone();
            let admitted = attempts.apply(slot, &instance, Command::CloseAsked(close));
            (instance, admitted, session)
        };
        let Effect::CloseOffer(route, offer) = super::create::require_effect(admitted, &self.wire)?
        else {
            return Err(LifecycleRefusal::OperationNotOffered);
        };

        let (relay, received) = oneshot::channel();
        #[cfg(any(test, feature = "test-observation"))]
        self.wire
            .at_opening_effect_for_test(OpeningPausePoint::BeforeCloseOffer);
        if let Err(error) = self.wire.offer_close(
            &route,
            close,
            session,
            &offer,
            Box::new(CloseRelay {
                send: relay,
                #[cfg(any(test, feature = "test-observation"))]
                wire: Arc::clone(&self.wire),
            }),
        ) {
            let refused =
                lock(&self.attempts).apply(slot, &instance, Command::CloseOffered(close, false));
            if let Ok(effect) = refused {
                effect.finish(&self.wire);
            }
            let refusal = super::super::lifecycle_wire_refusal(error);
            if refusal == LifecycleRefusal::RouteUnavailable && route.is_removed() {
                self.observe_health()?;
                return Err(LifecycleRefusal::BoundRouteUnavailable);
            }
            return Err(refusal);
        }
        #[cfg(any(test, feature = "test-observation"))]
        self.wire
            .at_opening_effect_for_test(OpeningPausePoint::BeforeCloseCommit);
        let offered =
            lock(&self.attempts).apply(slot, &instance, Command::CloseOffered(close, true));
        if let Ok(effect) = offered {
            let faulted = effect.faulted();
            effect.finish(&self.wire);
            if faulted {
                reply.send(CloseFact::Unknown {
                    run: close,
                    cause: CloseUncertainty::EffectUnconfirmed,
                });
                return Ok(());
            }
        }

        // The frame was admitted even if retirement removed its row in
        // between. Its physical receipt can never settle a successor.
        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        self.tasks.spawn(async move {
            if let Ok(fact) = received.await {
                let settled = lock(&attempts).apply(slot, &instance, Command::CloseSettled(fact));
                if let Ok(effect) = settled {
                    let faulted = effect.faulted();
                    effect.finish(&wire);
                    reply.send(if faulted {
                        CloseFact::Unknown {
                            run: close,
                            cause: CloseUncertainty::EffectUnconfirmed,
                        }
                    } else {
                        fact
                    });
                }
            }
        });
        Ok(())
    }

    pub(in super::super) fn admit_again(&self, close: RunAttempt<CloseAttempt>) {
        if self.observe_health().is_err() {
            return;
        }
        let slot = OpeningSlot::Run(close.generation);
        let (instance, admitted) = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.get(slot) else {
                return;
            };
            let instance = row.instance().clone();
            let admitted = attempts.apply(slot, &instance, Command::AdmitAgain(close.attempt));
            (instance, admitted)
        };
        match admitted {
            Ok(effect @ Effect::Readmit(_, _)) => {
                let reopened = effect.finish(&self.wire);
                let result = lock(&self.attempts).apply(
                    slot,
                    &instance,
                    Command::ReadmitResult(close.attempt, reopened),
                );
                if let Ok(effect) = result {
                    effect.finish(&self.wire);
                }
            }
            Err(rejected) => {
                tracing::debug!(?close, reason=?rejected.reason, "ACP refused the exact close re-admission")
            }
            Ok(effect) => {
                effect.finish(&self.wire);
            }
        }
    }
}

struct CloseRelay {
    send: oneshot::Sender<CloseFact>,
    #[cfg(any(test, feature = "test-observation"))]
    wire: Arc<OwnerAcpWire>,
}

impl ReservedReply<CloseFact> for CloseRelay {
    fn send(self: Box<Self>, fact: CloseFact) {
        let _owner_retired = self.send.send(fact);
        #[cfg(any(test, feature = "test-observation"))]
        self.wire
            .at_opening_effect_for_test(OpeningPausePoint::CloseReceiptPublished);
    }
}
