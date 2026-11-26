//! Offer and settle a close against one exact routed opening.

use super::observations::{dispose, remove_state};
use super::restore::apply_milestone;
use super::row::{DeferredRestore, StoppedReceipt};
use super::{
    Arc, LifecycleRefusal, LifecycleReply, Mutex, OpeningAttempts, OpeningEvent, OpeningInstance,
    OpeningPhase, OpeningSlot, OpeningState, OwnerOpenings, RunAttempt, Transition, lock,
};
use crate::owner_opening_instance::OpeningOffer;
#[cfg(any(test, feature = "test-observation"))]
use crate::{owner_wire::OpeningPausePoint, owner_wire::OwnerAcpWire};
use provider_session_machines::{
    harness_lifecycle::CloseFact, session_lifecycle_machine::CloseAttempt,
};
use switchboard_harness_contracts::ReservedReply;
use tokio::sync::oneshot;

impl OwnerOpenings {
    pub(in super::super) fn close(
        &self,
        close: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal> {
        self.observe_health()?;
        let slot = OpeningSlot::Run(close.generation);
        let offer = OpeningOffer::default();
        let (instance, route, session) = {
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
            let instance = row.instance.clone();
            let route = attempts
                .apply(
                    slot,
                    &instance,
                    OpeningEvent::CloseAsked,
                    (),
                    |origin, ()| {
                        let Some(route) = origin.run().map(|run| run.route.clone()) else {
                            return Err((origin, ()));
                        };
                        Ok(Transition::Store(
                            OpeningState::OfferingClose {
                                origin: Box::new(origin),
                                attempt: close.attempt,
                                offer: offer.clone(),
                                deferred: None,
                            },
                            route,
                        ))
                    },
                )
                .map_err(|(why, ())| why)?;
            (instance, route, session)
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
            self.restore_unoffered_close(slot, &instance);
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
        let offered = {
            let mut attempts = lock(&self.attempts);
            let event = match attempts.exact(slot, &instance).map(|row| &row.state) {
                Some(OpeningState::OfferingClose { origin, .. })
                    if origin.phase() == OpeningPhase::Active =>
                {
                    OpeningEvent::CloseAcceptedLive
                }
                _ => OpeningEvent::CloseAcceptedRestore,
            };
            attempts.apply(slot, &instance, event, (), |state, ()| {
                let OpeningState::OfferingClose {
                    origin,
                    attempt,
                    offer,
                    deferred,
                } = state
                else {
                    return Err((state, ()));
                };
                let Some(run) = origin.run().cloned() else {
                    return Err((
                        OpeningState::OfferingClose {
                            origin,
                            attempt,
                            offer,
                            deferred,
                        },
                        (),
                    ));
                };
                let closing = if event == OpeningEvent::CloseAcceptedLive {
                    OpeningState::ClosingLive { run, attempt }
                } else {
                    OpeningState::ClosingRestore { run, attempt }
                };
                Ok(Transition::Store(closing, (origin, deferred)))
            })
        };
        if let Ok((origin, deferred)) = offered {
            drop(origin);
            if let Some(deferred) = deferred {
                let _worker_gone = deferred.outcome.send(false);
            }
        }

        // The frame was admitted even if retirement removed its row in
        // between. Its physical receipt can never settle a successor.
        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        self.tasks.spawn(async move {
            if let Ok(fact) = received.await
                && let Some(result) = finish_close(&attempts, slot, &instance, close, fact)
            {
                if let Some(ended) = result {
                    dispose(&wire, &instance, ended);
                }
                reply.send(fact);
            }
        });
        Ok(())
    }

    fn restore_unoffered_close(&self, slot: OpeningSlot, instance: &OpeningInstance) {
        let resolved = {
            let mut attempts = lock(&self.attempts);
            let Some(OpeningState::OfferingClose { origin, .. }) =
                attempts.exact(slot, instance).map(|row| &row.state)
            else {
                return;
            };
            let event = match origin.phase() {
                OpeningPhase::AwaitingRestore => OpeningEvent::CloseUnsentAwaiting,
                OpeningPhase::CapturingReplay => OpeningEvent::CloseUnsentReplay,
                OpeningPhase::HeldRestore => OpeningEvent::CloseUnsentHeld,
                OpeningPhase::PreparedRestore => OpeningEvent::CloseUnsentPrepared,
                OpeningPhase::Active => OpeningEvent::CloseUnsentActive,
                _ => return,
            };
            match attempts.apply(slot, instance, event, (), |state, ()| match state {
                OpeningState::OfferingClose {
                    origin, deferred, ..
                } => Ok(Transition::Store(*origin, deferred)),
                other => Err((other, ())),
            }) {
                Ok(Some(DeferredRestore { milestone, outcome })) => Some((
                    apply_milestone(&mut attempts, slot, instance, milestone),
                    outcome,
                )),
                Ok(None) | Err(_) => None,
            }
        };
        if let Some((admitted, outcome)) = resolved {
            let accepted = admitted.is_ok();
            if let Ok(Some(ended)) = admitted {
                dispose(&self.wire, instance, ended);
            }
            let _worker_gone = outcome.send(accepted);
        }
    }

    pub(in super::super) fn admit_again(&self, close: RunAttempt<CloseAttempt>) {
        if self.observe_health().is_err() {
            return;
        }
        let slot = OpeningSlot::Run(close.generation);
        let admitted = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.get(slot) else {
                return;
            };
            let instance = row.instance.clone();
            attempts.apply(
                slot,
                &instance,
                OpeningEvent::AdmitAgain,
                close.attempt,
                |state, wanted| match state {
                    OpeningState::CloseRefusedLive { run, attempt }
                        if attempt == wanted
                            && self.wire.reopen_opening_route(&run.route, attempt) =>
                    {
                        Ok(Transition::Store(OpeningState::Active(run), ()))
                    }
                    other => Err((other, wanted)),
                },
            )
        };
        if let Err((reason, _)) = admitted {
            tracing::debug!(?close, ?reason, "ACP refused the exact close re-admission");
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

fn finish_close(
    attempts: &Mutex<OpeningAttempts>,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    expected: RunAttempt<CloseAttempt>,
    fact: CloseFact,
) -> Option<Option<OpeningState>> {
    let (close, event) = match fact {
        CloseFact::Closed { run } => (run, OpeningEvent::CloseConfirmed),
        CloseFact::RefusedStillLive { run } => (run, OpeningEvent::CloseRefused),
        CloseFact::Unknown { run, .. } | CloseFact::NotOffered { run } => {
            (run, OpeningEvent::CloseUnknown)
        }
    };
    if close != expected {
        return None;
    }
    let mut attempts = lock(attempts);
    let row = attempts.exact(slot, instance)?;
    let current = match &row.state {
        OpeningState::ClosingRestore { attempt, .. }
        | OpeningState::ClosingLive { attempt, .. }
        | OpeningState::RouteLost {
            close: Some(attempt),
            ..
        }
        | OpeningState::StoppedAwaitingReply(StoppedReceipt::Close(_, attempt)) => *attempt,
        _ => return None,
    };
    if current != close.attempt {
        return None;
    }
    let event = if row.phase() == OpeningPhase::StoppedAwaitingReply {
        OpeningEvent::FinishOnStoppedWriter
    } else {
        event
    };
    attempts
        .apply(slot, instance, event, (), |state, ()| match state {
            OpeningState::ClosingLive { run, attempt } if event == OpeningEvent::CloseRefused => {
                Ok(Transition::Store(
                    OpeningState::CloseRefusedLive { run, attempt },
                    None,
                ))
            }
            other => remove_state(other, ()).map(|ending| match ending {
                Transition::Remove(removed) => Transition::Remove(Some(removed)),
                Transition::Store(state, effects) => Transition::Store(state, Some(effects)),
            }),
        })
        .ok()
}
