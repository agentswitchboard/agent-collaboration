//! Stage the exact reader, then authorize it only after its real handoff.

use super::policy::activation_refusal;
use super::row::{ReaderEnd, ReaderHandoff};
use super::{
    Arc, Command, Effect, OpeningSlot, OwnerOpenings, RunRetirement, SessionLifecycleFact,
    SessionLifecyclePort, lock,
};
use crate::owner_session_stream::OwnerStreamTarget;
use provider_session_machines::harness_lifecycle::{ActivationRefusal, ActivationSetup};

impl OwnerOpenings {
    pub(in super::super) fn activate(
        &self,
        setup: ActivationSetup,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), ActivationRefusal> {
        self.observe_health().map_err(activation_refusal)?;
        let (instance, generation, handoff) = lock(&self.attempts).begin_reader(self, setup)?;
        let Effect::StartReader(handoff) = handoff else {
            handoff.finish(&self.wire);
            return Err(ActivationRefusal::OperationNotOffered);
        };
        let slot = OpeningSlot::Run(generation);
        let ReaderHandoff {
            mut session,
            skills,
            run,
            install_route,
        } = handoff;
        let route = &run.route;
        let handoff = (|| {
            if install_route {
                let session_id = self
                    .session
                    .get()
                    .ok_or(ActivationRefusal::AttemptMismatch)?;
                self.wire
                    .install_opening_route(route, session_id.clone())
                    .map_err(|_| ActivationRefusal::RouteUnavailable)?;
            }
            self.observe_health().map_err(activation_refusal)?;
            if !self.wire.activate_opening_route(route) {
                return Err(ActivationRefusal::RouteUnavailable);
            }
            session.register_in_folder_opening(&self.wire, route);
            let stream = OwnerStreamTarget::for_opening_route(
                route,
                Arc::clone(&self.wire),
                run.stopped.clone(),
                Arc::clone(&events),
            )
            .ok_or(ActivationRefusal::RouteUnavailable)?;
            session
                .prepare_stream(stream)
                .map_err(|_| ActivationRefusal::ReaderUnavailable)
        })();
        let (start, reader) = match handoff {
            Ok(reader) => reader,
            Err(reason) => {
                #[cfg(any(test, feature = "test-observation"))]
                self.wire.at_opening_effect_for_test(
                    crate::owner_wire::OpeningPausePoint::BeforeFailedHandoffCommit,
                );
                let ended = lock(&self.attempts).apply(slot, &instance, Command::ActivationFailed);
                if let Ok(effect) = ended {
                    effect.finish(&self.wire);
                }
                // This actor alone can accept the handoff. If its failure
                // arrives after StartingReader has gone, the earlier
                // retirement or observed loss already fenced its exact route.
                return Err(reason);
            }
        };
        #[cfg(any(test, feature = "test-observation"))]
        self.wire.at_opening_effect_for_test(
            crate::owner_wire::OpeningPausePoint::ReaderHandoffAccepted,
        );
        self.observe_health().map_err(activation_refusal)?;
        {
            let mut attempts = lock(&self.attempts);
            if self.connection_retired.is_cancelled() {
                drop(attempts);
                let retired = self.connection_retirement_observer();
                retired();
                return Err(ActivationRefusal::ConnectionRetired);
            }
            let route_lost = route.is_removed();
            let command = if route_lost {
                Command::RouteRemoved(route.clone())
            } else {
                Command::ReaderAccepted
            };
            let accepted = attempts.apply(slot, &instance, command);
            drop(attempts);
            if route_lost {
                if let Ok(effect) = accepted {
                    effect.finish(&self.wire);
                }
                return Err(ActivationRefusal::RouteUnavailable);
            }
            let effect = accepted.map_err(|rejected| activation_refusal(rejected.reason))?;
            if !effect.finish(&self.wire) {
                return Err(ActivationRefusal::OperationNotOffered);
            }
        }
        if install_route {
            self.watch_route(slot, instance.clone(), route.clone());
        }
        let stopped = run.stopped;
        let route = run.route;
        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        self.tasks.spawn(async move {
            let read = async {
                if let Some(view) = skills
                    && events
                        .send(SessionLifecycleFact::SkillView {
                            run: generation,
                            view,
                        })
                        .await
                        .is_err()
                {
                    return ReaderEnd::IngressClosed;
                }
                if route.is_removed() {
                    return ReaderEnd::HandoffClosed;
                }
                if start.send(()).is_err() {
                    return ReaderEnd::HandoffClosed;
                }
                if reader.await.is_err() {
                    ReaderEnd::Failed
                } else {
                    ReaderEnd::Finished
                }
            };
            let reason = tokio::select! {
                biased;
                () = stopped.cancelled() => return,
                () = retired => ReaderEnd::Retired,
                reason = read => reason,
            };
            let ended = lock(&attempts).apply(slot, &instance, Command::ReaderStopped(reason));
            #[cfg(any(test, feature = "test-observation"))]
            {
                use crate::owner_wire::{OpeningPausePoint, OpeningReaderEnd};
                let observed = match reason {
                    ReaderEnd::Retired => OpeningReaderEnd::Retired,
                    ReaderEnd::Finished => OpeningReaderEnd::Finished,
                    ReaderEnd::Failed => OpeningReaderEnd::Failed,
                    ReaderEnd::IngressClosed => OpeningReaderEnd::IngressClosed,
                    ReaderEnd::HandoffClosed => OpeningReaderEnd::HandoffClosed,
                };
                wire.observe_reader_ending_for_test(route.run, observed);
                wire.at_opening_effect_for_test(OpeningPausePoint::ReaderStoppedDelivered);
            }
            if let Ok(effect) = ended {
                let report = matches!(
                    effect,
                    Effect::ReaderEnded {
                        report_loss: true,
                        ..
                    }
                );
                effect.finish(&wire);
                if report {
                    let _owner_gone = events
                        .send(SessionLifecycleFact::RunObservationFailed { run: generation })
                        .await;
                }
            }
        });
        Ok(())
    }
}
