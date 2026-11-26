//! Stage the exact reader, then authorize it only after its real handoff.

use super::policy::activation_refusal;
use super::{
    Arc, Command, OpeningSlot, OwnerOpenings, ReaderHandoff, RunRetirement, SessionLifecycleFact,
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
                let ended = lock(&self.attempts).apply(slot, &instance, Command::ActivationFailed);
                if let Ok(effect) = ended {
                    effect.finish(&self.wire);
                } else if !route.is_removed() {
                    self.wire.retire_opening_route(route, &instance);
                }
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
            let accepted = attempts.apply(slot, &instance, Command::ReaderAccepted);
            drop(attempts);
            accepted
                .map_err(|rejected| activation_refusal(rejected.reason))?
                .finish(&self.wire);
        }
        if install_route {
            self.watch_route(slot, instance, route.clone());
        }
        let stopped = run.stopped;
        let route = run.route;
        self.tasks.spawn(async move {
            let read = async {
                if let Some(view) = skills {
                    tokio::select! {
                        biased;
                        () = stopped.cancelled() => return,
                        _result = events.send(SessionLifecycleFact::SkillView { run: generation, view }) => {}
                    }
                }
                if stopped.is_cancelled() || route.is_removed() {
                    return;
                }
                let _reader_retired = start.send(());
                if reader.await.is_err() {
                    tokio::select! {
                        biased;
                        () = stopped.cancelled() => {}
                        _result = events.send(SessionLifecycleFact::RunObservationFailed { run: generation }) => {}
                    }
                }
            };
            tokio::pin!(read);
            tokio::select! {
                () = retired => {
                    stopped.cancel();
                    read.await;
                }
                () = &mut read => {}
            }
        });
        Ok(())
    }
}
