//! Stage the exact reader, then authorize it only after its real handoff.

use super::observations::{dispose, remove_state, run_resources};
use super::policy::activation_refusal;
use super::row::{HeldRestore, RunResources};
use super::{
    Arc, OpenAttempt, OpeningEvent, OpeningInstance, OpeningPhase, OpeningSlot, OpeningState,
    OwnerOpenings, RunRetirement, SessionGeneration, SessionLifecycleFact, SessionLifecyclePort,
    Transition, lock,
};
use crate::{owner_session_open::OwnerOpenedSession, owner_session_stream::OwnerStreamTarget};
use provider_session_machines::harness_lifecycle::{ActivationRefusal, ActivationSetup};
use session_event_model::SkillView;

struct ReaderHandoff {
    session: Box<OwnerOpenedSession>,
    skills: Option<SkillView>,
    run: RunResources,
    install_route: bool,
}

impl OwnerOpenings {
    pub(in super::super) fn activate(
        &self,
        setup: ActivationSetup,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), ActivationRefusal> {
        self.observe_health().map_err(activation_refusal)?;
        let (instance, generation, handoff) = self.begin_reader(setup)?;
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
                let ended = {
                    lock(&self.attempts).apply(
                        slot,
                        &instance,
                        OpeningEvent::ActivationFailed,
                        (),
                        remove_state,
                    )
                };
                if let Ok(ended) = ended {
                    dispose(&self.wire, &instance, ended);
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
            attempts
                .apply(
                    slot,
                    &instance,
                    OpeningEvent::ReaderAccepted,
                    (),
                    |state, ()| match state {
                        OpeningState::StartingReader(run) if !run.route.is_removed() => {
                            Ok(Transition::Store(OpeningState::Active(run), ()))
                        }
                        other => Err((other, ())),
                    },
                )
                .map_err(|(why, ())| activation_refusal(why))?;
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

    fn begin_reader(
        &self,
        setup: ActivationSetup,
    ) -> Result<(OpeningInstance, SessionGeneration, ReaderHandoff), ActivationRefusal> {
        let mut attempts = lock(&self.attempts);
        if let Some(reason) = attempts.port_refusal {
            return Err(activation_refusal(reason));
        }
        let (identity, generation) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => (OpenAttempt::Create(attempt), generation),
            ActivationSetup::Restored(run) => (OpenAttempt::Restore(run), run.generation),
        };
        let run_slot = OpeningSlot::Run(generation);
        if generation == SessionGeneration::UNTRACKED {
            return Err(ActivationRefusal::AttemptMismatch);
        }
        if let Some(row) = attempts.get(run_slot) {
            if row.attempt != identity {
                return Err(ActivationRefusal::AttemptMismatch);
            }
            if row.phase() == OpeningPhase::Active {
                return Err(ActivationRefusal::AlreadyActive);
            }
            if matches!(identity, OpenAttempt::Create(_)) {
                return Err(activation_refusal(row.phase().refusal()));
            }
        }
        let slot = OpeningSlot::for_attempt(identity);
        let instance = attempts
            .targeted(identity)
            .map(|row| row.instance.clone())
            .ok_or_else(|| activation_refusal(self.opening_refusal(&attempts, identity)))?;
        let handoff = attempts
            .apply(slot, &instance, OpeningEvent::Activate, (), |state, ()| {
                let handoff = match state {
                    OpeningState::ConfiguredCreate(session) => ReaderHandoff {
                        session,
                        skills: None,
                        run: run_resources(self.owner, generation, &self.stopped),
                        install_route: true,
                    },
                    OpeningState::PreparedRestore(restore) => {
                        let HeldRestore {
                            session, skills, ..
                        } = *restore.held;
                        ReaderHandoff {
                            session,
                            skills,
                            run: restore.run,
                            install_route: false,
                        }
                    }
                    other => return Err((other, ())),
                };
                Ok(Transition::Store(
                    OpeningState::StartingReader(handoff.run.clone()),
                    handoff,
                ))
            })
            .map_err(|(why, ())| activation_refusal(why))?;
        if handoff.install_route
            && let Some(row) = attempts.remove(slot)
        {
            attempts.insert(run_slot, row);
        }
        Ok((instance, generation, handoff))
    }
}
