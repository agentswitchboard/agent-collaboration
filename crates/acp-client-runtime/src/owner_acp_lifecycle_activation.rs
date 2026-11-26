//! Activate ACP routes and drain their owned SDK readers.

use super::policy::activation_refusal;
use super::{OpenedRun, OpeningEvent, OpeningPhase, OwnerOpenings, lock, transport_run};
use crate::owner_session_stream::OwnerStreamTarget;
use provider_session_machines::SessionGeneration;
use provider_session_machines::harness_lifecycle::{
    ActivationRefusal, ActivationSetup, OpenAttempt, RunRetirement, SessionLifecycleFact,
    SessionLifecyclePort,
};
use provider_session_machines::session_owner::TurnMembership;
use std::sync::Arc;

impl OwnerOpenings {
    pub(in super::super) fn activate(
        &self,
        setup: ActivationSetup,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), ActivationRefusal> {
        if let Some(refusal) = self.writer_refusal() {
            return Err(activation_refusal(refusal));
        }
        let mut attempts = lock(&self.attempts);
        let (generation, mut session, skills, stopped) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => {
                let identity = OpenAttempt::Create(attempt);
                if generation == SessionGeneration::UNTRACKED {
                    return Err(activation_refusal(self.mismatched_call()));
                }
                if let Some(entry) = attempts.runs.get(&generation) {
                    return Err(if entry.attempt != identity {
                        activation_refusal(self.mismatched_call())
                    } else if entry.phase == OpeningPhase::Active {
                        ActivationRefusal::AlreadyActive
                    } else {
                        activation_refusal(entry.phase.refusal())
                    });
                }
                if !attempts
                    .creates
                    .get(&attempt.0)
                    .is_some_and(|entry| entry.phase.accepts(OpeningEvent::Activate))
                {
                    return Err(activation_refusal(
                        self.opening_refusal(&attempts, identity),
                    ));
                }
                let session_id = self
                    .session
                    .get()
                    .ok_or_else(|| activation_refusal(self.mismatched_call()))?;
                let route = transport_run(self.owner, generation);
                self.wire.open_route(route, session_id.clone());
                if !self.wire.activate_route(route) {
                    self.wire.apply_membership(
                        self.owner.incarnation,
                        TurnMembership::Retire(generation),
                    );
                    return Err(ActivationRefusal::RouteUnavailable);
                }
                let Some(mut opening) = attempts.creates.remove(&attempt.0) else {
                    return Err(activation_refusal(self.mismatched_call()));
                };
                let Some(session) = opening.configured.take() else {
                    opening.phase.advance(OpeningEvent::ActivationFailed);
                    self.wire.apply_membership(
                        self.owner.incarnation,
                        TurnMembership::Retire(generation),
                    );
                    return Err(ActivationRefusal::ReaderUnavailable);
                };
                let stopped = self.stopped.child_token();
                attempts.runs.insert(
                    generation,
                    OpenedRun {
                        attempt: identity,
                        stopped: stopped.clone(),
                        phase: opening.phase,
                        instance: opening.instance,
                        held: None,
                        close_attempt: None,
                    },
                );
                (generation, *session, None, stopped)
            }
            ActivationSetup::Restored(run) => {
                let identity = OpenAttempt::Restore(run);
                let Some(entry) = attempts.runs.get_mut(&run.generation) else {
                    return Err(activation_refusal(
                        self.opening_refusal(&attempts, identity),
                    ));
                };
                if entry.attempt != identity {
                    return Err(activation_refusal(self.mismatched_call()));
                }
                if entry.stopped.is_cancelled() {
                    return Err(ActivationRefusal::RouteUnavailable);
                }
                if entry.phase == OpeningPhase::Active {
                    return Err(ActivationRefusal::AlreadyActive);
                }
                if !entry.phase.accepts(OpeningEvent::Activate) {
                    return Err(activation_refusal(entry.phase.refusal()));
                }
                if !self
                    .wire
                    .activate_route(transport_run(self.owner, run.generation))
                {
                    return Err(ActivationRefusal::RouteUnavailable);
                }
                let Some(held) = entry.held.take() else {
                    return Err(ActivationRefusal::ReaderUnavailable);
                };
                (
                    run.generation,
                    held.session,
                    held.skills,
                    entry.stopped.clone(),
                )
            }
        };
        let route = transport_run(self.owner, generation);
        session.register_in_folder_scope(&self.wire, route);
        let stream = OwnerStreamTarget::for_lifecycle(
            route,
            Arc::clone(&self.wire),
            stopped.clone(),
            Arc::clone(&events),
        );
        let reader = match session.start_stream(stream) {
            Ok(reader) => reader,
            Err(_) => {
                if let Some(mut entry) = attempts.runs.remove(&generation) {
                    entry.phase.advance(OpeningEvent::ActivationFailed);
                }
                self.wire
                    .apply_membership(self.owner.incarnation, TurnMembership::Retire(generation));
                stopped.cancel();
                return Err(ActivationRefusal::ReaderUnavailable);
            }
        };
        let Some(entry) = attempts.runs.get_mut(&generation) else {
            return Err(ActivationRefusal::RouteUnavailable);
        };
        if !entry.phase.advance(OpeningEvent::Activate) {
            return Err(activation_refusal(entry.phase.refusal()));
        }
        drop(attempts);
        self.tasks.spawn(async move {
            let read = async {
                if let Some(view) = skills {
                    tokio::select! {
                        biased;
                        () = stopped.cancelled() => return,
                        _result = events.send(SessionLifecycleFact::SkillView { run: generation, view }) => {}
                    }
                }
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
