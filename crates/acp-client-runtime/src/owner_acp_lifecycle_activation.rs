//! Activate ACP routes and drain their owned SDK readers.

use super::{CreateOpening, OpenedRun, OwnerOpenings, RunOpening, lock, transport_run};
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
        let mut attempts = lock(&self.attempts);
        let (generation, mut session, skills, stopped) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => {
                if generation == SessionGeneration::UNTRACKED {
                    return Err(ActivationRefusal::AttemptMismatch);
                }
                if let Some(entry) = attempts.runs.get(&generation) {
                    return Err(if entry.attempt == OpenAttempt::Create(attempt) {
                        ActivationRefusal::AlreadyActive
                    } else {
                        ActivationRefusal::AttemptMismatch
                    });
                }
                if !attempts.creates.is_empty() && !attempts.creates.contains_key(&attempt.0) {
                    return Err(ActivationRefusal::AttemptMismatch);
                }
                let session_id = self
                    .session
                    .get()
                    .ok_or(ActivationRefusal::OpeningNotHeld)?;
                if !matches!(
                    attempts.creates.get(&attempt.0),
                    Some(CreateOpening::Configured(_))
                ) {
                    return Err(ActivationRefusal::OpeningNotHeld);
                }
                let Some(CreateOpening::Configured(session)) = attempts.creates.remove(&attempt.0)
                else {
                    return Err(ActivationRefusal::OpeningNotHeld);
                };
                let route = transport_run(self.owner, generation);
                self.wire.open_route(route, session_id.clone());
                if !self.wire.activate_route(route) {
                    self.wire.apply_membership(
                        self.owner.incarnation,
                        TurnMembership::Retire(generation),
                    );
                    return Err(ActivationRefusal::RouteUnavailable);
                }
                let stopped = self.stopped.child_token();
                attempts.runs.insert(
                    generation,
                    OpenedRun {
                        attempt: OpenAttempt::Create(attempt),
                        stopped: stopped.clone(),
                        phase: RunOpening::Active,
                    },
                );
                (generation, *session, None, stopped)
            }
            ActivationSetup::Restored(run) => {
                let entry = attempts
                    .runs
                    .get_mut(&run.generation)
                    .ok_or(ActivationRefusal::OpeningNotHeld)?;
                if entry.attempt != OpenAttempt::Restore(run) {
                    return Err(ActivationRefusal::AttemptMismatch);
                }
                if entry.stopped.is_cancelled() {
                    return Err(ActivationRefusal::OpeningNotHeld);
                }
                match entry.phase {
                    RunOpening::Opening => return Err(ActivationRefusal::OpeningNotHeld),
                    RunOpening::Active => return Err(ActivationRefusal::AlreadyActive),
                    RunOpening::Held(_) => {}
                }
                if !self
                    .wire
                    .activate_route(transport_run(self.owner, run.generation))
                {
                    return Err(ActivationRefusal::RouteUnavailable);
                }
                let RunOpening::Held(held) =
                    std::mem::replace(&mut entry.phase, RunOpening::Active)
                else {
                    return Err(ActivationRefusal::OpeningNotHeld);
                };
                (
                    run.generation,
                    held.session,
                    held.skills,
                    entry.stopped.clone(),
                )
            }
        };
        drop(attempts);
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
                self.retire(generation);
                return Err(ActivationRefusal::ReaderUnavailable);
            }
        };
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
