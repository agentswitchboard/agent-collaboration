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
        if let Some(refusal) = self.stopped_writer_refusal() {
            return Err(refusal.activation());
        }
        let mut attempts = lock(&self.attempts);
        let (generation, mut session, skills, stopped) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => {
                let identity = OpenAttempt::Create(attempt);
                if generation == SessionGeneration::UNTRACKED {
                    return Err(self.mismatched_call().activation());
                }
                if let Some(entry) = attempts.runs.get(&generation) {
                    return Err(if entry.attempt == identity {
                        ActivationRefusal::AlreadyActive
                    } else {
                        self.mismatched_call().activation()
                    });
                }
                if !matches!(
                    attempts.creates.get(&attempt.0),
                    Some(CreateOpening::Configured(_))
                ) {
                    return Err(self.opening_refusal(&attempts, identity).activation());
                }
                let session_id = self
                    .session
                    .get()
                    .ok_or_else(|| self.mismatched_call().activation())?;
                let Some(CreateOpening::Configured(session)) = attempts.creates.remove(&attempt.0)
                else {
                    return Err(self.opening_refusal(&attempts, identity).activation());
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
                        attempt: identity,
                        stopped: stopped.clone(),
                        phase: RunOpening::Active,
                    },
                );
                (generation, *session, None, stopped)
            }
            ActivationSetup::Restored(run) => {
                let identity = OpenAttempt::Restore(run);
                let Some(entry) = attempts.runs.get_mut(&run.generation) else {
                    return Err(self.opening_refusal(&attempts, identity).activation());
                };
                if entry.attempt != identity {
                    return Err(self.mismatched_call().activation());
                }
                if entry.stopped.is_cancelled() {
                    return Err(self.opening_refusal(&attempts, identity).activation());
                }
                match entry.phase {
                    RunOpening::Opening(_) => {
                        return Err(self.opening_refusal(&attempts, identity).activation());
                    }
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
                    return Err(self.opening_refusal(&attempts, identity).activation());
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
