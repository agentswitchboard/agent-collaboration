//! Activate ACP routes and drain their owned SDK readers.

use super::{CreateOpening, OpenedRun, OwnerOpenings, lock, transport_run};
use crate::owner_session_stream::OwnerStreamTarget;
use provider_session_machines::harness_lifecycle::{
    ActivationSetup, RunRetirement, SessionLifecycleFact, SessionLifecyclePort,
};
use std::sync::Arc;

impl OwnerOpenings {
    pub(in super::super) fn activate(
        &self,
        setup: ActivationSetup,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) {
        let mut attempts = lock(&self.attempts);
        let (generation, mut session, skills, stopped) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => {
                let Some(session_id) = self.session.get() else {
                    return;
                };
                if attempts.runs.contains_key(&generation)
                    || !matches!(
                        attempts.creates.get(&attempt.0),
                        Some(CreateOpening::Configured(_))
                    )
                {
                    return;
                }
                let Some(CreateOpening::Configured(session)) = attempts.creates.remove(&attempt.0)
                else {
                    return;
                };
                self.wire
                    .open_route(transport_run(self.owner, generation), session_id.clone());
                let stopped = self.stopped.child_token();
                attempts.runs.insert(
                    generation,
                    OpenedRun {
                        attempt: None,
                        stopped: stopped.clone(),
                        held: None,
                    },
                );
                (generation, *session, None, stopped)
            }
            ActivationSetup::Restored(run) => {
                let Some(entry) = attempts.runs.get_mut(&run.generation).filter(|entry| {
                    entry.attempt == Some(run.attempt) && !entry.stopped.is_cancelled()
                }) else {
                    return;
                };
                let Some(held) = entry.held.take() else {
                    return;
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
        self.wire.activate_route(route);
        session.register_in_folder_scope(&self.wire, route);
        let stream = OwnerStreamTarget::for_lifecycle(
            route,
            Arc::clone(&self.wire),
            stopped.clone(),
            Arc::clone(&events),
        );
        self.tasks.spawn(async move {
            let read = async {
                if let Some(view) = skills {
                    tokio::select! {
                        biased;
                        () = stopped.cancelled() => return,
                        _result = events.send(SessionLifecycleFact::SkillView { run: generation, view }) => {}
                    }
                }
                if session.read_stream(stream).await.is_err() {
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
    }
}
