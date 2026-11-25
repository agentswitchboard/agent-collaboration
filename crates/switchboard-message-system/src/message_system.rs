use std::{future::Future, sync::Arc};

use router_clock::RuntimeClock;
use switchboard_central_link::link_policy::LinkStatus;
use switchboard_message_custody::recipient_sessions::RecipientSessionDirectory;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    CloseReport, Event, Front, FrontRefusal, MessageFront, MessageRoots, MessageSystemEvents,
    MessageSystemObservation,
};
use crate::{
    interpreter::{Interpreter, MessageInputs, OpenedEnrollment, start_supervisor},
    message_unit::{MessageUnit, MessageUnitBuildError, UnitBuildInputs},
};

/// The message system of one Router.
#[must_use]
pub struct MessageSystem {
    close: CloseRequest,
    interpreter: JoinHandle<CloseReport>,
    front: MessageFront,
    status: watch::Receiver<MessageSystemObservation>,
}

struct CloseRequest(mpsc::UnboundedSender<Event>);

impl Drop for CloseRequest {
    fn drop(&mut self) {
        let _ = self.0.send(Event::Close);
    }
}

impl MessageSystem {
    #[cfg(all(feature = "test-support", debug_assertions))]
    pub(crate) fn metadata_reread(&self) -> impl Fn() -> bool + Send + use<> {
        let events = self.close.0.clone();
        move || events.send(Event::EnrollmentMayHaveChanged).is_ok()
    }

    /// Starts observation on the caller's Tokio runtime. Enrollment is observed
    /// asynchronously and can change while the Router is running.
    pub fn start(
        roots: MessageRoots,
        clock: Arc<dyn RuntimeClock>,
        recipients: Arc<dyn RecipientSessionDirectory>,
        events: MessageSystemEvents,
    ) -> Self {
        Self::start_with_parts(
            MessageInputs {
                roots,
                clock,
                recipients,
                events,
            },
            None,
            MessageUnit::build,
        )
    }

    pub(crate) fn start_with_parts<
        Build: Future<Output = Result<MessageUnit, MessageUnitBuildError>> + Send + 'static,
    >(
        inputs: MessageInputs,
        enrollment: Option<OpenedEnrollment>,
        build: impl Fn(UnitBuildInputs) -> Build + Send + Sync + 'static,
    ) -> Self {
        let (supervisor, effects) = start_supervisor();
        let initial = match supervisor.state().front() {
            Front::Refusing(reason) => reason,
            Front::Forwarding(_) => FrontRefusal::CouldNotStart,
        };
        let (front_out, front) = MessageFront::refusing(initial);
        let (status_out, status) = watch::channel(MessageSystemObservation::project(
            supervisor.state(),
            &LinkStatus::NotEnrolled,
            None,
        ));
        let (events, queue) = mpsc::unbounded_channel();
        let close = CloseRequest(events.clone());
        let interpreter =
            Interpreter::new(inputs, front_out, status_out, events, queue, enrollment);
        let interpreter = tokio::spawn(interpreter.run(supervisor, effects, build));
        Self {
            close,
            interpreter,
            front,
            status,
        }
    }

    #[must_use]
    pub fn front(&self) -> MessageFront {
        self.front.clone()
    }

    #[must_use]
    pub fn status(&self) -> watch::Receiver<MessageSystemObservation> {
        self.status.clone()
    }

    /// Queues Close and waits for the Supervisor's bounded close report.
    pub async fn close(self) -> CloseReport {
        drop(self.close);
        self.interpreter
            .await
            .unwrap_or(CloseReport::InterpreterLost)
    }
}
