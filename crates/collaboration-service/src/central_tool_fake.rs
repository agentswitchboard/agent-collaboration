//! A scripted stand-in for the link behind every Central-backed tool.
use std::{
    collections::VecDeque,
    future::ready,
    sync::{Mutex, PoisonError},
};

use collaboration_protocol::central_tool_port::{
    AdmittedLocalSession, CentralSubmission, CentralToolFuture, CentralToolPayload, CentralToolPort,
};
use collaboration_protocol::link_tool_result::LinkRefusal;
use collaboration_protocol::message_tools::SessionRef;

/// One scripted answer of the port: what the link reports, or how it refused.
pub type ScriptedCentralOutcome = Result<CentralSubmission, LinkRefusal>;

/// Records each submission with its admitted caller and replays scripted outcomes in order.
#[derive(Default)]
pub struct FakeCentralToolPort {
    submitted: Mutex<Vec<(SessionRef, CentralToolPayload)>>,
    outcomes: Mutex<VecDeque<ScriptedCentralOutcome>>,
}

impl FakeCentralToolPort {
    pub fn answering(outcomes: impl IntoIterator<Item = ScriptedCentralOutcome>) -> Self {
        Self {
            submitted: Mutex::default(),
            outcomes: Mutex::new(outcomes.into_iter().collect()),
        }
    }

    /// Every submission so far, oldest first.
    pub fn submitted(&self) -> Vec<(SessionRef, CentralToolPayload)> {
        self.submitted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The scripted outcomes no submission has used yet.
    pub fn unused_outcomes(&self) -> usize {
        self.outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl CentralToolPort for FakeCentralToolPort {
    #[expect(
        clippy::panic,
        reason = "a test must script an outcome for every submission it causes"
    )]
    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        payload: CentralToolPayload,
    ) -> CentralToolFuture<'call> {
        self.submitted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((caller.address().clone(), payload));
        let outcome = self
            .outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        let Some(outcome) = outcome else {
            panic!("FakeCentralToolPort received a submission with no scripted outcome");
        };
        Box::pin(ready(outcome))
    }
}
