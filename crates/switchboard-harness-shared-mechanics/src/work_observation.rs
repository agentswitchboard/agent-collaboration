//! Observe the controller's work offer and hand-over at their decision points.

use switchboard_harness_contracts::DispatchAuthorization;

#[derive(Clone, Copy)]
pub enum WorkObservationKind {
    Prompt,
    ManagedNotice,
}

pub trait WorkObservationSink: Send + Sync {
    fn offered(&self, authorization: &DispatchAuthorization, work_kind: WorkObservationKind);

    fn newly_taken(&self, authorization: &DispatchAuthorization, work_kind: WorkObservationKind);
}
