//! Mechanics shared by harnesses while opening a provider session.

mod lifecycle_route;
mod opening_instance;
mod opening_policy;
mod opening_row;
mod opening_table;
#[cfg(test)]
mod opening_test_adapter;

pub use lifecycle_route::LifecycleRoute;
pub use opening_instance::{OpeningInstance, OpeningOffer};
pub use opening_policy::{
    OpeningEvent, OpeningPhase, OpeningPolicy, WrongOpeningPhase, activation_refusal,
};
pub use opening_row::{
    Command, CreateStart, DeferredRestore, Effect, HeldRestore, OpenCreateRequest,
    OpenRestoreRequest, OpeningRow, PendingSetter, ReaderEnd, ReaderHandoff, Rejected, Release,
    RestoreDecision, RestoreMilestone, RestoreOpening, RestoreStart, RunResources, SetterWrite,
    Slot, StepFault,
};
pub use opening_table::OpeningTable;

use switchboard_harness_contracts::{
    AcceptedRunSettings, LifecycleRefusal, SettingAttempt, SetupReport,
};

/// The owned values a harness places in its opening table.
pub trait OpeningAdapter: Sized {
    type OpenedSession;
    type SettingPlan;
    type Capabilities;
    type Settings;
    type Skills;
    type RestoreSignal: SendRestoreDecision;

    /// Accept the catalogue under the table guard without sending or re-entering the table.
    fn accept_catalogue(
        session: &mut Self::OpenedSession,
        accepted: AcceptedRunSettings,
    ) -> SetupReport<Self::Capabilities, Self::Settings>;

    /// Build the setter under the table guard after the row checks the generation.
    fn plan_setting_put_back(
        session_name: String,
        attempt: SettingAttempt,
        accepted_mode_fallback: Option<String>,
    ) -> Result<Self::SettingPlan, LifecycleRefusal>;
}

pub trait SendRestoreDecision {
    fn send(self, decision: RestoreDecision);
}

/// The harness handles these effects after unlocking the table.
pub trait OpeningEffects {
    fn retire_opening_route(&self, route: &LifecycleRoute, instance: &OpeningInstance);
    fn report_step_fault(&self, fault: &StepFaultReport);
}

pub struct StepFaultReport {
    pub phase: OpeningPhase,
    pub edge: OpeningEvent,
    pub declared: OpeningPhase,
    pub actual: Option<OpeningPhase>,
}
