//! The engine's view of its host (P3, module direction):
//! Engine modules depend on EngineHost. Central core implements it for WorkspaceRuntime.
use super::attempt_administration::{
    AdminMachineKindRegistry, DiscardOutcome, PreparedAttemptDiscard,
};
use super::due_batch_execution::{TimedDueDispatch, WholeDueUnitControl, WholeUnitOutcome};
use central_engine_model::EngineTime;
use central_storage::StoreFailure;
use fleet_contracts::CentralObservation;
use fleet_contracts::{
    AlarmNotPublished, EngineSubject, EventCompletion, EventCompletionHook, LegacyUnitOutcome,
    MailDeliveryReady, ObservedControl,
};
use fleet_runtime::DueBatchState;
use fleet_runtime_ports::CentralStore;
use std::future::Future;

pub trait EngineHost: Send + Sync + Sized {
    type TimedDueDispatch: TimedDueDispatch<Self>;
    type WholeUnitKind: Copy + Send + Sync + 'static;
    type CentralStoreBackend: CentralStore;
    type AdminMachineKind: Copy + Eq + Send + Sync + 'static;
    fn central_store(&self) -> &Self::CentralStoreBackend;
    /// The one injected clock.
    fn current_engine_time(&self) -> Result<EngineTime, StoreFailure>;
    fn host_id_source(&self) -> &dyn fleet_runtime_ports::IdSource;
    fn due_batch_state(&self) -> &DueBatchState;
    fn emit_central_observation(&self, observation: CentralObservation);
    fn offer_ready_mail(&self, delivery: MailDeliveryReady);
    /// Today's organization stamp check (P3): `Ok(false)` when unprovisioned.
    fn organization_is_provisioned(
        &self,
    ) -> impl Future<Output = Result<bool, StoreFailure>> + Send;
    fn timed_due_dispatch(&self) -> &Self::TimedDueDispatch;
    fn admin_machine_kind_registry(
        &self,
    ) -> &'static AdminMachineKindRegistry<'static, Self::AdminMachineKind>;
    /// Selects the finisher and event for an attempt the engine resolved on this host.
    fn finish_registered_discard(
        &self,
        discard: PreparedAttemptDiscard<'_, Self>,
    ) -> impl Future<Output = Result<DiscardOutcome, StoreFailure>> + Send;
    fn ordered_whole_units(&self) -> &'static [Self::WholeUnitKind];
    fn whole_unit(&self, kind: Self::WholeUnitKind) -> WholeDueUnitControl;
    fn run_whole_unit(
        &self,
        kind: Self::WholeUnitKind,
        now: EngineTime,
        subject: &EngineSubject,
        observed: &ObservedControl,
    ) -> impl Future<Output = Result<WholeUnitOutcome, LegacyUnitOutcome>> + Send;
    #[cfg(feature = "test-support")]
    fn engine_test_pause_points(&self) -> &fleet_runtime::PausePoints;
    #[cfg(feature = "test-support")]
    fn record_engine_completion_entry(&self);
}

/// The engine bound to one host. Every engine operation is a method here, so
/// none is attributed to WorkspaceRuntime.
pub struct FleetEngine<'a, Host: EngineHost>(pub(crate) &'a Host);

impl<'a, Host: EngineHost> FleetEngine<'a, Host> {
    pub fn new(host: &'a Host) -> Self {
        Self(host)
    }
    pub(crate) fn central_store(&self) -> &'a Host::CentralStoreBackend {
        self.0.central_store()
    }
    pub(crate) fn current_engine_time(&self) -> Result<EngineTime, StoreFailure> {
        self.0.current_engine_time()
    }
    pub(crate) fn host_id_source(&self) -> &'a dyn fleet_runtime_ports::IdSource {
        self.0.host_id_source()
    }
    pub(crate) fn due_batch_state(&self) -> &'a DueBatchState {
        self.0.due_batch_state()
    }
    pub(crate) fn emit_central_observation(&self, observation: CentralObservation) {
        self.0.emit_central_observation(observation);
    }
    #[cfg(feature = "test-support")]
    pub(crate) fn engine_test_pause_points(&self) -> &'a fleet_runtime::PausePoints {
        self.0.engine_test_pause_points()
    }
}

impl<Host: EngineHost> EventCompletionHook for FleetEngine<'_, Host> {
    async fn complete_event(&self) -> Result<EventCompletion, AlarmNotPublished> {
        self.complete_event_with_observation().await
    }
}
