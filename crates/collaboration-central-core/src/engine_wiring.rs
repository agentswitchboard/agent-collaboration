//! Composes the retained engine with the runtime's execution capabilities.
use crate::workspace_runtime::engine_admin_dispatch::{KIND_REGISTRY, finish_registered_discard};
use crate::workspace_runtime::engine_dispatch::{CoreTimedRows, WholeKind};
use crate::{WorkspaceRuntime, organization_admin_contract::EngineKindName};
use central_engine_model::EngineTime;
use central_storage::StoreFailure;
use fleet_contracts::{
    AlarmNotPublished, CentralObservation, EngineSubject, EventCompletion, LegacyUnitOutcome,
    MailDeliveryReady, ObservedControl,
};
use fleet_engine::{
    AdminMachineKindRegistry, DiscardOutcome, EngineHost, FleetEngine, PreparedAttemptDiscard,
    WholeDueUnitControl, WholeUnitOutcome,
};
use fleet_runtime::DueBatchState;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};

impl<H: CentralStore, S: WorkspaceStores> EngineHost for WorkspaceRuntime<H, S> {
    type CentralStoreBackend = H;
    type TimedDueDispatch = CoreTimedRows;
    type WholeUnitKind = WholeKind;
    type AdminMachineKind = EngineKindName;
    fn central_store(&self) -> &H {
        self.execution.statement_host()
    }
    fn current_engine_time(&self) -> Result<EngineTime, StoreFailure> {
        self.execution.now()
    }
    fn host_id_source(&self) -> &dyn fleet_runtime_ports::IdSource {
        self.execution.ids()
    }
    fn due_batch_state(&self) -> &DueBatchState {
        self.execution.due_batches()
    }
    fn emit_central_observation(&self, observation: CentralObservation) {
        self.execution.observe(observation);
    }
    fn offer_ready_mail(&self, delivery: MailDeliveryReady) {
        (self.mail_delivery_ready)(delivery);
    }
    async fn organization_is_provisioned(&self) -> Result<bool, StoreFailure> {
        self.alarm_organization_is_provisioned().await
    }
    fn timed_due_dispatch(&self) -> &CoreTimedRows {
        &CoreTimedRows
    }
    fn admin_machine_kind_registry(
        &self,
    ) -> &'static AdminMachineKindRegistry<'static, Self::AdminMachineKind> {
        &KIND_REGISTRY
    }
    async fn finish_registered_discard(
        &self,
        discard: PreparedAttemptDiscard<'_, Self>,
    ) -> Result<DiscardOutcome, StoreFailure> {
        finish_registered_discard(discard).await
    }
    fn ordered_whole_units(&self) -> &'static [WholeKind] {
        &[WholeKind::Windows, WholeKind::Drains]
    }
    fn whole_unit(&self, kind: WholeKind) -> WholeDueUnitControl {
        kind.description()
    }
    async fn run_whole_unit(
        &self,
        kind: WholeKind,
        now: EngineTime,
        subject: &EngineSubject,
        observed: &ObservedControl,
    ) -> Result<WholeUnitOutcome, LegacyUnitOutcome> {
        self.execute_whole_unit(kind, now, subject, observed).await
    }
    #[cfg(feature = "test-support")]
    fn engine_test_pause_points(&self) -> &fleet_runtime::PausePoints {
        self.execution.pause_points()
    }
    #[cfg(feature = "test-support")]
    fn record_engine_completion_entry(&self) {
        self.execution.record_engine_completion_entry();
    }
}

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    /// Await the engine's completion after the event's own work.
    pub async fn after_event(&self) -> Result<EventCompletion, AlarmNotPublished> {
        self.execution.after_event(&FleetEngine::new(self)).await
    }

    /// Completes the drive of a retained physical alarm.
    pub async fn after_alarm(&self) -> Result<EventCompletion, AlarmNotPublished> {
        FleetEngine::new(self).complete_alarm().await
    }
}
