//! Core's concrete timed jobs and whole-unit execution.
use crate::WorkspaceRuntime;
use central_engine_model::{
    DueBatchPolicy, DueRow, EngineTime, InstanceKey, MachineKind, RetirementThreshold,
};
use fleet_contracts::{AlarmResult, EngineSubject, LegacyUnitOutcome, ObservedControl};
use fleet_engine::{
    DueCutoff, DueDiscoveryError, FleetEngine, TimedDueDispatch, TimedRowOutcome,
    TimedSourceSchedule, WholeDueUnitControl, WholeUnitOutcome,
};
use fleet_mail::{
    DirectMark, ExpireAt, SystemMessageExpiry, SystemMessageExpiryStore, insert_message,
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_schedules::{self as automation_timer, AutomationStore};

#[derive(Clone, Copy)]
pub enum DueKind {
    Wake,
    Automation,
    DirectMessage,
    SystemMessageExpiry,
    #[cfg(feature = "test-support")]
    Probe,
}
impl DueKind {
    fn kind(self) -> MachineKind {
        match self {
            Self::Wake => MachineKind("wake_timer"),
            Self::Automation => MachineKind("automation_timer"),
            Self::DirectMessage => MachineKind("direct_message"),
            Self::SystemMessageExpiry => {
                <SystemMessageExpiry as central_engine_model::Machine>::KIND
            }
            #[cfg(feature = "test-support")]
            Self::Probe => MachineKind("engine_probe"),
        }
    }
}

pub struct CoreTimedRows;
impl<H: CentralStore, S: WorkspaceStores> TimedDueDispatch<WorkspaceRuntime<H, S>>
    for CoreTimedRows
{
    type TimedSourceKind = DueKind;
    fn ordered_timed_source_kinds(&self) -> &'static [DueKind] {
        &[
            DueKind::Wake,
            DueKind::Automation,
            DueKind::DirectMessage,
            DueKind::SystemMessageExpiry,
            #[cfg(feature = "test-support")]
            DueKind::Probe,
        ]
    }
    fn timed_source_schedule(&self, kind: DueKind) -> TimedSourceSchedule {
        TimedSourceSchedule {
            machine_kind: kind.kind(),
            due_cutoff_policy: if matches!(kind, DueKind::SystemMessageExpiry) {
                DueCutoff::BatchStart
            } else {
                DueCutoff::LookAheadHorizon
            },
        }
    }
    async fn discover_due_rows(
        &self,
        runtime: &WorkspaceRuntime<H, S>,
        kind: DueKind,
        cutoff: EngineTime,
        limit: usize,
    ) -> Result<Vec<DueRow>, DueDiscoveryError> {
        let store = runtime.execution.statement_host();
        match kind {
            DueKind::Wake => fleet_wakes::due_wakes(store, cutoff, limit).await,
            DueKind::Automation => fleet_schedules::due_schedules(store, cutoff, limit).await,
            DueKind::DirectMessage => fleet_mail::due_direct_messages(store, cutoff, limit).await,
            DueKind::SystemMessageExpiry => {
                fleet_mail::due_system_message_expiries(store, cutoff, limit).await
            }
            #[cfg(feature = "test-support")]
            DueKind::Probe => {
                fleet_engine::read_due_rows(
                    store,
                    central_engine_model::DueListKind::Probes,
                    cutoff,
                    limit,
                )
                .await
            }
        }
    }

    async fn dispatch_timed_due_row(
        &self,
        engine: &FleetEngine<'_, WorkspaceRuntime<H, S>>,
        runtime: &WorkspaceRuntime<H, S>,
        kind: DueKind,
        row: DueRow,
        previous_batch_start: Option<EngineTime>,
    ) -> TimedRowOutcome {
        let mut work = AlarmResult::default();
        let (done, down) = match kind {
            #[cfg(feature = "test-support")]
            DueKind::Probe => {
                engine
                    .run_due_machine_instance::<crate::engine_probe::EngineProbe, _>(
                        &crate::engine::probe_store::ProbeStore(runtime),
                        row,
                        "engine_probe_items.due_at",
                        |occurrence| crate::engine_probe::ProbeEvent::Due { occurrence },
                    )
                    .await
            }
            DueKind::Wake => runtime.dispatch_due_wake(engine, row).await,
            DueKind::Automation => {
                // A row already eligible at a prior warm batch can still be
                // pending after the cap or after a late recurrence is projected.
                let deferred = previous_batch_start
                    .and_then(|start| start.checked_add_signed(DueBatchPolicy::DEFAULT.look_ahead))
                    .is_some_and(|cutoff| row.effective_due_ms <= cutoff.timestamp_millis());
                engine
                    .run_due_machine_instance::<automation_timer::AutomationTimer, _>(
                        &AutomationStore::new(runtime, insert_message),
                        row,
                        "automation_schedules.next_due_at",
                        |occurrence| {
                            if deferred {
                                automation_timer::Event::DeferredDue { occurrence }
                            } else {
                                automation_timer::Event::Due { occurrence }
                            }
                        },
                    )
                    .await
            }
            DueKind::DirectMessage => {
                let subject = fleet_contracts::EngineSubject {
                    kind: central_engine_model::MachineKind("direct_message"),
                    key: row.key,
                };
                let (Ok(message_id), Some(occurrence), Ok(now)) = (
                    subject.key.0.parse(),
                    EngineTime::from_timestamp_millis(row.occurrence_ms),
                    runtime.execution.now(),
                ) else {
                    return TimedRowOutcome::CentralStoreFailed;
                };
                let store = &runtime.direct_messages;
                engine
                    .run_checked_due_machine_instance(
                        &subject,
                        store.apply_mark(DirectMark {
                            message_id,
                            occurrence,
                            now,
                            ids: runtime.execution.shared_ids(),
                        }),
                        |rejection, now| store.charge_rejected_mark(rejection, now),
                    )
                    .await
            }
            DueKind::SystemMessageExpiry => {
                engine
                    .run_due_machine_instance::<SystemMessageExpiry, _>(
                        &SystemMessageExpiryStore::new(runtime),
                        row,
                        "mail_messages.delivery_window_ends_at",
                        ExpireAt,
                    )
                    .await
            }
        };
        match kind {
            DueKind::Wake => work.wakes_fired = usize::from(done),
            DueKind::Automation => work.automations_fired = usize::from(done),
            // Cancels are observed through the messages they change.
            DueKind::DirectMessage | DueKind::SystemMessageExpiry => {}
            #[cfg(feature = "test-support")]
            DueKind::Probe => {}
        }

        if down {
            TimedRowOutcome::CentralStoreFailed
        } else {
            TimedRowOutcome::Handled(work)
        }
    }
}

#[derive(Clone, Copy)]
pub enum WholeKind {
    Windows,
    Drains,
}
impl WholeKind {
    pub(crate) fn description(self) -> WholeDueUnitControl {
        match self {
            Self::Windows => fleet_notices::window_unit(),
            Self::Drains => WholeDueUnitControl {
                attempt_subject: EngineSubject {
                    kind: MachineKind("legacy_workspace_drains"),
                    key: InstanceKey("all".into()),
                },
                retirement_threshold: RetirementThreshold::NOTICE_UNITS,
            },
        }
    }
}

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub(crate) async fn execute_whole_unit(
        &self,
        kind: WholeKind,
        now: EngineTime,
        subject: &EngineSubject,
        observed: &ObservedControl,
    ) -> Result<WholeUnitOutcome, LegacyUnitOutcome> {
        let mut work = AlarmResult::default();
        match kind {
            WholeKind::Windows => {
                let Some(_turn) = self.execution.try_central_turn() else {
                    return Ok(WholeUnitOutcome::Skipped);
                };
                #[cfg(feature = "test-support")]
                self.execution
                    .count_legacy_invocation(|count| count.windows += 1);
                work.windows_fired = self.run_window_unit(now, observed, subject).await?;
            }
            WholeKind::Drains => {
                #[cfg(feature = "test-support")]
                self.execution
                    .count_legacy_invocation(|count| count.drains += 1);
                work.workspaces_drained = self.drain_due_workspaces(now).await?;
            }
        }
        Ok(WholeUnitOutcome::Completed(work))
    }

    /// Runs under the central turn its caller took without waiting (change 7).
    async fn run_window_unit(
        &self,
        now: EngineTime,
        observed: &ObservedControl,
        subject: &EngineSubject,
    ) -> Result<usize, LegacyUnitOutcome> {
        let plan = fleet_notices::plan_due_windows_held(
            &self.execution,
            &crate::notice_mailbox::NoticeMailbox(&self.execution),
            now,
            subject,
            observed,
        )
        .await?;
        let count = plan.delivery_count();
        // The kept notice tests hold the commit that delivers windows at the
        // existing timer-commit points; an empty window unit passes them by.
        #[cfg(feature = "test-support")]
        if count > 0 {
            self.execution
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::BeforeTimerCommit)
                .await;
        }
        for delivery in plan.apply_held().await? {
            (self.mail_delivery_ready)(delivery);
        }
        #[cfg(feature = "test-support")]
        if count > 0 {
            self.execution
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::AfterTimerCommit)
                .await;
        }
        Ok(count)
    }
}
