//! Core's concrete timed jobs and whole-unit execution.
use crate::WorkspaceRuntime;
use central_engine_model::{DueRow, EngineTime, InstanceKey, MachineKind, RetirementThreshold};
use fleet_contracts::{AlarmResult, EngineSubject, LegacyUnitOutcome, ObservedControl};
use fleet_engine::{
    DueCutoff, DueDiscoveryError, Engine, TimedRowOutcome, TimedRows, TimedSource, WholeUnit,
    WholeUnitOutcome,
};
use fleet_mail::{
    DirectMark, ExpireAt, SystemMessageExpiry, SystemMessageExpiryStore, insert_message,
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_schedules::{self as automation_timer, AutomationStore};
use fleet_wakes::{self as wake_timer, WakeStore};

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
impl<H: CentralStore, S: WorkspaceStores> TimedRows<WorkspaceRuntime<H, S>> for CoreTimedRows {
    type Kind = DueKind;
    fn ordered_kinds(&self) -> &'static [DueKind] {
        &[
            DueKind::Wake,
            DueKind::Automation,
            DueKind::DirectMessage,
            DueKind::SystemMessageExpiry,
            #[cfg(feature = "test-support")]
            DueKind::Probe,
        ]
    }
    fn source(&self, kind: DueKind) -> TimedSource {
        TimedSource {
            machine_kind: kind.kind(),
            cutoff: if matches!(kind, DueKind::SystemMessageExpiry) {
                DueCutoff::BatchStart
            } else {
                DueCutoff::LookAhead
            },
        }
    }
    async fn discover(
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

    async fn run_row(
        &self,
        engine: &Engine<'_, WorkspaceRuntime<H, S>>,
        runtime: &WorkspaceRuntime<H, S>,
        kind: DueKind,
        row: DueRow,
    ) -> TimedRowOutcome {
        let mut work = AlarmResult::default();
        let (done, down) = match kind {
            #[cfg(feature = "test-support")]
            DueKind::Probe => {
                engine
                    .run_machine_unit::<crate::engine_probe::EngineProbe, _>(
                        &crate::engine::probe_store::ProbeStore(runtime),
                        row,
                        "engine_probe_items.due_at",
                        |occurrence| crate::engine_probe::ProbeEvent::Due { occurrence },
                    )
                    .await
            }
            DueKind::Wake => {
                engine
                    .run_machine_unit::<wake_timer::WakeTimer, _>(
                        &WakeStore::new(runtime, insert_message),
                        row,
                        "automation_wakes.due_at",
                        |occurrence| wake_timer::Event::Due { occurrence },
                    )
                    .await
            }
            DueKind::Automation => {
                engine
                    .run_machine_unit::<automation_timer::AutomationTimer, _>(
                        &AutomationStore::new(runtime, insert_message),
                        row,
                        "automation_schedules.next_due_at",
                        |occurrence| automation_timer::Event::Due { occurrence },
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
                    return TimedRowOutcome::CentralDown;
                };
                let store = &runtime.direct_messages;
                engine
                    .run_checked_due_unit(
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
                    .run_machine_unit::<SystemMessageExpiry, _>(
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
            TimedRowOutcome::CentralDown
        } else {
            TimedRowOutcome::Finished(work)
        }
    }
}

#[derive(Clone, Copy)]
pub enum WholeKind {
    Windows,
    Drains,
}
impl WholeKind {
    pub(crate) fn description(self) -> WholeUnit {
        match self {
            Self::Windows => fleet_notices::window_unit(),
            Self::Drains => WholeUnit {
                subject: EngineSubject {
                    kind: MachineKind("legacy_workspace_drains"),
                    key: InstanceKey("all".into()),
                },
                threshold: RetirementThreshold::NOTICE_UNITS,
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
