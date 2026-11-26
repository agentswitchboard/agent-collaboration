//! ACP frames submitted through the session owner's ordered writer.

mod owner_opening_offers;
pub(crate) use owner_opening_offers::RestoreOffer;
mod owner_output;
pub(crate) use owner_output::{OwnerAcpOutput, PromptTerminal};
#[cfg(any(test, feature = "test-observation"))]
mod owner_opening_test_control;
#[cfg(any(test, feature = "test-observation"))]
pub use crate::owner_opening_disposal::{OpeningDisposalKind, OpeningDisposalProbe};
#[cfg(feature = "test-observation")]
pub use owner_opening_test_control::{OpeningAsyncPause, ProviderConnectionEnd};
#[cfg(any(test, feature = "test-observation"))]
pub use owner_opening_test_control::{OpeningPause, OpeningPausePoint, OpeningReaderEnd};
pub(crate) mod owner_restore_update_router;
pub mod owner_session_open;
pub mod owner_session_setting;
pub mod owner_session_stream;
mod owner_work_offers;

use crate::acp_transport::{
    AcpTransport, CallbackKind, CallbackResolution, SlotEvidence, TransportRefusal, TransportRun,
};
use crate::owner_session_open::{
    OpenFrame, OpenReply, OpenRequest, OwnerSessionOpenReceipt, OwnerSessionOpenSetup,
};
use crate::owner_session_setting::{CorrelatedSetting, OwnerSettingWirePlan, SettingCorrelation};
use crate::session_owner_task::OwnerCompletion;
use agent_client_protocol::schema::v1::{CancelNotification, CloseSessionRequest};
use agent_client_protocol::{Error, JsonRpcMessage as _, UntypedMessage};
use provider_session_machines::SessionGeneration;
use provider_session_machines::harness_lifecycle::{
    CloseFact, LifecycleReply, SettingFact, owner_event_for_close, owner_event_for_restore_setting,
};
use provider_session_machines::session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia};
use provider_session_machines::session_owner::{
    DispatchAuthorization, DispatchResult, OwnerIncarnation, OwnerSetting, RunAttempt,
    TurnMembership,
};
use provider_session_machines::session_turn_machine::TurnId;
use provider_session_machines::tool_ownership::{ToolObservation, ToolOwnershipEffect};
use session_event_model::ProviderAuthStatus;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};
use switchboard_harness_contracts::{
    HandOverRefusal, HarnessOwnerEvents, InputId, OwnerEvidence, ReservedWorkReply, SteerIdlePolicy,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

enum AcpFrame {
    Request {
        message: UntypedMessage,
        correlation: ResponseKind,
    },
    Cancel(CancelNotification),
    Open(OpenFrame),
    #[cfg(feature = "test-observation")]
    FixtureDispatchFence(FixtureDispatchPoint, oneshot::Sender<Result<(), Error>>),
    #[cfg(feature = "test-observation")]
    FixtureStopWriter,
    #[cfg(feature = "test-observation")]
    FixturePauseWriter(oneshot::Sender<()>, oneshot::Receiver<()>),
}

#[cfg(feature = "test-observation")]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum FixtureDispatchPoint {
    Cancel,
    Restore,
}

#[cfg(feature = "test-observation")]
type FixtureDispatchObservation =
    Option<(FixtureDispatchPoint, oneshot::Sender<Result<(), Error>>)>;

enum ResponseKind {
    Prompt {
        authorization: DispatchAuthorization,
        turn: TurnId,
        compact: bool,
        reader: Option<mpsc::Sender<PromptTerminal>>,
        completion: ReservedWorkReply,
    },
    Steer {
        authorization: DispatchAuthorization,
        idle: SteerIdlePolicy,
        completion: ReservedWorkReply,
    },
    Close {
        run: RunAttempt<CloseAttempt>,
        completion: LifecycleReply<CloseFact>,
    },
    Setting(Box<CorrelatedSetting>),
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerWireRefusal {
    #[error(transparent)]
    Transport(#[from] TransportRefusal),
    #[error("the Router couldn't encode the provider request")]
    Encoding,
}

pub(crate) type AcpOwnerEvents = dyn HarnessOwnerEvents<ProviderAuthStatus>;

pub struct OwnerAcpWire {
    transport: AcpTransport<AcpFrame>,
    route_edits: Mutex<()>,
    #[cfg(any(test, feature = "test-observation"))]
    opening_test: owner_opening_test_control::OpeningTestControls,
    readers: Mutex<BTreeMap<TransportRun, mpsc::Sender<PromptTerminal>>>,
    restore_updates: Arc<crate::owner_restore_updates::RestoreUpdates>,
    cursor_plans: Arc<crate::provider_cursor_plan_items::CursorPlanItems>,
    behavior: Arc<dyn crate::ProviderBehavior>,
    owner_events: Mutex<BTreeMap<OwnerIncarnation, Weak<AcpOwnerEvents>>>,
    #[cfg(feature = "test-observation")]
    fixture_dispatch: Mutex<FixtureDispatchObservation>,
    #[cfg(feature = "test-observation")]
    fixture_created_handoff: Mutex<Option<crate::owner_session_open::CreatedHandoffFault>>,
}

impl OwnerAcpWire {
    #[cfg(all(debug_assertions, feature = "test-observation"))]
    pub fn pause_work_handover_for_test(
        &self,
        point: crate::handover_test_control::HandoverPausePoint,
        input: session_event_model::InputId,
    ) -> crate::handover_test_control::HandoverPause {
        self.transport.handover_test.arm(point, input)
    }

    #[cfg(all(debug_assertions, feature = "test-observation"))]
    pub fn try_recovered_handover_for_test(
        &self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, HandOverRefusal> {
        self.hand_over_work(authorization, || true)
    }

    pub(crate) fn new(
        capacity: NonZeroUsize,
        behavior: Arc<dyn crate::ProviderBehavior>,
    ) -> (Self, OwnerAcpOutput) {
        let (transport, output) = AcpTransport::new(capacity);
        let restore_updates = Arc::new(crate::owner_restore_updates::RestoreUpdates::default());
        (
            Self {
                transport,
                route_edits: Mutex::default(),
                #[cfg(any(test, feature = "test-observation"))]
                opening_test: Default::default(),
                readers: Mutex::default(),
                restore_updates: restore_updates.clone(),
                cursor_plans: Arc::default(),
                behavior,
                owner_events: Mutex::default(),
                #[cfg(feature = "test-observation")]
                fixture_dispatch: Mutex::default(),
                #[cfg(feature = "test-observation")]
                fixture_created_handoff: Mutex::default(),
            },
            OwnerAcpOutput {
                output,
                restore_updates,
            },
        )
    }

    /// Close the next created SDK reader's exact activation channel on demand.
    #[cfg(feature = "test-observation")]
    pub fn close_created_handoff_for_test(&self) -> (oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (close, close_requested) = oneshot::channel();
        let (closed, closure_observed) = oneshot::channel();
        let previous = self
            .fixture_created_handoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(crate::owner_session_open::CreatedHandoffFault {
                close_requested,
                closed,
            });
        assert!(
            previous.is_none(),
            "the earlier create handoff fault has not been used"
        );
        (close, closure_observed)
    }

    /// Arms a one-shot wire fence for an actual owner dispatch attempt.
    #[cfg(feature = "test-observation")]
    pub fn observe_dispatch_for_test(
        &self,
        point: FixtureDispatchPoint,
    ) -> oneshot::Receiver<Result<(), Error>> {
        let (complete, observed) = oneshot::channel();
        self.fixture_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace((point, complete));
        observed
    }

    /// Called at the completed owner step whether the preceding frame was enqueued or lost.
    #[cfg(feature = "test-observation")]
    pub fn fence_dispatch_for_test(&self, run: TransportRun, point: FixtureDispatchPoint) {
        let observation = self
            .fixture_dispatch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|(armed, _)| *armed == point);
        if let Some((point, complete)) = observation {
            let _enqueued = self
                .transport
                .enqueue_control(run, AcpFrame::FixtureDispatchFence(point, complete));
        }
    }

    pub fn open_route(&self, run: TransportRun, session: String) {
        let _route_edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.transport.open_route(run, session);
    }

    /// Reserves the real writer's free slots until the returned guard is dropped.
    #[cfg(feature = "test-observation")]
    #[must_use]
    pub fn hold_writer_capacity_for_test(&self) -> impl Sized + use<> {
        self.transport.hold_ordered_capacity()
    }

    pub fn create(
        &self,
        owner: OwnerIncarnation,
        setup: OwnerSessionOpenSetup,
    ) -> Result<OwnerSessionOpenReceipt, TransportRefusal> {
        self.enqueue_open(
            TransportRun {
                incarnation: owner,
                generation: SessionGeneration::UNTRACKED,
            },
            OpenRequest::New(setup),
        )
    }

    pub fn restore(
        &self,
        run: TransportRun,
        attempt: LoadAttempt,
        session: String,
        via: RestoreVia,
        setup: OwnerSessionOpenSetup,
    ) -> Result<OwnerSessionOpenReceipt, TransportRefusal> {
        self.restore_opening(run, attempt, session, via, setup)
            .map(|(_instance, receipt)| receipt)
    }

    pub(crate) fn restore_opening(
        &self,
        run: TransportRun,
        attempt: LoadAttempt,
        session: String,
        via: RestoreVia,
        setup: OwnerSessionOpenSetup,
    ) -> Result<
        (
            crate::owner_opening_instance::OpeningInstance,
            OwnerSessionOpenReceipt,
        ),
        TransportRefusal,
    > {
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tap = crate::owner_opening_instance::OpeningInstance::default();
        if !self.transport.attach_restore_registration(run, None, &tap) {
            return Err(TransportRefusal::RouteUnavailable);
        }
        let updates = self
            .restore_updates
            .open_instance(run, session.clone(), &tap);
        let reply = self.enqueue_open(
            run,
            OpenRequest::Restore {
                provider_id: session,
                attempt,
                tap: tap.clone(),
                updates,
                via,
                setup,
            },
        );
        if reply.is_err() {
            self.restore_updates.discard_unoffered(run, &tap);
            self.transport.clear_restore_registration(run, &tap);
        }
        reply.map(|receipt| (tap, receipt))
    }

    fn enqueue_open(
        &self,
        run: TransportRun,
        request: OpenRequest,
    ) -> Result<OwnerSessionOpenReceipt, TransportRefusal> {
        let (frame, result) = self.prepare_open(request, OpenReply::Direct);
        self.transport.enqueue_control(run, frame)?;
        Ok(result)
    }

    fn prepare_open<Outcome>(
        &self,
        request: OpenRequest,
        reply: impl FnOnce(oneshot::Sender<Outcome>) -> OpenReply,
    ) -> (AcpFrame, oneshot::Receiver<Outcome>) {
        let (respond, result) = oneshot::channel();
        #[cfg(feature = "test-observation")]
        let created_handoff_fault = if matches!(&request, OpenRequest::New(_)) {
            self.fixture_created_handoff
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        } else {
            None
        };
        (
            AcpFrame::Open(OpenFrame {
                request,
                respond: reply(respond),
                #[cfg(feature = "test-observation")]
                created_handoff_fault,
            }),
            result,
        )
    }

    pub fn activate_route(&self, run: TransportRun) -> bool {
        self.transport.activate_route(run)
    }

    pub(crate) fn writer_has_stopped(&self) -> bool {
        self.transport.writer_has_stopped()
    }

    #[must_use]
    pub fn current_turn(&self, run: TransportRun) -> Option<TurnId> {
        self.transport.current_turn(run)
    }

    pub(crate) fn provider_started_turn(&self, run: TransportRun) -> Option<TurnId> {
        self.transport.provider_started_turn(run)
    }

    pub fn reopen_route(&self, run: TransportRun, attempt: CloseAttempt) {
        self.transport.reopen_route(run, attempt);
    }

    pub fn apply_membership(&self, owner: OwnerIncarnation, change: TurnMembership) {
        let _route_edit = matches!(change, TurnMembership::Retire(_)).then(|| {
            self.route_edits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        self.transport.apply_membership(owner, change);
        if let TurnMembership::End { run, turn } = change {
            self.cursor_plans.forget_turn(
                TransportRun {
                    incarnation: owner,
                    generation: run,
                },
                turn,
            );
        }
        if let TurnMembership::Retire(generation) = change {
            let run = TransportRun {
                incarnation: owner,
                generation,
            };
            self.restore_updates.retire(run);
            self.clear_run_resources(run);
        }
    }

    fn clear_run_resources(&self, run: TransportRun) {
        self.cursor_plans.forget_run(run);
        self.readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&run);
    }

    pub(crate) fn cursor_plan_items(
        &self,
    ) -> Arc<crate::provider_cursor_plan_items::CursorPlanItems> {
        self.cursor_plans.clone()
    }

    pub(crate) fn register_folders(
        &self,
        run: TransportRun,
        grant: crate::in_folder_allowance::InFolderGrant,
    ) {
        self.transport.install_folders(run, grant);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn router_auto_grant_for_test(&self, run: TransportRun) -> Option<bool> {
        self.transport
            .router_auto_grant_for_test(run, self.behavior.auto_permission_mode())
    }

    pub(crate) fn select_in_folder_permission(
        &self,
        session: &str,
        tool: &str,
        scope: (TransportRun, TurnId),
        permission: crate::in_folder_allowance::InFolderPermission,
    ) -> Option<agent_client_protocol::schema::v1::PermissionOptionId> {
        self.transport.select_in_folder_permission(
            session,
            tool,
            scope,
            permission,
            self.behavior.auto_permission_mode(),
        )
    }

    pub(crate) fn register_reader(&self, run: TransportRun) -> mpsc::Receiver<PromptTerminal> {
        let (send, receive) = mpsc::channel(1);
        self.readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(run, send);
        receive
    }

    pub fn observe_tool(
        &self,
        run: TransportRun,
        tool_id: &str,
        observation: ToolObservation,
    ) -> ToolOwnershipEffect {
        self.transport.observe_tool(run, tool_id, observation)
    }

    /// The last settings accepted by the owner for this route.
    pub fn session_catalog(
        &self,
        run: TransportRun,
    ) -> Option<provider_session_machines::ProviderSettingsCatalog> {
        self.transport.catalogue(run)
    }

    pub fn publish_run_settings(
        &self,
        run: TransportRun,
        settings: provider_session_machines::session_owner::AcceptedRunSettings,
    ) {
        self.transport.accept_settings(run, settings);
    }

    pub(crate) fn current_scope(&self, session: &str) -> Option<(TransportRun, TurnId)> {
        self.transport.current_scope(session)
    }

    pub fn resolve_callback(&self, tool_id: &str, kind: CallbackKind) -> CallbackResolution {
        self.transport.resolve_callback(tool_id, kind)
    }

    pub(crate) fn route_for_session(&self, session: &str) -> Option<TransportRun> {
        self.transport.route_for_session(session)
    }

    pub(crate) fn session_for_route(&self, run: TransportRun) -> Option<String> {
        self.transport.session_for_route(run)
    }

    pub(crate) fn register_owner_events(
        &self,
        owner: OwnerIncarnation,
        events: &Arc<AcpOwnerEvents>,
    ) {
        self.owner_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(owner, Arc::downgrade(events));
    }

    pub(crate) fn unregister_owner_events(
        &self,
        owner: OwnerIncarnation,
        events: &Arc<AcpOwnerEvents>,
    ) {
        let mut registered = self
            .owner_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registered
            .get(&owner)
            .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(events)))
        {
            registered.remove(&owner);
        }
    }

    pub(crate) fn auth_observation_targets(&self) -> (Vec<Arc<AcpOwnerEvents>>, Vec<String>) {
        let routes = self.transport.routed_owner_sessions();
        let registered = self
            .owner_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner_ports = registered.values().filter_map(Weak::upgrade).collect();
        let unbound_sessions = routes
            .into_iter()
            .filter_map(|(owner, session)| (!registered.contains_key(&owner)).then_some(session))
            .collect();
        (owner_ports, unbound_sessions)
    }

    pub(crate) fn allows_explicit_tool_request(&self, session: &str, tool: &str) -> bool {
        self.transport.allows_explicit_tool_request(session, tool)
    }

    pub fn reapply_setting(
        &self,
        owner: OwnerIncarnation,
        run: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        plan: OwnerSettingWirePlan,
        completion: OwnerCompletion,
    ) -> Result<(), TransportRefusal> {
        self.reapply_lifecycle_setting(
            owner,
            run,
            setting,
            plan,
            completion.map(owner_event_for_restore_setting),
        )
    }

    pub(crate) fn reapply_lifecycle_setting(
        &self,
        owner: OwnerIncarnation,
        run: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        plan: OwnerSettingWirePlan,
        completion: LifecycleReply<SettingFact>,
    ) -> Result<(), TransportRefusal> {
        self.transport.enqueue_request(
            TransportRun {
                incarnation: owner,
                generation: run.generation,
            },
            restore_setting_frame(run, setting, plan, completion),
        )
    }

    pub(crate) fn reapply_exact_setting(
        &self,
        route: &crate::acp_transport::LifecycleRoute,
        run: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        plan: OwnerSettingWirePlan,
        claim: &crate::owner_opening_instance::OpeningOffer,
        completion: LifecycleReply<SettingFact>,
    ) -> Result<(), TransportRefusal> {
        self.transport.enqueue_lifecycle_request(
            route,
            claim,
            restore_setting_frame(run, setting, plan, completion),
        )
    }
}

fn restore_setting_frame(
    run: RunAttempt<LoadAttempt>,
    setting: OwnerSetting,
    plan: OwnerSettingWirePlan,
    completion: LifecycleReply<SettingFact>,
) -> AcpFrame {
    let correlation = match setting.kind {
        provider_session_machines::ProviderSettingKind::Fast => {
            SettingCorrelation::RestoreFastOff {
                run,
                setting,
                completion,
            }
        }
        _ => SettingCorrelation::RestoreMode {
            run,
            setting,
            completion,
        },
    };
    let (message, setting) = plan.correlate(correlation);
    AcpFrame::Request {
        message,
        correlation: ResponseKind::Setting(Box::new(setting)),
    }
}

impl OwnerAcpWire {
    pub(crate) fn take(
        &self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TransportRefusal> {
        self.hand_over_work(authorization, || true)
            .map_err(TransportRefusal::from)
    }

    pub(crate) fn hand_over_work(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: impl FnOnce() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        let result = self
            .transport
            .hand_over_work(authorization, still_requested)?;
        if matches!(result, DispatchResult::Taken(_))
            && authorization.target
                == provider_session_machines::session_owner::AuthorizationTarget::NewTurn
        {
            crate::provider_transport_diagnostics::record_prompt_enqueued();
        }
        Ok(result)
    }

    pub(crate) fn revoke(
        &self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TransportRefusal> {
        self.transport.revoke(authorization)
    }

    pub fn revoke_incarnation(
        &self,
        owner: OwnerIncarnation,
    ) -> Vec<(DispatchAuthorization, SlotEvidence)> {
        self.transport.revoke_incarnation(owner)
    }

    pub fn forget(&self, authorization: &DispatchAuthorization) {
        self.transport.forget(authorization);
    }

    pub fn abandon_incarnation(
        &self,
        owner: OwnerIncarnation,
        asked_inputs: &BTreeSet<InputId>,
    ) -> OwnerEvidence {
        let evidence = self.transport.give_up_owner(owner, asked_inputs);
        for generation in self.transport.runs_of(owner) {
            self.apply_membership(owner, TurnMembership::Retire(generation));
        }
        evidence
    }

    pub fn enqueue_close(
        &self,
        owner: OwnerIncarnation,
        run: RunAttempt<CloseAttempt>,
        session: String,
        completion: OwnerCompletion,
    ) -> Result<(), OwnerWireRefusal> {
        self.enqueue_lifecycle_close(owner, run, session, completion.map(owner_event_for_close))
    }

    pub(crate) fn enqueue_lifecycle_close(
        &self,
        owner: OwnerIncarnation,
        run: RunAttempt<CloseAttempt>,
        session: String,
        completion: LifecycleReply<CloseFact>,
    ) -> Result<(), OwnerWireRefusal> {
        let frame = AcpFrame::Request {
            message: CloseSessionRequest::new(session)
                .to_untyped_message()
                .map_err(|_| OwnerWireRefusal::Encoding)?,
            correlation: ResponseKind::Close { run, completion },
        };
        Ok(self.transport.enqueue_close(
            TransportRun {
                incarnation: owner,
                generation: run.generation,
            },
            run.attempt,
            frame,
        )?)
    }
}

#[cfg(test)]
#[path = "owner_wire/owner_output_stop_test.rs"]
pub(crate) mod output_stop_tests;
#[cfg(test)]
#[path = "owner_wire_test.rs"]
mod tests;
