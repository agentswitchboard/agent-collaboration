//! ACP frames submitted through the session owner's ordered writer.

pub mod owner_session_open;
pub mod owner_session_setting;
pub mod owner_session_stream;
mod owner_work_offers;

use crate::acp_transport::{
    AcpTransport, CallbackKind, CallbackResolution, OrderedOutput, SlotEvidence, TransportRefusal,
    TransportRun,
};
use crate::owner_session_open::{
    OpenFrame, OpenRequest, OwnerOpenConfiguration, OwnerSessionOpenReceipt, OwnerSessionOpenSetup,
};
use crate::owner_session_setting::{CorrelatedSetting, OwnerSettingWirePlan, SettingCorrelation};
use crate::provider_prompt_result_codec::decode_prompt_result;
use crate::session_owner_task::OwnerCompletion;
use agent_client_protocol::schema::v1::{CancelNotification, CloseSessionRequest, ErrorCode};
use agent_client_protocol::{Agent, ConnectionTo, Error, JsonRpcMessage as _, UntypedMessage};
use provider_session_machines::SessionGeneration;
use provider_session_machines::harness_lifecycle::{CloseFact, LifecycleReply, SettingFact};
use provider_session_machines::session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia};
use provider_session_machines::session_owner::{
    DispatchAuthorization, DispatchResult, OwnerEvent, OwnerIncarnation, OwnerSetting,
    PromptCompletion, RestorePreparation, RunAttempt, SessionWireResult, TurnMembership,
    WireOutcome,
};
use provider_session_machines::session_turn_machine::TurnId;
use provider_session_machines::tool_ownership::{ToolObservation, ToolOwnershipEffect};
use session_event_model::ProviderAuthStatus;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};
use switchboard_harness_contracts::{
    CloseUncertainty, HandOverRefusal, HarnessOwnerEvents, ReservedWorkReply, SteerIdlePolicy,
    WorkReply,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

enum AcpFrame {
    Request {
        message: UntypedMessage,
        correlation: ResponseKind,
    },
    Cancel(CancelNotification),
    Open(OpenFrame),
    #[cfg(feature = "test-observation")]
    FixtureDispatchFence(FixtureDispatchPoint, oneshot::Sender<Result<(), Error>>),
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
    readers: Mutex<BTreeMap<TransportRun, mpsc::Sender<PromptTerminal>>>,
    restore_updates: Arc<crate::owner_restore_updates::RestoreUpdates>,
    cursor_plans: Arc<crate::provider_cursor_plan_items::CursorPlanItems>,
    behavior: Arc<dyn crate::ProviderBehavior>,
    owner_events: Mutex<BTreeMap<OwnerIncarnation, Weak<AcpOwnerEvents>>>,
    #[cfg(feature = "test-observation")]
    fixture_dispatch: Mutex<FixtureDispatchObservation>,
}

pub(crate) struct PromptTerminal {
    pub(crate) turn: TurnId,
    pub(crate) compact: bool,
    authorization: DispatchAuthorization,
    reply: Option<Result<serde_json::Value, Error>>,
    completion: ReservedWorkReply,
}

impl PromptTerminal {
    fn route(self, reader: Option<mpsc::Sender<Self>>, connection_lost: bool) {
        if connection_lost {
            self.completion.send(WorkReply::ConnectionLost);
        } else if let Some(reader) = reader {
            if let Err(undelivered) = reader.try_send(self) {
                undelivered.into_inner().finish(String::new(), None);
            }
        } else {
            self.finish(String::new(), None);
        }
    }

    pub(crate) fn stop_reason(&self) -> Option<session_event_model::StopReason> {
        decode_prompt_result(self.reply.as_ref()?.as_ref().ok()?).ok()
    }

    pub(crate) fn finish(
        self,
        response: String,
        local_cause: Option<session_event_model::LocalCause>,
    ) {
        if let Some(Err(error)) = &self.reply
            && let Some(failure) =
                crate::provider_acp_error_mapping::acp_operation_error(error.clone())
                    .coded_wire_failure()
        {
            self.completion
                .send(WorkReply::Dispatch(DispatchResult::PromptFailed {
                    authorization: self.authorization,
                    failure,
                }));
            return;
        }
        let outcome = match self.reply {
            Some(Ok(reply)) => {
                decode_prompt_result(&reply).map_or(WireOutcome::Unknown, |stop_reason| {
                    WireOutcome::Confirmed(PromptCompletion {
                        stop_reason,
                        local_cause,
                        response,
                    })
                })
            }
            _ => WireOutcome::Unknown,
        };
        self.completion
            .send(WorkReply::Dispatch(DispatchResult::PromptFinished {
                authorization: self.authorization,
                outcome,
            }));
    }
}

pub(crate) struct OwnerAcpOutput {
    output: mpsc::Receiver<OrderedOutput<AcpFrame>>,
    restore_updates: Arc<crate::owner_restore_updates::RestoreUpdates>,
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

    pub(crate) fn new(
        capacity: NonZeroUsize,
        behavior: Arc<dyn crate::ProviderBehavior>,
    ) -> (Self, OwnerAcpOutput) {
        let (transport, output) = AcpTransport::new(capacity);
        let restore_updates = Arc::new(crate::owner_restore_updates::RestoreUpdates::default());
        (
            Self {
                transport,
                readers: Mutex::default(),
                restore_updates: restore_updates.clone(),
                cursor_plans: Arc::default(),
                behavior,
                owner_events: Mutex::default(),
                #[cfg(feature = "test-observation")]
                fixture_dispatch: Mutex::default(),
            },
            OwnerAcpOutput {
                output,
                restore_updates,
            },
        )
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
        let updates = self.restore_updates.open(run, session.clone());
        let reply = self.enqueue_open(
            run,
            OpenRequest::Restore {
                provider_id: session,
                attempt,
                updates,
                via,
                setup,
            },
        );
        if reply.is_err() {
            self.restore_updates.retire(run);
            self.restore_updates.reply_settled(run);
        }
        reply
    }

    fn enqueue_open(
        &self,
        run: TransportRun,
        request: OpenRequest,
    ) -> Result<OwnerSessionOpenReceipt, TransportRefusal> {
        let (respond, result) = oneshot::channel();
        self.transport
            .enqueue_control(run, AcpFrame::Open(OpenFrame { request, respond }))?;
        Ok(result)
    }

    pub fn activate_route(&self, run: TransportRun) {
        self.transport.activate_route(run);
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
            self.cursor_plans.forget_run(run);
            self.readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&run);
        }
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

    pub(crate) fn forward_restored_update(
        &self,
        session: &str,
        message: agent_client_protocol::Dispatch,
    ) -> Option<agent_client_protocol::Dispatch> {
        self.restore_updates
            .forward(session, self.route_for_session(session), message)
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
        let catalogue = |record| {
            Arc::new(provider_session_machines::ProviderSettingsCatalog::from_record(record))
        };
        self.reapply_lifecycle_setting(
            owner,
            run,
            setting,
            plan,
            completion.map(move |fact| match fact {
                SettingFact::Mode {
                    restore: run,
                    setting,
                    outcome,
                    catalogue: record,
                } => OwnerEvent::Wire(SessionWireResult::RestoreMode {
                    run,
                    setting,
                    outcome,
                    catalogue: record.map(catalogue),
                }),
                SettingFact::FastOff {
                    restore: run,
                    setting,
                    observation,
                    catalogue: record,
                } => OwnerEvent::Prepare(RestorePreparation::FastOff {
                    run,
                    setting,
                    observation,
                    catalogue: record.map(catalogue),
                }),
            }),
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
        self.transport.enqueue_request(
            TransportRun {
                incarnation: owner,
                generation: run.generation,
            },
            AcpFrame::Request {
                message,
                correlation: ResponseKind::Setting(Box::new(setting)),
            },
        )
    }

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
    ) -> Vec<(DispatchAuthorization, SlotEvidence)> {
        let evidence = self.revoke_incarnation(owner);
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
        self.enqueue_lifecycle_close(
            owner,
            run,
            session,
            completion.map(|fact| {
                let (run, outcome) = match fact {
                    CloseFact::Closed { run } => (run, WireOutcome::Confirmed(())),
                    CloseFact::RefusedStillLive { run } => (run, WireOutcome::Refused),
                    CloseFact::NotOffered { run } => {
                        return OwnerEvent::Wire(SessionWireResult::CloseNotSent(run));
                    }
                    CloseFact::Unknown { run, .. } => (run, WireOutcome::Unknown),
                };
                OwnerEvent::Wire(SessionWireResult::Close { run, outcome })
            }),
        )
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

impl OwnerAcpOutput {
    pub(crate) async fn run(
        mut self,
        connection: ConnectionTo<Agent>,
        configuration: OwnerOpenConfiguration,
        shutdown: CancellationToken,
        input_ended: CancellationToken,
    ) {
        let shutdown = shutdown.child_token();
        let replies = TaskTracker::new();
        loop {
            let next = tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                next = self.output.recv() => next,
            };
            let Some(OrderedOutput { frame, run }) = next else {
                break;
            };
            match frame {
                AcpFrame::Open(open) => open.start(
                    run,
                    &connection,
                    &configuration,
                    &replies,
                    shutdown.clone(),
                    self.restore_updates.clone(),
                ),
                AcpFrame::Cancel(notification) => {
                    if connection.send_notification(notification).is_err() {
                        tracing::debug!("ACP cancel could not leave the ordered writer");
                    }
                }
                #[cfg(feature = "test-observation")]
                AcpFrame::FixtureDispatchFence(point, complete) => {
                    let after = match point {
                        FixtureDispatchPoint::Cancel => "session/cancel",
                        FixtureDispatchPoint::Restore => "session/load",
                    };
                    let result = UntypedMessage::new(
                        "fixture/orderedDispatchFence",
                        serde_json::json!({"after":after}),
                    )
                    .and_then(|fence| connection.send_notification(fence));
                    let _fixture_gone = complete.send(result);
                }
                AcpFrame::Request {
                    message,
                    correlation,
                } => {
                    let behavior = configuration.behavior.clone();
                    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
                    let witness = match &correlation {
                        ResponseKind::Prompt { authorization, .. }
                        | ResponseKind::Steer { authorization, .. } => {
                            Some((authorization, message.method.clone()))
                        }
                        ResponseKind::Close { .. } | ResponseKind::Setting(_) => None,
                    };
                    let request = connection.send_request(message);
                    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
                    if let Some((authorization, method)) = witness {
                        crate::work_call_observation::record_frame_id(
                            authorization,
                            &method,
                            request.id(),
                        );
                    }
                    let request = request.block_task();
                    let shutdown = shutdown.clone();
                    let input_ended = input_ended.clone();
                    let connection = connection.clone();
                    replies.spawn(async move {
                        let (response, connection_lost) = ResponseKind::read_reply(
                            matches!(&correlation, ResponseKind::Prompt { .. }),
                            request,
                            &shutdown,
                            |error| {
                                connection.is_incoming_closed()
                                    || input_ended.is_cancelled()
                                    || agent_client_protocol::is_incoming_transport_closed(error)
                            },
                        )
                        .await;
                        correlation.complete(response, connection_lost, behavior.as_ref());
                    });
                }
            }
        }
        shutdown.cancel();
        replies.close();
        replies.wait().await;
    }
}

impl ResponseKind {
    async fn read_reply(
        prompt_pending: bool,
        request: impl std::future::Future<Output = Result<serde_json::Value, Error>>,
        shutdown: &CancellationToken,
        incoming_lost: impl FnOnce(&Error) -> bool,
    ) -> (Option<Result<serde_json::Value, Error>>, bool) {
        let response = tokio::select! {
            biased;
            reply = request => Some(reply),
            () = shutdown.cancelled() => None,
        };
        let connection_lost = response.as_ref().is_none_or(|reply| {
            reply.as_ref().is_err_and(|error| {
                // The SDK can wake a prompt with an unmarked internal error
                // after the process exits, before incoming EOF is observed.
                (prompt_pending && shutdown.is_cancelled()) || incoming_lost(error)
            })
        });
        (response, connection_lost)
    }

    fn complete(
        self,
        reply: Option<Result<serde_json::Value, Error>>,
        connection_lost: bool,
        behavior: &dyn crate::ProviderBehavior,
    ) {
        match self {
            Self::Prompt {
                authorization,
                turn,
                compact,
                reader,
                completion,
            } => PromptTerminal {
                turn,
                compact,
                authorization,
                reply,
                completion,
            }
            .route(reader, connection_lost),
            Self::Setting(setting) => {
                setting.settle(if connection_lost { None } else { reply }, behavior);
            }
            Self::Steer {
                authorization,
                idle,
                completion,
            } => {
                completion.send(WorkReply::Dispatch(settle_steer_reply(
                    authorization,
                    idle,
                    reply,
                )));
            }
            Self::Close { run, completion } => {
                let fact = match reply {
                    Some(Ok(_)) => CloseFact::Closed { run },
                    Some(Err(error)) if definite_refusal(&error) => {
                        CloseFact::RefusedStillLive { run }
                    }
                    Some(Err(_)) => CloseFact::Unknown {
                        run,
                        cause: CloseUncertainty::EffectUnconfirmed,
                    },
                    None => CloseFact::Unknown {
                        run,
                        cause: CloseUncertainty::ReplyLost,
                    },
                };
                completion.send(fact);
            }
        }
    }
}

fn settle_steer_reply(
    authorization: DispatchAuthorization,
    idle: SteerIdlePolicy,
    reply: Option<Result<serde_json::Value, Error>>,
) -> DispatchResult {
    match reply {
        Some(Ok(reply)) => match reply.get("outcome").and_then(serde_json::Value::as_str) {
            Some("startedNewTurn") => DispatchResult::SteerStartedNewTurn {
                authorization,
                turn: TurnId(Uuid::now_v7()),
            },
            Some("promptRequired") => match idle {
                SteerIdlePolicy::AllowOwnerPrompt => DispatchResult::PromptRequired {
                    authorization,
                    replacement_turn: TurnId(Uuid::now_v7()),
                },
                SteerIdlePolicy::Refuse => DispatchResult::SteerNeedsPrompt(authorization),
            },
            Some("injected") => DispatchResult::SteerFinished {
                authorization,
                outcome: WireOutcome::Confirmed(()),
            },
            Some("failed") => DispatchResult::SteerFinished {
                authorization,
                outcome: WireOutcome::Refused,
            },
            _ => DispatchResult::SteerInvalidReply(authorization),
        },
        Some(Err(error)) if definite_refusal(&error) => DispatchResult::SteerFinished {
            authorization,
            outcome: WireOutcome::Refused,
        },
        _ => DispatchResult::SteerFinished {
            authorization,
            outcome: WireOutcome::Unknown,
        },
    }
}

#[cfg(test)]
#[path = "owner_wire_test.rs"]
mod tests;

pub(crate) fn definite_refusal(error: &Error) -> bool {
    !agent_client_protocol::is_incoming_transport_closed(error)
        && matches!(
            error.code,
            ErrorCode::MethodNotFound
                | ErrorCode::InvalidParams
                | ErrorCode::AuthRequired
                | ErrorCode::ResourceNotFound
        )
}
