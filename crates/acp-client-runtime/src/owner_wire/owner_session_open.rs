//! Start SDK routing as an ordered connection effect.
use super::writer_reply_signals::WriterReplySignals;

use crate::{
    ExternalProviderRuntimeError, ProviderBehavior, ProviderCapabilityReport,
    owner_session_stream::{OwnerStreamTarget, ProjectionBuffer},
    provider_item_projection::ProviderItemProjection,
};
use agent_client_protocol::schema::v1::{
    LoadSessionRequest, McpServer, NewSessionRequest, ResumeSessionRequest, SessionId,
};
use agent_client_protocol::{ActiveSession, Agent, ConnectionTo, Dispatch};
use futures_util::FutureExt as _;
use provider_session_machines::session_lifecycle_machine::{LoadAttempt, RestoreVia};
use provider_session_machines::{ModelReasoningMetadata, ProviderSettingsCatalog};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::TaskTracker;

pub use switchboard_harness_contracts::OpenSetup as OwnerSessionOpenSetup;

pub type OwnerSessionOpenReceipt =
    oneshot::Receiver<Result<OwnerOpenedSession, ExternalProviderRuntimeError>>;

pub(crate) type LifecycleOpenReceipt =
    oneshot::Receiver<Result<OwnerOpenedSession, OpenReceiptFailure>>;

pub(crate) enum OpenReceiptFailure {
    Provider(ExternalProviderRuntimeError),
    OpenNotSent,
}

pub(crate) enum OpenReply {
    Lifecycle(oneshot::Sender<Result<OwnerOpenedSession, OpenReceiptFailure>>),
    Direct(oneshot::Sender<Result<OwnerOpenedSession, ExternalProviderRuntimeError>>),
}

impl OpenReply {
    fn deliver(self, outcome: Result<OwnerOpenedSession, ExternalProviderRuntimeError>) -> bool {
        match self {
            Self::Lifecycle(reply) => reply
                .send(outcome.map_err(OpenReceiptFailure::Provider))
                .is_ok(),
            Self::Direct(reply) => reply.send(outcome).is_ok(),
        }
    }

    pub(super) fn unsent(self) {
        match self {
            Self::Lifecycle(reply) => {
                let _owner_retired = reply.send(Err(OpenReceiptFailure::OpenNotSent));
            }
            Self::Direct(reply) => {
                let _owner_retired =
                    reply.send(Err(ExternalProviderRuntimeError::TransportFailure));
            }
        }
    }
}

pub struct OwnerOpenedSession {
    pub(crate) provider_id: SessionId,
    pub(crate) connection: ConnectionTo<Agent>,
    pub(crate) route: Option<OpenedSessionRoute>,
    pub(crate) capabilities: ProviderCapabilityReport,
    pub(crate) catalog: ProviderSettingsCatalog,
    pub(crate) behavior: Arc<dyn ProviderBehavior>,
    pub(crate) buffer: Arc<ProjectionBuffer>,
    pub(crate) projection: Option<ProviderItemProjection>,
    pub(crate) in_folder: Option<crate::in_folder_allowance::InFolderGrant>,
    #[cfg(any(test, feature = "test-observation"))]
    pub(crate) observed_tool_calls: crate::owner_session_stream::TestToolCallRecords,
    #[cfg(any(test, feature = "test-observation"))]
    pub(crate) opening_disposal: Option<crate::owner_opening_disposal::OpeningDisposalToken>,
}

pub(crate) enum OpenedSessionRoute {
    Created(oneshot::Sender<CreatedStream>),
    Restored {
        sdk: Box<ActiveSession<'static, Agent>>,
        updates: mpsc::UnboundedReceiver<Dispatch>,
    },
}

pub(crate) struct CreatedStream {
    pub(crate) opened: OwnerOpenedSession,
    pub(crate) target: OwnerStreamTarget,
    pub(crate) start: oneshot::Receiver<()>,
    pub(crate) finished: oneshot::Sender<Result<(), ExternalProviderRuntimeError>>,
}

impl OwnerOpenedSession {
    fn new(
        sdk: &ActiveSession<'_, Agent>,
        route: Option<OpenedSessionRoute>,
        config: &OwnerOpenConfiguration,
        in_folder: Option<crate::in_folder_allowance::InFolderGrant>,
        auth_status: session_event_model::ProviderAuthStatus,
    ) -> Self {
        let response = sdk.response();
        let catalog = crate::provider_settings_catalog_codec::catalog_from_session_response(
            &response,
            config.behavior.as_ref(),
            &config.reasoning,
        );
        Self {
            provider_id: sdk.session_id().clone(),
            connection: sdk.connection().clone(),
            route,
            capabilities: config
                .capabilities
                .with_session_response(&response)
                .with_auth_status(auth_status),
            catalog,
            behavior: Arc::clone(&config.behavior),
            buffer: Arc::default(),
            projection: None,
            in_folder,
            #[cfg(any(test, feature = "test-observation"))]
            observed_tool_calls: config.observed_tool_calls.clone(),
            #[cfg(any(test, feature = "test-observation"))]
            opening_disposal: None,
        }
    }

    #[must_use]
    pub fn provider_id(&self) -> String {
        self.provider_id.to_string()
    }

    pub fn register_in_folder_scope(
        &mut self,
        wire: &crate::owner_wire::OwnerAcpWire,
        run: crate::acp_transport::TransportRun,
    ) {
        if let Some(grant) = self.in_folder.take() {
            wire.register_folders(run, grant);
        }
    }

    pub(crate) fn register_in_folder_opening(
        &mut self,
        wire: &crate::owner_wire::OwnerAcpWire,
        route: &crate::acp_transport::LifecycleRoute,
    ) {
        if let Some(grant) = self.in_folder.take() {
            wire.register_opening_folders(route, grant);
        }
    }
}

#[derive(Clone)]
pub(crate) struct OwnerOpenConfiguration {
    pub(crate) behavior: Arc<dyn ProviderBehavior>,
    pub(crate) capabilities: ProviderCapabilityReport,
    pub(crate) mcp_servers: Vec<McpServer>,
    pub(crate) reasoning: ModelReasoningMetadata,
    pub(crate) auth_status: Arc<tokio::sync::RwLock<session_event_model::ProviderAuthStatus>>,
    #[cfg(any(test, feature = "test-observation"))]
    pub(crate) observed_tool_calls: crate::owner_session_stream::TestToolCallRecords,
    #[cfg(feature = "test-observation")]
    pub(crate) reply_test_wire: Option<Arc<crate::owner_wire::OwnerAcpWire>>,
}

pub(crate) enum OpenRequest {
    New(OwnerSessionOpenSetup),
    Restore {
        provider_id: String,
        attempt: LoadAttempt,
        tap: crate::owner_opening_instance::OpeningInstance,
        updates: mpsc::UnboundedReceiver<Dispatch>,
        via: RestoreVia,
        setup: OwnerSessionOpenSetup,
    },
}

pub(crate) struct OpenFrame {
    pub(crate) request: OpenRequest,
    pub(crate) respond: OpenReply,
    #[cfg(feature = "test-observation")]
    pub(crate) created_handoff_fault: Option<CreatedHandoffFault>,
}

#[cfg(feature = "test-observation")]
pub(crate) struct CreatedHandoffFault {
    pub(crate) close_requested: oneshot::Receiver<()>,
    pub(crate) closed: oneshot::Sender<()>,
}

impl OpenFrame {
    pub(super) fn start(
        self,
        run: crate::acp_transport::TransportRun,
        connection: &ConnectionTo<Agent>,
        config: &OwnerOpenConfiguration,
        tasks: &TaskTracker,
        signals: WriterReplySignals,
        restore_updates: Arc<crate::owner_restore_updates::RestoreUpdates>,
    ) {
        use crate::provider_acp_error_mapping::{acp_load_session_error, acp_operation_error};

        #[cfg(feature = "test-observation")]
        let reply_test_wire = config.reply_test_wire.clone();
        match self.request {
            OpenRequest::New(setup) => {
                #[cfg(feature = "test-observation")]
                let created_handoff_fault = self.created_handoff_fault;
                let folders = crate::session_start_folders::session_start_folders(
                    &setup.cwd,
                    setup.scratch_folder.as_deref(),
                    &setup.work_roots,
                    config.behavior.as_ref(),
                    config.capabilities.supports_additional_directories,
                );
                let mut request = NewSessionRequest::new(&setup.cwd)
                    .mcp_servers(config.mcp_servers.clone())
                    .additional_directories(folders.additional_directories);
                request.meta = folders.meta;
                let builder = connection.build_session_from(request).block_task();
                let reply_connection = connection.clone();
                let reply_signals = signals.clone();
                let config = config.clone();
                let start = async move {
                    let mut respond = Some(self.respond);
                    // Keep the SDK route inside its scope and return the
                    // original handshake failure through the same receipt.
                    let result = builder
                        .run_until(async |mut sdk| {
                            let (activate, activation) = oneshot::channel();
                            let auth_status = config.auth_status.read().await.clone();
                            let opened = OwnerOpenedSession::new(
                                &sdk,
                                Some(OpenedSessionRoute::Created(activate)),
                                &config,
                                folders.in_folder,
                                auth_status,
                            );
                            if !respond
                                .take()
                                .is_some_and(|reply| reply.deliver(Ok(opened)))
                            {
                                return Ok(());
                            }
                            if let Some(CreatedStream {
                                opened,
                                target,
                                start,
                                finished,
                            }) = receive_created_handoff(
                                activation,
                                #[cfg(feature = "test-observation")]
                                created_handoff_fault,
                            )
                            .await
                                && start.await.is_ok()
                            {
                                let _owner_retired =
                                    finished.send(opened.read_sdk_stream(&mut sdk, target).await);
                            }
                            Ok(())
                        })
                        .await;
                    if let (Some(respond), Err(error)) = (respond, result)
                        && !reply_signals.reply_lost(&reply_connection, &error)
                    {
                        respond.deliver(Err(acp_operation_error(error)));
                    }
                };
                let mut start = Box::pin(start);
                // The first poll enqueues the handshake in writer order.
                if start.as_mut().now_or_never().is_none() {
                    tasks.spawn(async move {
                        #[cfg(feature = "test-observation")]
                        if let Some(wire) = reply_test_wire {
                            wire.before_taken_reply_for_test().await;
                        }
                        tokio::select! {
                            biased;
                            () = start => {}
                            () = signals.shutdown.cancelled() => {}
                        }
                    });
                }
            }
            OpenRequest::Restore {
                provider_id,
                attempt,
                tap,
                updates,
                via,
                setup,
            } => {
                let folders = crate::session_start_folders::session_start_folders(
                    &setup.cwd,
                    setup.scratch_folder.as_deref(),
                    &setup.work_roots,
                    config.behavior.as_ref(),
                    config.capabilities.supports_additional_directories,
                );
                let connection = connection.clone();
                let servers = config.mcp_servers.clone();
                let config = config.clone();
                let reply_signals = signals.clone();
                let start = async move {
                    let requested_session = SessionId::new(provider_id);
                    let sdk = match via {
                        RestoreVia::Load => {
                            let mut request =
                                LoadSessionRequest::new(requested_session.clone(), &setup.cwd)
                                    .mcp_servers(servers)
                                    .additional_directories(folders.additional_directories);
                            request.meta = folders.meta;
                            connection
                                .load_session_from(request)
                                .block_task()
                                .start_session()
                                .await
                                .map(|restored| restored.into_session())
                        }
                        RestoreVia::Resume => {
                            let mut request =
                                ResumeSessionRequest::new(requested_session.clone(), &setup.cwd)
                                    .mcp_servers(servers)
                                    .additional_directories(folders.additional_directories);
                            request.meta = folders.meta;
                            connection
                                .resume_session_from(request)
                                .block_task()
                                .start_session()
                                .await
                                .map(|restored| restored.into_session())
                        }
                    }
                    .map_err(|error| {
                        if reply_signals.reply_lost(&connection, &error) {
                            ExternalProviderRuntimeError::RestoreDisconnected
                        } else {
                            acp_load_session_error(
                                error,
                                &requested_session.0,
                                config.behavior.as_ref(),
                            )
                        }
                    })?;
                    let auth_status = config.auth_status.read().await.clone();
                    let mut opened = OwnerOpenedSession::new(
                        &sdk,
                        None,
                        &config,
                        folders.in_folder,
                        auth_status,
                    );
                    opened.route = Some(OpenedSessionRoute::Restored {
                        sdk: Box::new(sdk),
                        updates,
                    });
                    Ok(opened)
                };
                let mut start = Box::pin(start);
                // The first poll registers the provisional SDK route and
                // enqueues load/resume before the next writer entry.
                if let Some(ready) = start.as_mut().now_or_never() {
                    self.respond.deliver(ready);
                    restore_updates.reply_settled(run, &tap);
                } else {
                    tasks.spawn(async move {
                        #[cfg(feature = "test-observation")]
                        let start = {
                            let mut buffered = None;
                            if let Some(wire) = reply_test_wire {
                                if wire.take_restore_success_buffer_for_test() {
                                    // The SDK's ordered restore blocks later
                                    // peer requests until its real result is
                                    // consumed and the new route attached.
                                    let result = start.as_mut().await;
                                    assert!(result.is_ok(), "expected the real SDK restore success");
                                    buffered = Some(result);
                                }
                                wire.before_taken_reply_for_test().await;
                            }
                            async move {
                                match buffered {
                                    Some(result) => result,
                                    None => start.await,
                                }
                            }
                        };
                        // Keep this SDK request and its replay tap correlated until
                        // the wire answers. A retired owner's receiver has no successor.
                        let result = tokio::select! {
                            biased;
                            result = start => result,
                            () = signals.shutdown.cancelled() => Err(ExternalProviderRuntimeError::RestoreDisconnected),
                        };
                        if !self.respond.deliver(result) {
                            tracing::debug!(?run, ?attempt, "settled retired ACP restore");
                        }
                        restore_updates.reply_settled(run, &tap);
                    });
                }
            }
        }
    }
}

async fn receive_created_handoff(
    activation: oneshot::Receiver<CreatedStream>,
    #[cfg(feature = "test-observation")] fault: Option<CreatedHandoffFault>,
) -> Option<CreatedStream> {
    #[cfg(feature = "test-observation")]
    if let Some(fault) = fault {
        let mut activation = activation;
        return tokio::select! {
            result = &mut activation => result.ok(),
            Ok(()) = fault.close_requested => {
                drop(activation);
                let _fixture_gone = fault.closed.send(());
                None
            }
        };
    }
    activation.await.ok()
}
