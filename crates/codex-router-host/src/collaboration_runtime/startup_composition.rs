//! Bind listeners, prepare service owners and start the Host runtime.
use super::message_startup::{HostCustodyEvents, HostMessageStartup};
use super::{
    BackendPublication, BoundCollaborationApi, CancellationToken, CollaborationRuntime,
    CollaborationRuntimeInputs, EndpointId, EndpointRef, HostCollaborationInputs, JoinSet,
    NativeRelayListener, NonEmptyText, RemoteControlServerName, RouterExecutableRelation,
    SchemaDigest, SocketAddr, create_private_socket_directory, io, load_installation_peer,
    load_service_identity, new_service_uuid,
};
use collaboration_service::collaboration_application::message_operations::MessageOperations;
use std::sync::Arc;
use switchboard_message_custody::recipient_sessions::{
    CombinedRecipientSessionDirectory, RecipientSessionSources,
};
use switchboard_message_system::MessageSystemEvents;

#[cfg(test)]
pub(crate) type BeforeProviderRetention = Box<dyn FnOnce(&CollaborationRuntime) + Send>;

impl CollaborationRuntime {
    /// Binds both private listeners before spawning tasks. No native process is started.
    pub async fn start(inputs: CollaborationRuntimeInputs) -> io::Result<Self> {
        Self::start_with_external_providers(inputs, Vec::new()).await
    }

    pub async fn start_with_external_providers(
        inputs: CollaborationRuntimeInputs,
        provider_launches: Vec<crate::ExternalProviderStartup>,
    ) -> io::Result<Self> {
        Box::pin(Self::start_with_optional_router_proxy_endpoint(
            inputs,
            provider_launches,
            std::sync::Arc::new(collaboration_mcp::FixedRouterExecutableRelation(
                RouterExecutableRelation::Match,
            )),
            None,
            None,
            std::sync::Arc::new(router_clock::SystemRuntimeClock::new()),
            None,
        ))
        .await
    }

    pub(crate) async fn start_for_host_with_router_proxy_endpoint(
        host_inputs: HostCollaborationInputs,
        provider_launches: Vec<crate::ExternalProviderStartup>,
        router_executable_check: std::sync::Arc<dyn collaboration_mcp::RouterExecutableCheck>,
        worker_clock: std::sync::Arc<dyn router_clock::RuntimeClock>,
        #[cfg(test)] before_retention: Option<BeforeProviderRetention>,
    ) -> io::Result<Self> {
        let composition_started_at = worker_clock.monotonic_now();
        crate::debug_readiness_timing::record_with_clock(
            "collaborationRuntimeBegin",
            composition_started_at,
            worker_clock.as_ref(),
        );
        let prepared_exposure = crate::tailscale_exposure::PreparedTailscaleExposure::prepare(
            host_inputs.remote_access,
        )
        .await?;
        let mut runtime = Box::pin(Self::start_with_optional_router_proxy_endpoint(
            host_inputs.collaboration_runtime,
            provider_launches,
            router_executable_check,
            Some(host_inputs.router_proxy_endpoint),
            prepared_exposure,
            worker_clock.clone(),
            host_inputs.message_roots,
        ))
        .await?;
        crate::debug_readiness_timing::record_with_clock(
            "collaborationRuntimeReady",
            composition_started_at,
            worker_clock.as_ref(),
        );
        #[cfg(test)]
        if let Some(before_retention) = before_retention {
            before_retention(&runtime);
        }
        let retention_started_at = worker_clock.monotonic_now();
        if let Err(error) = runtime
            .configure_provider_operation_retention(host_inputs.provider_operation_retention_days)
            .await
        {
            let cleanup = runtime.shutdown().await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => io::Error::other(format!("{error}; startup cleanup: {cleanup}")),
            });
        }
        crate::debug_readiness_timing::record_with_clock(
            "providerRetentionConfigured",
            retention_started_at,
            worker_clock.as_ref(),
        );
        Ok(runtime)
    }

    #[cfg(feature = "runtime-test-support")]
    pub async fn start_with_test_clock(
        inputs: CollaborationRuntimeInputs,
        worker_clock: std::sync::Arc<dyn router_clock::RuntimeClock>,
    ) -> io::Result<Self> {
        Self::runtime_test_start_with_provider_clock(inputs, Vec::new(), worker_clock).await
    }

    #[cfg(feature = "runtime-test-support")]
    pub async fn runtime_test_start_with_provider_clock(
        inputs: CollaborationRuntimeInputs,
        providers: Vec<crate::ExternalProviderStartup>,
        worker_clock: std::sync::Arc<dyn router_clock::RuntimeClock>,
    ) -> io::Result<Self> {
        Box::pin(Self::start_with_optional_router_proxy_endpoint(
            inputs,
            providers,
            std::sync::Arc::new(collaboration_mcp::FixedRouterExecutableRelation(
                RouterExecutableRelation::Match,
            )),
            None,
            None,
            worker_clock,
            None,
        ))
        .await
    }

    pub(super) async fn start_with_optional_router_proxy_endpoint(
        inputs: CollaborationRuntimeInputs,
        provider_launches: Vec<crate::ExternalProviderStartup>,
        router_executable_check: std::sync::Arc<dyn collaboration_mcp::RouterExecutableCheck>,
        router_proxy_endpoint: Option<SocketAddr>,
        prepared_exposure: Option<crate::tailscale_exposure::PreparedTailscaleExposure>,
        worker_clock: std::sync::Arc<dyn router_clock::RuntimeClock>,
        message_roots: Option<HostMessageStartup>,
    ) -> io::Result<Self> {
        let mut owner_human_id = inputs.owner_human_id.clone();
        let service_id = load_service_identity(&inputs.directory)?;
        let installation_peer = load_installation_peer(&inputs.directory)?;
        let service_epoch = new_service_uuid()?;
        let machine_identity = collaboration_contracts::MachineIdentity::new(
            service_id.clone(),
            inputs
                .remote_control_server_name
                .as_ref()
                .map(RemoteControlServerName::as_str),
        )
        .map_err(io::Error::other)?;
        let native_digest = if let Some(export) = &inputs.native_schema {
            super::schema_bundle_retention::publish_in_use_schema_bundle(
                &inputs.directory,
                export.bundle(),
            )?;
            let encoded = format!(
                "sha256:{}",
                export
                    .bundle()
                    .digest()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            Some(SchemaDigest::try_from(encoded).map_err(io::Error::other)?)
        } else {
            None
        };
        let native_definitions = inputs.native_schema.as_ref().and_then(|export| {
            collaboration_mcp::NativeSchemaDefinitions::from_bundle(export.bundle())
        });
        let identity = collaboration_contracts::RouterIdentity::new(
            service_id.clone(),
            service_epoch.clone(),
            Some(installation_peer.clone()),
            machine_identity.clone(),
        )
        .map_err(io::Error::other)?;
        let endpoint_directory = collaboration_runtime::EndpointDirectory::new(service_id.clone());
        let provider_store = match switchboard_session_store::ProviderOperationStore::open(
            &inputs.directory.join("provider-operations.sqlite"),
        )
        .await
        {
            Ok(store) => {
                let store = std::sync::Arc::new(tokio::sync::Mutex::new(store));
                Some(store)
            }
            Err(_) => {
                tracing::warn!(
                    "provider operation storage unavailable; external providers remain disabled"
                );
                None
            }
        };
        let proxy_authority = prepared_exposure
            .as_ref()
            .map(|prepared| prepared.authority().clone());
        let collaboration_api =
            BoundCollaborationApi::bind(inputs.mcp_bind, proxy_authority).await?;
        let exposure_backend = collaboration_api.local_address();
        let mcp_url = collaboration_api.url();
        let startup = crate::provider_startup_composition::compose_provider_startup(
            provider_launches,
            provider_store.clone(),
            &service_id,
            &service_epoch,
            &mcp_url,
            inputs
                .directory
                .parent()
                .map(codex_native_integration::router_scratch_root),
            Some(inputs.directory.join("hooks")),
            std::sync::Arc::clone(&worker_clock),
        )
        .await?;
        let provider_face_endpoints = startup.endpoints.clone();
        let provider_retirements = startup.retirements;
        let external_provider_supervisor = startup.supervisor;
        let provider_session_hub = startup.hub;
        let provider_cache_warm = crate::provider_cache_warm::compose_provider_cache_warm(
            startup.cache_warm,
            external_provider_supervisor.as_ref(),
            provider_store.as_ref(),
            worker_clock.clone(),
        );
        let provider_endpoints = startup
            .endpoints
            .iter()
            .map(|description| description.endpoint.clone())
            .collect();
        endpoint_directory.publish_inventory(startup.endpoints)?;
        let provider_conversations = external_provider_supervisor.as_ref().map(|supervisor| {
            supervisor.clone()
                as std::sync::Arc<dyn collaboration_contracts::ProviderConversationBackend>
        });
        let endpoint = EndpointRef {
            service_id: service_id.clone(),
            endpoint_id: EndpointId::try_from("codex-local".to_owned())
                .map_err(io::Error::other)?,
        };
        let publication = BackendPublication::new(
            endpoint_directory.clone(),
            endpoint.clone(),
            service_epoch.clone(),
            inputs.backend_socket,
        )?;
        let native_backend = collaboration_runtime::NativeControlBackend {
            codex_home: inputs.codex_home.clone(),
            endpoint,
            gate: publication.admission_gate(),
        };
        let unmaterialized_threads =
            std::sync::Arc::new(collaboration_runtime::UnmaterializedThreadHolder::new());
        let open_scratch_scopes =
            std::sync::Arc::new(collaboration_runtime::OpenScratchScopes::new());
        let peer_registry_directory = inputs.peer_registry_directory.clone().map_or_else(
            crate::session_message_route_composition::default_peer_registry_directory,
            Ok,
        )?;
        let claude_background_jobs =
            std::sync::Arc::new(claude_code_peer_messaging::NativeClaudeJobReader::default());
        let claude_code_sessions =
            std::sync::Arc::new(claude_code_peer_messaging::ClaudeCodeSessionRegistry::new(
                peer_registry_directory.clone(),
            ));
        let message_routes =
            crate::session_message_route_composition::compose_session_message_routes(
                crate::session_message_route_composition::SessionMessageRouteInputs {
                    clock: worker_clock.clone(),
                    provider_endpoints,
                    directory: endpoint_directory.clone(),
                    native_backend: native_backend.clone(),
                    unmaterialized_threads: std::sync::Arc::clone(&unmaterialized_threads),
                    provider_supervisor: external_provider_supervisor.clone(),
                    provider_store: provider_store.clone(),
                    peer_registry_directory,
                },
            )?;
        let provider_delivery_route = message_routes.provider_route;
        let session_delivery_router = Some(message_routes.router);
        let codex_recorder = crate::codex_conversation_recording_composition::recording_for_store(
            provider_store.as_ref(),
            &inputs.directory,
            &service_id,
        )?;
        let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(32));
        let application = {
            let native_control =
                collaboration_service::collaboration_application::NativeSessionControl::new(
                    identity.clone(),
                    endpoint_directory.clone(),
                    Some(native_backend.clone()),
                    codex_recorder.clone().map(|recorder| {
                        recorder as std::sync::Arc<dyn collaboration_runtime::CodexPermissionModes>
                    }),
                );
            let codex_inventory = collaboration_service::CodexSessionInventory::new(
                identity.clone(),
                endpoint_directory.clone(),
                Some(native_backend.clone()),
            );
            let provider_inventory = collaboration_service::ProviderSessionInventory::new(
                identity.clone(),
                endpoint_directory.clone(),
                provider_store.clone(),
                provider_session_hub
                    .clone()
                    .map(|hub| hub as std::sync::Arc<dyn collaboration_contracts::SessionEventHub>),
                Some(claude_code_sessions.clone()),
                Some(claude_background_jobs.clone()),
            );
            let sessions = collaboration_service::collaboration_application::SessionOperations::new(
                identity.clone(),
                endpoint_directory.clone(),
                native_control,
                codex_inventory,
                provider_inventory,
            );
            let conversations =
                collaboration_service::collaboration_application::ConversationOperations::new(
                    identity.clone(),
                    provider_conversations,
                    provider_store.clone(),
                    codex_recorder.clone(),
                    endpoint_directory.clone(),
                );
            let observation =
                collaboration_service::collaboration_application::ObservationOperations::new(
                    identity.clone(),
                    provider_session_hub.clone().map(|hub| {
                        hub as std::sync::Arc<dyn collaboration_contracts::SessionEventHub>
                    }),
                    endpoint_directory.clone(),
                    worker_clock.clone(),
                );
            collaboration_service::CollaborationApplication::new(
                identity.clone(),
                conversations,
                observation,
                sessions,
            )
        };
        let collaboration_api =
            collaboration_api.with_service_socket(&inputs.directory.join("control.sock"))?;
        let native = NativeRelayListener::bind(
            &inputs.directory.join("codex-native.sock"),
            publication.admission_gate(),
        )?
        .with_connection_budget(std::sync::Arc::clone(&permits));
        let stored = std::sync::Arc::new(codex_acp_adapter::NativeStoredSessions::new(
            inputs.codex_home.clone(),
            String::from(service_id.clone()),
        ));
        let mut acp = collaboration_runtime::AcpChannelListener::bind(
            &inputs.directory.join("codex-acp.sock"),
            service_id.clone(),
            publication.admission_gate(),
            stored,
            unmaterialized_threads.clone(),
            crate::codex_conversation_recording_composition::adapter_recorder(codex_recorder),
            std::sync::Arc::clone(&open_scratch_scopes)
                as std::sync::Arc<dyn codex_acp_adapter::ScratchScopeRegister>,
        )?
        .with_connection_budget(permits);
        let mut provider_app_servers = Vec::new();
        if !provider_face_endpoints.is_empty() && external_provider_supervisor.is_some() {
            owner_human_id =
                crate::owner_identity_resolution::resolve_face_owner_human_id(owner_human_id).await;
        }
        if let (Some(supervisor), Some(delivery), Some(hub), Some(store)) = (
            &external_provider_supervisor,
            &provider_delivery_route,
            &provider_session_hub,
            &provider_store,
        ) {
            let commands: std::sync::Arc<dyn collaboration_contracts::SessionCommandPort> =
                std::sync::Arc::new(crate::HostSessionCommandPort::new(
                    std::sync::Arc::clone(supervisor),
                    std::sync::Arc::clone(delivery),
                    std::sync::Arc::clone(hub),
                    std::sync::Arc::clone(store),
                    std::sync::Arc::clone(&worker_clock),
                ));
            let events: std::sync::Arc<dyn collaboration_contracts::SessionEventHub> =
                std::sync::Arc::clone(hub) as _;
            let socket_directory = inputs.directory.join("router-sessions");
            if owner_human_id.is_some() {
                create_private_socket_directory(&socket_directory)?;
            }
            for description in provider_face_endpoints {
                if supervisor.runtime_for(&description.endpoint).is_none() {
                    continue;
                }
                let endpoint = message_board::SessionEndpointRef {
                    service_id: message_board::ServiceId::try_from(String::from(
                        description.endpoint.service_id.clone(),
                    ))
                    .map_err(io::Error::other)?,
                    endpoint_id: message_board::EndpointId::try_from(String::from(
                        description.endpoint.endpoint_id.clone(),
                    ))
                    .map_err(io::Error::other)?,
                };
                acp = acp.with_provider_session_backend(
                    endpoint.clone(),
                    std::sync::Arc::clone(&commands),
                    std::sync::Arc::clone(&events),
                    std::sync::Arc::clone(&worker_clock),
                );
                let Some(owner_human_id) = owner_human_id.as_ref() else {
                    continue;
                };
                let context = collaboration_runtime::RouterSessionAppServerContext::new(
                    endpoint,
                    message_board::Identity::Human {
                        human_id: owner_human_id.clone(),
                    },
                    std::sync::Arc::clone(&commands),
                    std::sync::Arc::clone(&events),
                    std::sync::Arc::new(
                        crate::provider_model_catalog_adapter::SupervisorModelCatalog::new(
                            std::sync::Arc::clone(supervisor),
                        ),
                    ),
                )
                .with_project_trust(std::sync::Arc::new(
                    codex_native_integration::CodexHomeProjectTrust::new(inputs.codex_home.clone()),
                ));
                let socket_path = socket_directory.join(format!(
                    "{}.sock",
                    String::from(description.endpoint.endpoint_id.clone())
                ));
                provider_app_servers.push(
                    collaboration_runtime::RouterSessionAppServerListener::bind(
                        &socket_path,
                        std::sync::Arc::new(context),
                    )?,
                );
            }
        }
        #[cfg(feature = "runtime-test-support")]
        let provider_test_hub = provider_session_hub.as_ref().map(std::sync::Arc::downgrade);
        drop(provider_session_hub);
        let publication = publication.with_acp_listener()?;
        let manifest = collaboration_runtime::ManifestPublication::publish(
            &inputs.directory,
            &collaboration_protocol::ServiceManifest {
                version: collaboration_protocol::SERVICE_MANIFEST_VERSION,
                service_id: machine_identity.service_id().clone(),
                service_epoch: service_epoch.clone(),
                machine_label: machine_identity.machine_label().clone(),
                service_version: NonEmptyText::try_from(env!("CARGO_PKG_VERSION").to_owned())
                    .map_err(io::Error::other)?,
                api: collaboration_protocol::ApiSelector {
                    transport: collaboration_protocol::ApiTransport::StreamableHttpUnix,
                    path: collaboration_protocol::ApiSocketPath::ServiceSocket,
                },
                mcp: collaboration_protocol::McpSelector {
                    transport: collaboration_protocol::McpTransport::StreamableHttp,
                    url: mcp_url,
                },
                native_schema_digest: native_digest.clone(),
                router_proxy_endpoint,
            },
        )?;
        let (message_system, message_tools) = match message_roots {
            Some(startup) => {
                let custody_events = Arc::new(HostCustodyEvents);
                let recipient_directory = Arc::new(CombinedRecipientSessionDirectory::new(
                    RecipientSessionSources {
                        codex: Arc::new(
                            crate::recipient_session_inventory::CodexSessionInventory::new(
                                inputs.codex_home.clone(),
                                unmaterialized_threads.clone(),
                                publication.admission_gate(),
                            ),
                        ),
                        managed_providers: Arc::new(
                            crate::recipient_session_inventory::ManagedProviderSessionInventory::new(
                                service_id.clone(),
                                provider_store.clone(),
                                external_provider_supervisor.clone(),
                            )
                            .map_err(io::Error::other)?,
                        ),
                        claude_terminal: Arc::new(
                            crate::recipient_session_inventory::ClaudeTerminalSessionInventory::new(
                                claude_code_sessions,
                                worker_clock.clone(),
                                custody_events.clone(),
                            ),
                        ),
                    },
                ));
                let events = MessageSystemEvents::new(custody_events);
                let events = match external_provider_supervisor.clone() {
                    Some(supervisor) => events.with_managed_notice(move |machine| {
                        Arc::new(crate::ManagedAcpNoticeAdapter::new(
                            supervisor.clone(),
                            machine.clone(),
                        ))
                    }),
                    None => events,
                };
                let system = startup
                    .start(worker_clock.clone(), recipient_directory.clone(), events)
                    .await;
                let front = system.front();
                let admission = super::message_startup::v0_message_admission(
                    front.clone(),
                    recipient_directory,
                    worker_clock.clone(),
                );
                (
                    Some(system),
                    MessageOperations::new(admission, Arc::new(front), worker_clock.clone()),
                )
            }
            None => (None, MessageOperations::default()),
        };
        let application = application.with_messages(message_tools);
        let shutdown = CancellationToken::new();
        let collaboration_api =
            collaboration_api.serve(&collaboration_mcp::CollaborationApiConfig {
                application,
                service_directory: inputs.directory.clone(),
                native_definitions,
                router_executable_check,
                concurrent_requests: collaboration_mcp::DEFAULT_CONCURRENT_REQUESTS,
                shutdown: shutdown.clone(),
            });
        let mut tasks = JoinSet::new();
        let mut provider_retirement_tasks = JoinSet::new();
        tasks.spawn(native.run(shutdown.clone()));
        tasks.spawn(acp.run(shutdown.clone()));
        for app_server in provider_app_servers {
            tasks.spawn(app_server.run(shutdown.clone()));
        }
        for (retirement, description) in provider_retirements {
            provider_retirement_tasks.spawn(
                crate::provider_startup_composition::publish_provider_retirement(
                    retirement,
                    description,
                    endpoint_directory.clone(),
                    worker_clock.clone(),
                ),
            );
        }
        let mut runtime = Self {
            clock: worker_clock,
            message_system,
            installation_peer,
            owner_human_id,
            provider_store,
            #[cfg(feature = "runtime-test-support")]
            provider_test_hub,
            external_provider_supervisor,
            provider_cache_warm,
            provider_delivery_route,
            session_delivery_router,
            provider_retention: None,
            #[cfg(test)]
            provider_retention_observation: None,
            open_scratch_scopes,
            scratch_sweep: None,
            directory: inputs.directory,
            native_digest,
            payload_cache: crate::native_schema_cache::NativeSchemaCache::default(),
            service_id,
            service_epoch,
            publication,
            shutdown,
            tasks,
            provider_retirement_tasks,
            manifest: Some(manifest),
            collaboration_api,
            tailscale_exposure: None,
        };
        if let Some(prepared) = prepared_exposure {
            match prepared
                .start(
                    exposure_backend,
                    &runtime.service_id,
                    &runtime.service_epoch,
                )
                .await
            {
                Ok(exposure) => {
                    eprintln!("Private collaboration MCP: {}", exposure.url());
                    runtime.tailscale_exposure = Some(exposure);
                }
                Err(error) => {
                    let cleanup = runtime.shutdown().await;
                    return Err(match cleanup {
                        Ok(()) => error,
                        Err(cleanup) => io::Error::other(format!(
                            "{error}; collaboration startup cleanup: {cleanup}"
                        )),
                    });
                }
            }
        }
        Ok(runtime)
    }
}
