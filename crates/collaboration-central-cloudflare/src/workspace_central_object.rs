//! Org-scoped central: shared core over a statement-only DO adapter, no Fleet engine.
use crate::workspace_approval_transactions::WorkerApprovalTransactions;
use crate::workspace_session_transactions::{WorkerSessionTransactions, fill_session_secret};
use crate::{
    object_location::ObjectAddress,
    sign_in_trust::CloudflareSignInTrust,
    statement_store::{self, AlarmPolicy},
    worker_clock::WorkspaceClock,
};
use central_link_call_composition::OrganizationLinkDispatcher;
use central_storage::{
    BatchOutcome, ReadOutcome, Statement, StatementBatch, StoreCall, StoreFailure, StoreReply,
};
use collaboration_central_core::WorkspaceRuntime;
use fleet_contracts::{WorkspaceRequest, WorkspaceResponse, WorkspaceServiceProtocol};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::{CallFailure, Envelope, Transport};
use fleet_runtime::{ClockIds, FleetRuntime};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use futures_util::lock::Mutex;
use sqlx_durable_object::DoConnection;
use std::sync::Arc;
use worker::{
    DurableObject, Env, Request, Response, Result, State, durable_object,
    send::{SendFuture, SendWrapper},
    wasm_bindgen,
};

struct CentralStatements {
    #[cfg(feature = "test-support")]
    faults: Arc<fleet_runtime::TestFaults>,
    #[cfg(feature = "test-support")]
    zero_next_write_alarm: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "test-support")]
    witness_env: SendWrapper<Env>,
    #[cfg(feature = "test-support")]
    witnesses: workspace_link_witness::PendingWitnesses,
    connection: Arc<SendWrapper<DoConnection>>,
    state: Arc<SendWrapper<State>>,
    clock: WorkspaceClock,
    decide_alarm: central_storage::EngineAlarmDecider,
}
impl CentralStore for CentralStatements {
    fn run_batch(
        &self,
        batch: StatementBatch,
    ) -> impl std::future::Future<Output = std::result::Result<BatchOutcome, StoreFailure>> + Send
    {
        SendFuture::new(async move {
            #[cfg(feature = "test-support")]
            let fault = self.faults.take_commit(&batch);
            #[cfg(feature = "test-support")]
            if let Some(result) = fault.as_ref().and_then(|fault| fault.before()) {
                return result;
            }
            #[cfg(feature = "test-support")]
            if let Some(failure) = self.faults.take_alarm_batch_failure(&batch) {
                return Err(failure);
            }
            let decide_alarm = self.engine_decider(
                !batch.statements.is_empty()
                    && matches!(batch.alarm, central_storage::AlarmUpdate::Engine(_)),
            );
            #[cfg(feature = "test-support")]
            let (decide_alarm, captured_alarm) = workspace_link_witness::capture_alarm_inputs(
                &self.witness_env,
                decide_alarm,
                &batch,
            );
            let result = statement_store::run_batch(
                &self.connection,
                batch.into(),
                AlarmPolicy::Engine(self.state.storage(), self.clock.clone(), decide_alarm),
            )
            .await;
            #[cfg(feature = "test-support")]
            if let (Some(capture), Ok(BatchOutcome::Applied { publication, .. })) =
                (captured_alarm, &result)
            {
                self.witnesses.alarm_committed(capture, publication);
            }
            #[cfg(feature = "test-support")]
            if fault.is_some_and(|fault| {
                matches!(
                    fault.kind,
                    fleet_runtime::EngineCommitFaultKind::AppliedAnswerLost
                )
            }) && matches!(result, Ok(BatchOutcome::Applied { .. }))
            {
                return Err(StoreFailure::AnswerLost);
            }
            result
        })
    }
    fn read(
        &self,
        statement: Statement,
    ) -> impl std::future::Future<Output = std::result::Result<ReadOutcome, StoreFailure>> + Send
    {
        SendFuture::new(async move {
            #[cfg(feature = "test-support")]
            if let Some(failure) = self.faults.take_read_failure(&statement) {
                return Err(failure);
            }
            #[cfg(feature = "test-support")]
            let sql = statement.sql;
            let result = statement_store::read(&self.connection, statement.into()).await;
            #[cfg(feature = "test-support")]
            self.witnesses
                .read_completed(&self.witness_env, &self.state, sql, &result);
            result
        })
    }
    fn read_timed_due_rows(
        &self,
        query: fleet_runtime_ports::DueListQuery,
    ) -> impl std::future::Future<
        Output = std::result::Result<Vec<fleet_runtime_ports::DueRow>, StoreFailure>,
    > + Send {
        SendFuture::new(async move {
            #[cfg(feature = "test-support")]
            if let Some(failure) = self.faults.take_due_read_failure(query.kind()) {
                return Err(failure);
            }
            statement_store::read_timed_due_rows(&self.connection, query).await
        })
    }

    fn engine_alarm_inputs(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<central_storage::EngineAlarmTimes, StoreFailure>,
    > + Send {
        SendFuture::new(async move { statement_store::engine_alarm_inputs(&self.connection).await })
    }
    fn published_alarm(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<Option<chrono::DateTime<chrono::Utc>>, StoreFailure>,
    > + Send {
        SendFuture::new(async move {
            let alarm = self
                .state
                .storage()
                .get_alarm()
                .await
                .map_err(|_| StoreFailure::Unavailable)?;
            alarm
                .map(|millis| {
                    chrono::DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                        StoreFailure::Corrupt {
                            reason: central_storage::CorruptReason::BadValue {
                                column: "alarm.deadline".into(),
                            },
                        }
                    })
                })
                .transpose()
        })
    }
}
#[derive(Clone)]
struct WorkspaceTransport {
    env: Arc<SendWrapper<Env>>,
    #[cfg(feature = "test-support")]
    lose_read: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "test-support")]
    legacy_workspace_calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl WorkspaceStores for WorkspaceTransport {
    fn call_workspace(
        &self,
        id: &workspace_model::WorkspaceId,
        call: StoreCall,
    ) -> impl std::future::Future<Output = std::result::Result<StoreReply, StoreFailure>> + Send
    {
        let id = id.clone();
        SendFuture::new(async move {
            #[cfg(feature = "test-support")]
            self.legacy_workspace_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            #[cfg(feature = "test-support")]
            let is_read = matches!(&call, StoreCall::Read(_));
            let answer = crate::cloudflare_transport::call_store(&self.env, id, call.into()).await;
            #[cfg(feature = "test-support")]
            if is_read
                && self
                    .lose_read
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StoreFailure::AnswerLost);
            }
            answer
        })
    }
}
impl Transport<WorkspaceServiceProtocol> for WorkspaceTransport {
    fn exchange(
        &self,
        request: Envelope<workspace_ids::WorkspaceId, WorkspaceRequest>,
    ) -> impl std::future::Future<
        Output = std::result::Result<
            Envelope<workspace_ids::WorkspaceId, WorkspaceResponse>,
            CallFailure,
        >,
    > + Send {
        SendFuture::new(async move {
            let answer = crate::cloudflare_transport::workspace_exchange(
                &self.env,
                request.target.clone(),
                "/workspace-rpc",
                &request,
                None,
            )
            .await;
            #[cfg(feature = "test-support")]
            if self
                .lose_read
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(CallFailure::AnswerLost);
            }
            answer
        })
    }
}

type OrgRuntime = WorkspaceRuntime<CentralStatements, WorkspaceTransport>;
#[durable_object]
pub struct CentralObject {
    state: Arc<SendWrapper<State>>,
    env: Env,
    connection: DoConnection,
    runtime: Mutex<Option<Arc<OrgRuntime>>>,
    clock: WorkspaceClock,
    link_ids: Arc<ClockIds<WorkspaceClock>>,
    pending_mac_calls: workspace_typed_link::PendingMacCalls,
    due_batches: Arc<fleet_runtime::DueBatchState>,
    mail_pushes: Arc<std::sync::Mutex<Vec<fleet_contracts::MailDeliveryReady>>>,
    mail_offers: workspace_machine_link_io::WorkerMailOffers,
    #[cfg(feature = "test-support")]
    controls: workspace_cases::Controls,
    #[cfg(feature = "test-support")]
    witnesses: workspace_link_witness::PendingWitnesses,
    #[cfg(feature = "test-support")]
    launcher_clock_sampled: Mutex<bool>,
}
impl CentralObject {
    fn check_physical_organization(&self, organization: &OrganizationId) -> Result<()> {
        if self
            .env
            .durable_object("CENTRAL")?
            .id_from_name(organization.as_str())?
            .to_string()
            != self.state.id().to_string()
        {
            return Err(worker::Error::RustError("wrong physical org".to_owned()));
        }
        Ok(())
    }

    fn statement_host(&self) -> Arc<CentralStatements> {
        let connection = DoConnection::new(self.state.storage());
        #[cfg(feature = "test-support")]
        let connection = {
            let observations = self.controls.workspace_page_reads.clone();
            let retired_observations = self.controls.retired_page_reads.clone();
            let mark_guard_observations = self.controls.direct_mark_guard_reads.clone();
            connection.with_fetch_observer(move |sql, rows_read| {
                if sql
                    == include_str!(
                        "../../fleet-workspace-directory/src/queries/read_workspace_page.sql"
                    )
                    && let Some(armed) = observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut()
                {
                    armed.push(rows_read);
                }
                if sql == include_str!("../../fleet-mail/src/queries/read_due_mark_guard.sql")
                    && let Some(armed) = mark_guard_observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut()
                {
                    armed.push(rows_read);
                }
                if fleet_engine::is_retired_attempt_page_sql(sql)
                    && let Some(armed) = retired_observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_mut()
                {
                    armed.push(rows_read);
                }
            })
        };
        Arc::new(CentralStatements {
            #[cfg(feature = "test-support")]
            faults: self.controls.faults.clone(),
            #[cfg(feature = "test-support")]
            zero_next_write_alarm: self.controls.zero_next_write_alarm.clone(),
            #[cfg(feature = "test-support")]
            witness_env: SendWrapper::new(self.env.clone()),
            #[cfg(feature = "test-support")]
            witnesses: self.witnesses.clone(),
            connection: Arc::new(SendWrapper::new(connection)),
            state: self.state.clone(),
            clock: self.clock.clone(),
            decide_alarm: fleet_runtime::engine_alarm_decider(self.due_batches.clone()),
        })
    }
    async fn migrate(&self) -> std::result::Result<(), StoreFailure> {
        statement_store::migrate(
            &self.connection,
            Arc::new(fleet_storage_schema::workspace_migrations::central_migrator()),
        )
        .await
        .map_err(|_| StoreFailure::Unavailable)
    }
    async fn runtime(&self, organization: OrganizationId) -> Result<Arc<OrgRuntime>> {
        self.runtime_checked(organization, ColdAlarmStart::RecoverOverdueWakes)
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))
    }
    async fn runtime_checked(
        &self,
        organization: OrganizationId,
        startup: ColdAlarmStart,
    ) -> std::result::Result<Arc<OrgRuntime>, StoreFailure> {
        let mut initialized = self.runtime.lock().await;
        if let Some(runtime) = initialized.as_ref() {
            return Ok(runtime.clone());
        }
        self.migrate().await?;
        let host = self.statement_host();
        let clock: Arc<dyn fleet_runtime_ports::Clock> = Arc::new(self.clock.clone());
        let central = WorkspaceRuntime::new(
            FleetRuntime::open(
                organization.clone(),
                host.clone(),
                WorkspaceTransport {
                    env: Arc::new(SendWrapper::new(self.env.clone())),
                    #[cfg(feature = "test-support")]
                    lose_read: self.controls.lose_read.clone(),
                    #[cfg(feature = "test-support")]
                    legacy_workspace_calls: self.controls.legacy_workspace_calls.clone(),
                },
                clock.clone(),
                self.link_ids.clone(),
                self.due_batches.clone(),
                Arc::new(crate::signin_observation::observe_signin),
            ),
            host.clone(),
            host.clone(),
        );
        let ending_transport = Arc::new(
            workspace_typed_link::approval_delivery::WorkerApprovalDelivery::new(
                self.state.clone(),
                self.clock.clone(),
                self.pending_mac_calls.clone(),
            ),
        );
        let dispatcher = Arc::new(OrganizationLinkDispatcher::new(
            WorkerSessionTransactions {
                connection: host.connection.clone(),
                #[cfg(feature = "test-support")]
                fail_reserved_session_reply: self.controls.fail_reserved_session_reply.clone(),
            },
            WorkerApprovalTransactions {
                connection: host.connection.clone(),
                state: self.state.clone(),
                clock: self.clock.clone(),
                decide_alarm: host.decide_alarm.clone(),
                #[cfg(feature = "test-support")]
                fail_after_write: self.controls.fail_approval_after_write.clone(),
            },
            ending_transport.clone(),
            clock.clone(),
            self.link_ids.clone(),
            fill_session_secret,
        ));
        let central = central.with_person_approval_transactions(dispatcher.clone());
        #[cfg(not(feature = "test-support"))]
        let central = central.with_link_call_dispatcher(dispatcher);
        #[cfg(feature = "test-support")]
        let central = if self
            .env
            .var("TEST_SUPPORT_LINK_CALLS")
            .is_ok_and(|mode| mode.to_string() == "legacy")
        {
            central.with_fixture_link_call_dispatcher(dispatcher)
        } else {
            central.with_link_call_dispatcher(dispatcher)
        };
        let central = match crate::organization_signin_link::trust_settings(&self.env) {
            Ok(Some(settings)) => {
                match CloudflareSignInTrust::new(settings).for_organization(organization, clock) {
                    Ok(check) => central.with_sign_in_verification(check),
                    Err(fleet_signin::organization_authority::InvalidStampTrust) => central,
                }
            }
            // Invalid/missing trust never permits sign-in. The default checker
            // returns the closed InvalidConfiguration refusal for core to observe.
            Ok(None) | Err(_) => central,
        };
        let mail_pushes = self.mail_pushes.clone();
        let state_for_revocation = self.state.clone();
        let calls_for_revocation = self.pending_mac_calls.clone();
        let deadline_state = self.state.clone();
        let central = Arc::new(
            central
                .with_approval_delivery_completion(Arc::new(move |work| {
                    deadline_state.wait_until(work)
                }))
                .with_mail_delivery_observer(Arc::new(move |message| {
                    mail_pushes
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(message);
                }))
                .with_enrollment_invalidation_observer(Arc::new(move |invalidation| {
                    workspace_machine_link::close_enrollment_sockets(
                        &state_for_revocation,
                        &calls_for_revocation,
                        &invalidation,
                        None,
                    );
                })),
        );
        ending_transport.install_runtime(&central);
        match startup {
            ColdAlarmStart::DriveFollows => central.recompute_alarm().await?,
            ColdAlarmStart::RecoverOverdueWakes => Box::pin(central.recover_cold_alarm()).await?,
        }
        *initialized = Some(central.clone());
        Ok(central)
    }
}
impl DurableObject for CentralObject {
    fn new(state: State, env: Env) -> Self {
        let clock = WorkspaceClock::default();
        Self {
            connection: DoConnection::new(state.storage()),
            state: Arc::new(SendWrapper::new(state)),
            env,
            runtime: Mutex::new(None),
            link_ids: Arc::new(ClockIds::new(clock.clone())),
            clock,
            pending_mac_calls: workspace_typed_link::PendingMacCalls::default(),
            due_batches: Arc::new(fleet_runtime::DueBatchState::default()),
            mail_pushes: Default::default(),
            mail_offers: Default::default(),
            #[cfg(feature = "test-support")]
            controls: workspace_cases::Controls::default(),
            #[cfg(feature = "test-support")]
            witnesses: workspace_link_witness::PendingWitnesses::default(),
            #[cfg(feature = "test-support")]
            launcher_clock_sampled: Mutex::new(false),
        }
    }
    async fn fetch(&self, request: Request) -> Result<Response> {
        let address = request
            .headers()
            .get("x-central-object-address")?
            .ok_or_else(|| worker::Error::RustError("central address missing".to_owned()))?;
        let ObjectAddress::Central(organization) = serde_json::from_str(&address)? else {
            return Response::error("wrong object kind", 400);
        };
        self.check_physical_organization(&organization)?;
        #[cfg(feature = "test-support")]
        {
            signin_launcher_controls::sample_clock(self, &organization).await?;
            if request.path() == "/test/raw-signin-observe" {
                return signin_launcher_controls::observe(self, &organization).await;
            }
            if request.path().starts_with("/test/raw-") {
                return workspace_cases::raw_control(self, request).await;
            }
        }
        if workspace_typed_link::is_mac_call_path(&request.path()) {
            return self.handle_mac_call(request, organization).await;
        }
        self.migrate()
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        let identity =
            collaboration_central_core::read_central_organization(self.statement_host().as_ref())
                .await
                .map_err(|error| worker::Error::RustError(error.to_string()))?;
        if matches!(identity, collaboration_central_core::CentralOrganizationIdentity::Stamped { organization_id } if organization_id != organization)
        {
            if crate::organization_signin_link::is_signin_path(&request.path()) {
                return Response::empty().map(|response| response.with_status(503));
            }
            return Response::error("central organization mismatch", 403);
        }
        #[cfg(feature = "test-support")]
        if request.path().starts_with("/test/") {
            return workspace_cases::control(self, organization, request).await;
        }
        if crate::organization_signin_link::is_signin_path(&request.path()) {
            let runtime = match self
                .runtime_checked(organization, ColdAlarmStart::RecoverOverdueWakes)
                .await
            {
                Ok(runtime) => runtime,
                Err(_) => return Response::empty().map(|response| response.with_status(503)),
            };
            let request: worker::HttpRequest = request.try_into()?;
            let (parts, body) = request.into_parts();
            let body = match crate::http_body::complete(body).await? {
                crate::http_body::BodyCompletion::Complete(body) => body,
                crate::http_body::BodyCompletion::OverLimit => {
                    return crate::http_body::over_limit();
                }
            };
            use tower_service::Service;
            let response = collaboration_central_core::organization_signin_router(runtime.clone())
                .call(http::Request::from_parts(parts, body))
                .await?;
            self.flush_ready_mail(&runtime).await;
            return response.try_into();
        }
        if let Some(path_organization) = crate::workspace_link_entry::organization(&request.path())
        {
            if path_organization.as_ref() != Ok(&organization) {
                return Response::error("wrong organization", 403);
            }
            return self.upgrade_machine_link(organization, &request).await;
        }
        Response::error("unknown central route", 404)
    }

    async fn websocket_message(
        &self,
        socket: worker::WebSocket,
        message: worker::WebSocketIncomingMessage,
    ) -> Result<()> {
        self.handle_machine_message(socket, message).await
    }
    async fn websocket_close(
        &self,
        socket: worker::WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        self.close_machine_link(&socket, 1000, Some("closed"))
    }
    async fn websocket_error(
        &self,
        socket: worker::WebSocket,
        _error: worker::Error,
    ) -> Result<()> {
        self.close_machine_link(&socket, 1011, Some("link error"))
    }

    async fn alarm(&self) -> Result<Response> {
        self.drive_workspace_alarm().await
    }
}

#[cfg(feature = "test-support")]
#[path = "workspace_cases.rs"]
pub(crate) mod workspace_cases;

#[cfg(feature = "test-support")]
#[path = "signin_launcher_controls.rs"]
mod signin_launcher_controls;

#[path = "workspace_machine_link.rs"]
mod workspace_machine_link;
#[path = "workspace_machine_link_io.rs"]
mod workspace_machine_link_io;

#[path = "workspace_socket_attachment.rs"]
mod workspace_socket_attachment;

#[path = "workspace_typed_link.rs"]
pub(crate) mod workspace_typed_link;

#[cfg(feature = "test-support")]
#[path = "workspace_link_witness.rs"]
mod workspace_link_witness;

#[path = "workspace_alarm_decision.rs"]
mod workspace_alarm_decision;

#[path = "workspace_stamp_store.rs"]
mod stamp_store;

#[path = "workspace_direct_mail.rs"]
mod direct_mail;

#[path = "workspace_alarm_owner.rs"]
mod workspace_alarm_owner;
#[cfg(feature = "test-support")]
use workspace_alarm_owner::AlarmFailure;
use workspace_alarm_owner::ColdAlarmStart;
