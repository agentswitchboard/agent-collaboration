//! Native composition of org cores over the statement-only SQLite capability.
use crate::{
    machine_push_registry::MachinePushRegistry,
    object_storage::{ObjectStorage, direct_mail::NativeDirectMessageStore},
    sign_in_trust::NativeSignInTrust,
};
use collaboration_central_core::WorkspaceCaller;
use fleet_contracts::{
    OrganizationStatementDigestReply, ProvisionOrganization, ProvisionOrganizationResult,
    ReadOrganizationStatementDigest,
};
use fleet_contracts::{
    WorkspaceRequest as WorkspaceRpcRequest, WorkspaceResponse, WorkspaceServiceProtocol,
    WorkspaceStatusFailure,
};
use fleet_rpc::{CallFailure, Envelope, Transport, dispatch};
use fleet_workspace_service::WorkspaceService;
#[path = "workspace_alarm_owner.rs"]
mod workspace_alarm_owner;
#[path = "workspace_approval_delivery.rs"]
pub(crate) mod workspace_approval_delivery;
use crate::object_storage::session_transaction_owner::{
    NativeSessionTransactions, fill_session_secret,
};
use central_link_call_composition::OrganizationLinkDispatcher;
use central_link_protocol::{FreshCorrelationId, MacCallAddress, MacLinkRpc};
use central_storage::{
    BatchOutcome, ReadOutcome, Statement, StatementBatch, StoreCall, StoreFailure, StoreReply,
};
use collaboration_central_core::{
    MacCallResult, OrganizationAdminAnswer, OrganizationSignInPort, PersonApprovalAnswer,
    SignInTrustSettings, TYPED_LINK_CALL_TIMEOUT, WorkspaceRuntime, finish_mac_call,
    identity_contract::{AuthorizationRevision, AuthorizationRevisionAcknowledgement},
    person_request::PersonRequestInput,
};
use fleet_contracts::AuthorizationRevisionFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime::{ClockIds, FleetRuntime};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_runtime_ports::{Clock, IdSource, SyncTime};
use fleet_signin::ApprovalPersonVerificationInput;
use router_clock::RuntimeClock;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;
use workspace_alarm_owner::WorkspaceAlarmOwner;
use workspace_ids::MachineId;
use workspace_model::central_mail::{CentralMailRequest, CentralMailResult};
use workspace_model::{WorkspaceId, WorkspaceReply, WorkspaceRequest};

impl CentralStore for ObjectStorage {
    async fn run_batch(&self, batch: StatementBatch) -> Result<BatchOutcome, StoreFailure> {
        ObjectStorage::run_batch(self, batch).await
    }
    async fn read(&self, statement: Statement) -> Result<ReadOutcome, StoreFailure> {
        ObjectStorage::read(self, statement).await
    }
    async fn read_timed_due_rows(
        &self,
        query: fleet_runtime_ports::DueListQuery,
    ) -> Result<Vec<fleet_runtime_ports::DueRow>, StoreFailure> {
        ObjectStorage::read_timed_due_rows(self, query).await
    }

    async fn engine_alarm_inputs(&self) -> Result<central_storage::EngineAlarmTimes, StoreFailure> {
        ObjectStorage::engine_alarm_inputs(self).await
    }

    async fn published_alarm(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>, StoreFailure> {
        ObjectStorage::published_alarm(self)
    }
}
#[derive(Clone)]
struct ClockHandle(Arc<dyn RuntimeClock>);
impl Clock for ClockHandle {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(runtime_primitives::Clock::now(self.0.as_ref()).unix_millis())
    }
}
#[derive(Clone)]
pub struct TokioWorkspaceStores {
    root: PathBuf,
    stores: Arc<Mutex<BTreeMap<WorkspaceId, Arc<ObjectStorage>>>>,
}
impl TokioWorkspaceStores {
    /// Use the fixture's existing object storage for both legacy and typed calls.
    #[cfg(feature = "test-support")]
    pub fn with_fixture_store(root: PathBuf, id: WorkspaceId, store: Arc<ObjectStorage>) -> Self {
        Self {
            root,
            stores: Arc::new(Mutex::new(BTreeMap::from([(id, store)]))),
        }
    }

    async fn store(&self, id: &WorkspaceId) -> Result<Arc<ObjectStorage>, StoreFailure> {
        let mut stores = self.stores.lock().await;
        if let Some(store) = stores.get(id) {
            return Ok(store.clone());
        }
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(|_| StoreFailure::Unavailable)?;
        let store = Arc::new(ObjectStorage::new(
            self.root.join(format!("{id}.sqlite")),
            Arc::new(fleet_storage_schema::workspace_migrations::workspace_migrator()),
        ));
        stores.insert(id.clone(), store.clone());
        Ok(store)
    }
}
impl WorkspaceStores for TokioWorkspaceStores {
    async fn call_workspace(
        &self,
        workspace: &WorkspaceId,
        call: StoreCall,
    ) -> Result<StoreReply, StoreFailure> {
        let store = self.store(workspace).await?;
        match call {
            StoreCall::Read(statement) => store.read(statement).await.map(StoreReply::Read),
            StoreCall::Batch(mut batch) => {
                batch.alarm = central_storage::AlarmUpdate::Unchanged;
                store.run_batch(batch).await.map(StoreReply::Batch)
            }
        }
    }
}
impl Transport<WorkspaceServiceProtocol> for TokioWorkspaceStores {
    async fn exchange(
        &self,
        request: Envelope<WorkspaceId, WorkspaceRpcRequest>,
    ) -> Result<Envelope<WorkspaceId, WorkspaceResponse>, CallFailure> {
        let workspace = request.target.clone();
        dispatch::<WorkspaceServiceProtocol, _, _>(&workspace, request, |call| async {
            let store = match self.store(&workspace).await {
                Ok(store) => store,
                Err(_) => return call.unavailable_reply(WorkspaceStatusFailure::Unavailable),
            };
            match store.workspace_pool().await {
                Ok(pool) => {
                    fleet_contracts::dispatch_workspace_request(&WorkspaceService::new(pool), call)
                        .await
                }
                Err(_) => call.unavailable_reply(WorkspaceStatusFailure::Unavailable),
            }
        })
        .await
        .map_err(|_| CallFailure::NeverReached)
    }
}

/// One process-local owner of each org, with workspace stores shared by their UUID.
pub struct TokioWorkspaceHost {
    root: PathBuf,
    clock: Arc<dyn RuntimeClock>,
    sign_in: Option<Arc<NativeSignInTrust>>,
    stores: TokioWorkspaceStores,
    orgs:
        Mutex<BTreeMap<OrganizationId, Arc<WorkspaceRuntime<ObjectStorage, TokioWorkspaceStores>>>>,
    alarms: Mutex<Vec<WorkspaceAlarmOwner>>,
    pub(crate) push_registry: MachinePushRegistry,
    approval_deadline_tasks: workspace_approval_delivery::ApprovalDeadlineTasks,
    #[cfg(feature = "test-support")]
    fixture_link_calls: bool,
}
impl TokioWorkspaceHost {
    pub async fn open(root: &Path, clock: Arc<dyn RuntimeClock>) -> Result<Self, StoreFailure> {
        Self::open_configured(root, clock, None).await
    }

    /// Sign-in trust is supplied before reopening any on-disk organization.
    pub async fn open_with_signin(
        root: &Path,
        clock: Arc<dyn RuntimeClock>,
        settings: SignInTrustSettings,
    ) -> Result<Self, StoreFailure> {
        let trust = NativeSignInTrust::new(settings).map_err(|_| StoreFailure::Unavailable)?;
        Self::open_configured(root, clock, Some(Arc::new(trust))).await
    }

    async fn open_configured(
        root: &Path,
        clock: Arc<dyn RuntimeClock>,
        sign_in: Option<Arc<NativeSignInTrust>>,
    ) -> Result<Self, StoreFailure> {
        tokio::fs::create_dir_all(root.join("central"))
            .await
            .map_err(|_| StoreFailure::Unavailable)?;
        let host = Self {
            root: root.to_owned(),
            clock,
            sign_in,
            stores: TokioWorkspaceStores {
                root: root.join("workspace-stores"),
                stores: Default::default(),
            },
            orgs: Default::default(),
            alarms: Default::default(),
            push_registry: MachinePushRegistry::default(),
            approval_deadline_tasks: Default::default(),
            #[cfg(feature = "test-support")]
            fixture_link_calls: false,
        };
        let mut entries = tokio::fs::read_dir(root.join("central"))
            .await
            .map_err(|_| StoreFailure::Unavailable)?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|_| StoreFailure::Unavailable)?
        {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("sqlite") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or(StoreFailure::Unavailable)?;
            let organization =
                OrganizationId::try_from(name.to_owned()).map_err(|_| StoreFailure::Unavailable)?;
            // Organizations open independently: one bad store never blocks the rest.
            if let Err(error) = host.central(&organization).await {
                tracing::warn!(?error, organization = %organization.as_str(), "central organization open failed; continuing other organizations");
            }
        }
        Ok(host)
    }
    #[cfg(feature = "test-support")]
    pub(crate) fn with_fixture_link_calls(mut self) -> Self {
        self.fixture_link_calls = true;
        self
    }

    pub async fn central(
        &self,
        organization: &OrganizationId,
    ) -> Result<Arc<WorkspaceRuntime<ObjectStorage, TokioWorkspaceStores>>, StoreFailure> {
        let mut orgs = self.orgs.lock().await;
        if let Some(runtime) = orgs.get(organization) {
            return Ok(runtime.clone());
        }
        let clock_handle = ClockHandle(self.clock.clone());
        let ids: Arc<dyn IdSource> = Arc::new(ClockIds::new(clock_handle.clone()));
        let clock: Arc<dyn Clock> = Arc::new(clock_handle);
        let due_batches = Arc::new(fleet_runtime::DueBatchState::default());
        let storage = Arc::new(
            ObjectStorage::new(
                self.root
                    .join("central")
                    .join(format!("{}.sqlite", organization.as_str())),
                Arc::new(fleet_storage_schema::workspace_migrations::central_migrator()),
            )
            .with_engine_publication(
                clock.clone(),
                fleet_runtime::engine_alarm_decider(due_batches.clone()),
            ),
        );
        let central = WorkspaceRuntime::new(
            FleetRuntime::open(
                organization.clone(),
                storage.clone(),
                self.stores.clone(),
                clock.clone(),
                ids.clone(),
                due_batches,
                Arc::new(crate::native_logging::observe),
            ),
            storage.clone(),
            Arc::new(NativeDirectMessageStore::new(
                storage.clone(),
                clock.clone(),
            )),
        );
        let ending_transport = Arc::new(workspace_approval_delivery::NativeApprovalDelivery::new(
            self.push_registry.clone(),
            self.clock.clone(),
        ));
        let dispatcher = Arc::new(OrganizationLinkDispatcher::new(
            NativeSessionTransactions {
                storage: storage.clone(),
            },
            NativeSessionTransactions { storage },
            ending_transport.clone(),
            clock.clone(),
            ids,
            fill_session_secret,
        ));
        let central = central.with_person_approval_transactions(dispatcher.clone());
        #[cfg(not(feature = "test-support"))]
        let central = central.with_link_call_dispatcher(dispatcher);
        #[cfg(feature = "test-support")]
        let central = if self.fixture_link_calls {
            central.with_fixture_link_call_dispatcher(dispatcher)
        } else {
            central.with_link_call_dispatcher(dispatcher)
        };
        let central = match &self.sign_in {
            Some(trust) => match trust.for_organization(organization.clone(), clock) {
                Ok(check) => central.with_sign_in_verification(check),
                // The default checker explicitly refuses every offered statement with
                // InvalidConfiguration, at the core observation boundary.
                Err(fleet_signin::organization_authority::InvalidStampTrust) => central,
            },
            None => central,
        };
        let registry = self.push_registry.clone();
        let organization_key = organization.clone();
        let revocation_registry = self.push_registry.clone();
        let revocation_org = organization.clone();
        let deadline_tasks = self.approval_deadline_tasks.clone();
        let central = Arc::new(
            central
                .with_approval_delivery_completion(Arc::new(move |work| {
                    deadline_tasks.schedule(work)
                }))
                .with_mail_delivery_observer(Arc::new(move |delivery| {
                    registry.offer(&organization_key, &delivery.machine_id, delivery.message_id);
                }))
                .with_enrollment_invalidation_observer(Arc::new(move |invalidation| {
                    revocation_registry.invalidate_enrollment(&revocation_org, &invalidation);
                })),
        );
        ending_transport.install_runtime(&central);
        Box::pin(central.recover_cold_alarm()).await?;
        self.alarms.lock().await.push(WorkspaceAlarmOwner::start(
            central.clone(),
            self.clock.clone(),
        ));
        orgs.insert(organization.clone(), central.clone());
        Ok(central)
    }
    pub(crate) fn link_clock(&self) -> &dyn RuntimeClock {
        self.clock.as_ref()
    }

    pub fn current_time(&self) -> runtime_primitives::SyncTime {
        self.clock.now()
    }

    /// Route a registered RPC to the current socket for this organization machine.
    pub async fn call_machine<R: MacLinkRpc>(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        request: Envelope<R::Target, R::Request>,
    ) -> MacCallResult<R> {
        let target = request.target.clone();
        let call = R::request(request);
        if matches!(
            call,
            central_link_protocol::MacCall::DeliverEnding(_)
                | central_link_protocol::MacCall::ApprovalNotice(_)
        ) {
            return Err(CallFailure::NeverReached);
        }
        self.call_raw_machine::<R>(organization, machine, &target, call)
            .await
    }

    #[cfg(feature = "test-support")]
    pub async fn fixture_call_machine<R: MacLinkRpc>(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        request: Envelope<R::Target, R::Request>,
    ) -> MacCallResult<R> {
        let target = request.target.clone();
        self.call_raw_machine::<R>(organization, machine, &target, R::request(request))
            .await
    }

    async fn call_raw_machine<R: MacLinkRpc>(
        &self,
        organization: &OrganizationId,
        machine: &MachineId,
        target: &R::Target,
        call: central_link_protocol::MacCall,
    ) -> MacCallResult<R> {
        let deadline = self.clock.monotonic_now().after(TYPED_LINK_CALL_TIMEOUT);
        let address = MacCallAddress::from(&call);
        let addressed_organization = address.organization_id().unwrap_or(organization);
        if addressed_organization != organization || address.machine_id() != machine {
            return Err(CallFailure::NeverReached);
        }
        let correlation = FreshCorrelationId::mint_at(self.clock.now().unix_millis());
        let pending =
            self.push_registry
                .queue_call(organization, correlation, call, address, deadline)?;
        finish_mac_call::<R>(target, pending.wait(self.clock.as_ref()).await?)
    }

    pub async fn request(
        &self,
        caller: &WorkspaceCaller,
        request: WorkspaceRequest,
    ) -> Result<WorkspaceReply, StoreFailure> {
        self.central(caller.organization_id())
            .await?
            .request(caller, request)
            .await
    }
    pub async fn request_central(
        &self,
        caller: &WorkspaceCaller,
        request: CentralMailRequest,
    ) -> Result<CentralMailResult, StoreFailure> {
        self.central(caller.organization_id())
            .await?
            .request_central(caller, request)
            .await
    }
    /// Reads an already opened fixture organization without creating a store.
    #[cfg(feature = "test-support")]
    pub async fn observe_signin_organization(
        &self,
        organization: &OrganizationId,
    ) -> Result<collaboration_central_core::signin_test_observation::SignInObservation, StoreFailure>
    {
        use collaboration_central_core::signin_test_observation::{
            SignInObservation, observe_signin_organization,
        };
        let runtime = self.orgs.lock().await.get(organization).cloned();
        match runtime {
            Some(runtime) => {
                observe_signin_organization(runtime.runtime().statement_host(), organization).await
            }
            None => Ok(SignInObservation::nothing_stored()),
        }
    }

    #[cfg(feature = "test-support")]
    /// Lists already opened fixture organizations without opening another store.
    pub async fn signin_live_organizations(&self) -> Vec<OrganizationId> {
        self.orgs.lock().await.keys().cloned().collect()
    }

    #[cfg(feature = "test-support")]
    pub async fn fixture_decide_approval(
        &self,
        organization: &OrganizationId,
        decision: collaboration_central_core::FixtureApprovalDecision,
    ) -> Result<(), StoreFailure> {
        let runtime = self
            .orgs
            .lock()
            .await
            .get(organization)
            .cloned()
            .ok_or(StoreFailure::Unavailable)?;
        if let Some(completion) = runtime.fixture_decide_approval(decision).await? {
            self.approval_deadline_tasks.schedule(completion);
        }
        Ok(())
    }

    pub async fn close(&self) {
        let alarms = std::mem::take(&mut *self.alarms.lock().await);
        for alarm in alarms {
            alarm.close().await;
        }
        self.approval_deadline_tasks.close().await;
        for runtime in self.orgs.lock().await.values() {
            runtime.runtime().statement_host().close().await;
        }
        for store in self.stores.stores.lock().await.values() {
            store.close().await;
        }
    }
}

impl OrganizationSignInPort for TokioWorkspaceHost {
    async fn provision(
        &self,
        request: &ProvisionOrganization,
    ) -> Result<ProvisionOrganizationResult, fleet_contracts::ProvisionFailure> {
        self.central(&request.organization_id)
            .await
            .map_err(|_| fleet_contracts::ProvisionFailure::Unavailable)?
            .provision(request)
            .await
    }

    async fn digest(
        &self,
        request: &ReadOrganizationStatementDigest,
    ) -> Result<OrganizationStatementDigestReply, fleet_contracts::DigestFailure> {
        self.central(&request.organization_id)
            .await
            .map_err(|_| fleet_contracts::DigestFailure::Unavailable)?
            .digest(request)
            .await
    }

    async fn revision(
        &self,
        request: &AuthorizationRevision,
    ) -> Result<AuthorizationRevisionAcknowledgement, AuthorizationRevisionFailure> {
        self.central(request.organization_id())
            .await
            .map_err(|_| AuthorizationRevisionFailure::Unavailable)?
            .revision(request)
            .await
    }

    async fn admin(
        &self,
        organization: OrganizationId,
        input: PersonRequestInput,
    ) -> Result<OrganizationAdminAnswer, StoreFailure> {
        self.central(&organization)
            .await?
            .admin(organization, input)
            .await
    }
    async fn person_approval(
        &self,
        organization: OrganizationId,
        input: ApprovalPersonVerificationInput<'_>,
    ) -> Result<PersonApprovalAnswer, StoreFailure> {
        self.central(&organization)
            .await?
            .person_approval(organization, input)
            .await
    }

    async fn device_login(
        &self,
        request: &fleet_contracts::DecideDeviceLogin,
    ) -> Result<fleet_contracts::DeviceLoginDecision, StoreFailure> {
        self.central(&request.organization_id)
            .await?
            .device_login(request)
            .await
    }
    async fn machine_token(
        &self,
        request: &fleet_contracts::DecideMachineToken,
    ) -> Result<fleet_contracts::MachineTokenDecision, StoreFailure> {
        self.central(&request.organization_id)
            .await?
            .machine_token(request)
            .await
    }
}
