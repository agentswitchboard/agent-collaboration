use approval_request_protocol::RaiseReply;
use central_engine_model::{Context, DueRow, EngineTime};
use central_storage::{
    BatchOutcome, EngineAlarmTimes, ReadOutcome, Statement, StatementBatch, StoreFailure,
};
use fleet_actor_identity::{
    AuthenticatedSession, SessionCallOutcome, SessionSecretSource, SessionService,
    SessionServiceError,
};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyingStore, ApprovalHolderAdmission, ApprovalHolderScope,
    ApprovalSession,
};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, HarnessKind, PresentedSessionSecret, SessionCreator, SessionSecret,
};
use fleet_machine_login::{
    MachineLoginService, link_authority::ValidatedMachineConnection, policy::DecisionTime,
};
use fleet_runtime_ports::{CentralStore, DueListQuery};
use workspace_ids::CentralSessionId;

use super::{
    approval_store_cases::{first_holder, register},
    fixtures::{key, operation_id, provider, time},
    native_approval_store::{NativeApprovalStoreFixture, NativeApprovalTransaction},
};

struct ReadOnlyMachineAuthority(std::path::PathBuf);

struct FixtureIds;

impl fleet_runtime_ports::IdSource for FixtureIds {
    fn new_id(&self) -> uuid::Uuid {
        panic!("restoring a machine attachment never mints an ID")
    }
}

impl CentralStore for ReadOnlyMachineAuthority {
    async fn run_batch(&self, _: StatementBatch) -> Result<BatchOutcome, StoreFailure> {
        panic!("restoring a machine attachment never writes")
    }

    async fn read(&self, statement: Statement) -> Result<ReadOutcome, StoreFailure> {
        NativeApprovalStoreFixture::read_authority_statement(&self.0, statement).await
    }

    async fn read_timed_due_rows(&self, _: DueListQuery) -> Result<Vec<DueRow>, StoreFailure> {
        panic!("restoring a machine attachment never reads due work")
    }

    async fn engine_alarm_inputs(&self) -> Result<EngineAlarmTimes, StoreFailure> {
        panic!("restoring a machine attachment never reads alarm inputs")
    }

    async fn published_alarm(&self) -> Result<Option<EngineTime>, StoreFailure> {
        panic!("restoring a machine attachment never reads the published alarm")
    }
}

struct FixtureSessionSecret(u8);

impl SessionSecretSource for FixtureSessionSecret {
    fn new_session_secret(&mut self) -> Result<SessionSecret, SessionServiceError> {
        Ok(SessionSecret::from_bytes(&[self.0; 32]))
    }
}

pub(super) struct NativeSessionApprovalFixture {
    pub store: NativeApprovalStoreFixture,
    holder: ApprovalHolderScope,
    requester: ApprovalSession,
    pub machine: ValidatedMachineConnection,
    parent: CentralSessionId,
    presented: PresentedSessionSecret,
}

impl NativeSessionApprovalFixture {
    pub async fn open() -> Self {
        let store = NativeApprovalStoreFixture::open_fresh().await;
        let holder = first_holder();
        register(&store, &holder).await;
        store
            .alter_fixture(include_str!("../fixtures/approval_machine_authority.sql"))
            .await;
        let attach = serde_json::from_value(serde_json::json!({
            "organizationId": holder.organization_id,
            "machineId": holder.run.enrollment.machine_id,
            "enrollmentId": holder.run.enrollment.enrollment_id,
            "clientId": holder.run.enrollment.enrollment_id,
            "effectiveExpiresAt": 86_400,
        }))
        .unwrap();
        let authority = ReadOnlyMachineAuthority(store.database_path());
        let result =
            MachineLoginService::for_organization(&authority, &holder.organization_id, &FixtureIds)
                .restore_link(&attach, DecisionTime { now_ms: 0 })
                .await
                .unwrap();
        let Ok(machine) = result else {
            panic!("the machine attachment must restore against the enrollment rows");
        };
        let (opened, _) = store
            .apply_on_loan(None, async |loan, _, ids| {
                let mut service = SessionService::in_transaction(loan);
                let mut context = Context { now: time(0), ids };
                let (parent, presented) = create_bound_session(
                    &mut service,
                    &machine,
                    &mut context,
                    11,
                    "approval-parent",
                    SessionCreator::RouterOperator {},
                    CreateSessionKind::Ordinary {},
                )
                .await;
                let (child, _) = create_bound_session(
                    &mut service,
                    &machine,
                    &mut context,
                    12,
                    "approval-requester",
                    SessionCreator::Session {
                        session_id: parent.clone(),
                        presented: presented.clone(),
                    },
                    CreateSessionKind::Delegate {
                        parent: parent.clone(),
                    },
                )
                .await;
                Ok((parent, presented, child))
            })
            .await;
        let (_, (parent, presented, child)) = opened.unwrap();
        let requester = ApprovalSession {
            enrollment: holder.run.enrollment.clone(),
            session_id: child,
        };
        Self {
            store,
            holder,
            requester,
            machine,
            parent,
            presented,
        }
    }

    pub async fn raise(&self, number: u16) -> approval_request_protocol::ApprovalRequestId {
        let request = key(&self.holder.run, number);
        let raise = provider(&request, &self.requester, "Routed to a session");
        let holder = ApprovalHolderAdmission::for_connection(
            &self.machine,
            self.holder.run.router_run.clone(),
        );
        let (raised, _) = self
            .store
            .apply_on_loan(None, async |loan, commit, ids| {
                let mut store = ApprovalApplyingStore::new(
                    &commit,
                    loan,
                    &ActorApprovalSessionFacts,
                    &self.store.clock,
                    ids,
                );
                store.raise_from_holder(&holder, Box::new(raise)).await
            })
            .await;
        let (proof, reply) = raised.unwrap();
        assert!(matches!(
            reply.take_fixture_reply(&proof).unwrap(),
            RaiseReply::Receipt { .. }
        ));
        request.request_id
    }

    pub async fn authenticate(
        &self,
        loan: &mut NativeApprovalTransaction,
    ) -> Result<AuthenticatedSession, StoreFailure> {
        Ok(SessionService::in_transaction(loan)
            .authenticate_live_session(&self.machine, &self.parent, &self.presented)
            .await?
            .expect("the bound destination session authenticates"))
    }
}

async fn create_bound_session(
    service: &mut SessionService<'_, NativeApprovalTransaction>,
    machine: &ValidatedMachineConnection,
    context: &mut Context<'_>,
    number: u8,
    harness_session_id: &str,
    creator: SessionCreator,
    session_kind: CreateSessionKind,
) -> (CentralSessionId, PresentedSessionSecret) {
    let request = CreateFleetSession {
        operation_id: operation_id(u16::from(number)).as_str().parse().unwrap(),
        attempt: 1.try_into().unwrap(),
        creator,
        agent_program: HarnessKind::Codex,
        session_kind,
    };
    let created = service
        .create(machine, request, context, &mut FixtureSessionSecret(number))
        .await
        .unwrap();
    let SessionCallOutcome::Answered(CreateFleetSessionReply::Reserved {
        session_id, secret, ..
    }) = created
    else {
        panic!("expected a new session reservation, got {created:?}");
    };
    let bound = service
        .bind(
            machine,
            BindFleetSession {
                session_id: session_id.clone(),
                harness_session_id: harness_session_id.parse().unwrap(),
                secret: secret.clone(),
            },
            context,
        )
        .await
        .unwrap();
    assert!(matches!(
        bound,
        SessionCallOutcome::Answered(BindFleetSessionReply::Active { .. })
    ));
    (session_id, PresentedSessionSecret::Secret { secret })
}
