use crate::{
    OrganizationApprovalTransactions, OrganizationSessionTransactions, SessionApplyFailure,
    SessionApplyFault, SessionApplyFuture, SessionApplyResult,
    apply_failure::SessionApplySuccess,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{ApprovalLinkApplication, HolderCommand, HolderReply},
    parent_session_dispatch::{
        ParentApprovalCall, ParentApprovalOperation, dispatch_parent_approval,
    },
};
use central_engine_model::{Context, EngineTime};
use central_link_protocol::{
    CentralCall, CentralLinkRpc, LinkCallRefusal, TypedCallResult,
    approval_rpc::{RaiseApprovalRpc, RegisterRunRpc, StopApprovalRpc},
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc},
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallDispatch, CentralCallWork, CentralLinkDispatcher, TargetCheckedCentralCall,
    TypedCallCarrier, ValidatedMachineConnection, dispatch_fallible_central_call,
    dispatch_fallible_correlated_central_call, recheck_lent_link_authority,
};
use fleet_actor_identity::{
    SessionCallOutcome, SessionSecretSource, SessionService, SessionServiceError,
};
use fleet_identity_protocol::{OrganizationId, SessionSecret};
use fleet_lent_transaction::CentralTransaction;
use fleet_rpc::Envelope;
use fleet_runtime_ports::{Clock, IdSource};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{future::Future, pin::Pin, sync::Arc};
use zeroize::Zeroizing;

/// The host could not fill a session secret from secure entropy.
#[derive(Clone, Copy, Debug)]
pub struct SessionEntropyUnavailable;

#[path = "person_approval_dispatch.rs"]
mod person_approval_dispatch;

type FillSessionSecret = fn(&mut [u8; 32]) -> Result<(), SessionEntropyUnavailable>;

#[derive(Clone)]
struct ApplyingInputs {
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    fill_secret: FillSessionSecret,
}

/// The shared dispatch table installed by Central's organization hosts.
pub struct OrganizationLinkDispatcher<Transactions, Approvals> {
    transactions: Transactions,
    inputs: ApplyingInputs,
    approvals: ApprovalLinkApplication<Approvals>,
    ending_transport: Arc<dyn crate::ApprovalDeliveryTransport>,
}

impl<Transactions: OrganizationSessionTransactions, Approvals>
    OrganizationLinkDispatcher<Transactions, Approvals>
{
    pub fn new(
        transactions: Transactions,
        approval_transactions: Approvals,
        ending_transport: Arc<dyn crate::ApprovalDeliveryTransport>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        fill_secret: FillSessionSecret,
    ) -> Self {
        Self {
            transactions,
            ending_transport,
            approvals: ApprovalLinkApplication {
                transactions: Arc::new(approval_transactions),
                clock: clock.clone(),
                ids: ids.clone(),
            },
            inputs: ApplyingInputs {
                clock,
                ids,
                fill_secret,
            },
        }
    }

    fn approval_delivery(&self) -> ApprovalDelivery<Approvals> {
        ApprovalDelivery {
            application: self.approvals.clone(),
            transport: self.ending_transport.clone(),
        }
    }
}

impl<Transactions, Approvals, Db> OrganizationLinkDispatcher<Transactions, Approvals>
where
    Transactions: OrganizationSessionTransactions,
    Approvals: Send + Sync,
    for<'loan> Transactions::Transaction<'loan>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    fn apply<'call, R>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: Envelope<R::Target, R::Request>,
        dispatch: impl for<'loan, 'connection> FnOnce(
            TargetCheckedCentralCall<R>,
            &'loan mut Transactions::Transaction<'connection>,
            ValidatedMachineConnection,
            ApplyingInputs,
            EngineTime,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> Pin<Box<dyn Future<Output = CentralCallDispatch> + Send + 'call>>
    where
        R: CentralLinkRpc<Target = OrganizationId> + 'static,
    {
        Box::pin(async move {
            let request =
                match TargetCheckedCentralCall::<R>::check(machine.organization_id(), request) {
                    Ok(request) => request,
                    Err(reason) => return Ok(TypedCallResult::Refused { reason }),
                };
            let machine = machine.clone();
            let inputs = self.inputs.clone();
            let result =
                self.transactions
                    .apply(machine.organization_id().clone(), move |loan| {
                        Box::pin(async move {
                            let (time, lapse) = recheck_lent_link_authority(
                                loan,
                                machine.attachment_ref(),
                                inputs.clock.as_ref(),
                            )
                            .await?;
                            if let Some(cause) = lapse {
                                return Ok(SessionApplySuccess::AuthorityLapsed(cause));
                            }
                            let time = EngineTime::from_timestamp_millis(time.unix_millis())
                                .ok_or(SessionApplyFailure::Terminal(
                                    SessionApplyFault::ApplyingClockOutOfRange,
                                ))?;
                            dispatch(request, loan, machine, inputs, time).await
                        })
                    })
                    .await;
            report_application(result)
        })
    }
}

impl<Transactions, Approvals, Db> CentralLinkDispatcher
    for OrganizationLinkDispatcher<Transactions, Approvals>
where
    Transactions: OrganizationSessionTransactions,
    Approvals: OrganizationApprovalTransactions,
    for<'loan> Approvals::Transaction<'loan>: CentralTransaction<Database = Db>,
    for<'loan> Transactions::Transaction<'loan>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    #[cfg(feature = "test-support")]
    fn fixture_decide_approval(
        &self,
        organization: OrganizationId,
        decision: collaboration_central_core::FixtureApprovalDecision,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<collaboration_central_core::LinkCallCompletion>,
                        StoreFailure,
                    >,
                > + Send
                + '_,
        >,
    > {
        self.approval_delivery()
            .fixture_decide(organization, decision)
    }

    fn recover_approval_endings(
        &self,
        machine: ValidatedMachineConnection,
        run: workspace_ids::RouterRunId,
    ) -> Option<collaboration_central_core::LinkCallCompletion> {
        self.approval_delivery().after_reply(
            fleet_approvals::CommittedApprovalEffects::default(),
            Some((machine, run)),
        )
    }

    fn end_approval_enrollment(
        &self,
        organization: OrganizationId,
        invalidation: fleet_contracts::EnrollmentInvalidation,
        statements: Vec<central_storage::GuardedStatement>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreFailure>> + Send + '_>> {
        Box::pin(
            self.approvals
                .end_enrollment(organization, invalidation, statements),
        )
    }

    fn approval_ending_is_owed<'call>(
        &'call self,
        holder: &'call central_link_protocol::approval_rpc::ApprovalHolderTarget,
        request: &'call workspace_ids::ApprovalRequestId,
    ) -> Pin<Box<dyn Future<Output = Result<bool, StoreFailure>> + Send + 'call>> {
        let delivery = self.approval_delivery();
        Box::pin(async move { delivery.ending_is_owed(holder, request).await })
    }

    fn approval_notice_deadline<'call>(
        &'call self,
        notice: &'call central_link_protocol::approval_rpc::ApprovalNoticeEnvelope,
    ) -> Pin<Box<dyn Future<Output = Result<Option<EngineTime>, StoreFailure>> + Send + 'call>>
    {
        let delivery = self.approval_delivery();
        Box::pin(async move { delivery.notice_deadline(notice).await })
    }

    fn recover_approval_notices(
        &self,
        machine: ValidatedMachineConnection,
    ) -> collaboration_central_core::LinkCallCompletion {
        self.approval_delivery()
            .recover_notices_after_announcement(machine)
    }

    fn apply_approval_deadline(
        &self,
        organization: OrganizationId,
        deadline: central_engine_model::DueRow,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        collaboration_central_core::AppliedApprovalDeadline,
                        fleet_engine::CheckedDueFailure,
                    >,
                > + Send
                + '_,
        >,
    > {
        self.approval_delivery()
            .apply_deadline(organization, deadline)
    }

    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
        carrier: TypedCallCarrier,
    ) -> Pin<Box<dyn Future<Output = Result<CentralCallWork, StoreFailure>> + Send + 'call>> {
        Box::pin(async move {
            match request {
                CentralCall::CreateFleetSession(request) => self
                    .apply::<CreateFleetSessionRpc>(
                        machine,
                        request,
                        |request, loan, machine, inputs, time| {
                            Box::pin(async move {
                                dispatch_fallible_correlated_central_call(request, |body| async {
                                    let mut ids = ApplyingIds(inputs.ids.as_ref());
                                    let mut context = Context {
                                        now: time,
                                        ids: &mut ids,
                                    };
                                    let mut service = SessionService::in_transaction(loan);
                                    session_reply(
                                        service
                                            .create(
                                                &machine,
                                                body,
                                                &mut context,
                                                &mut ApplyingSecrets(inputs.fill_secret),
                                            )
                                            .await,
                                    )
                                })
                                .await
                                .map(SessionApplySuccess::Answered)
                            })
                        },
                    )
                    .await
                    .map(CentralCallWork::from),
                CentralCall::BindFleetSession(request) => self
                    .apply::<BindFleetSessionRpc>(
                        machine,
                        request,
                        |request, loan, machine, inputs, time| {
                            Box::pin(async move {
                                dispatch_fallible_central_call(request, |body| async {
                                    let mut ids = ApplyingIds(inputs.ids.as_ref());
                                    let mut context = Context {
                                        now: time,
                                        ids: &mut ids,
                                    };
                                    let mut service = SessionService::in_transaction(loan);
                                    session_reply(service.bind(&machine, body, &mut context).await)
                                })
                                .await
                                .map(SessionApplySuccess::Answered)
                            })
                        },
                    )
                    .await
                    .map(CentralCallWork::from),
                CentralCall::EndFleetSession(request) => {
                    self.approval_delivery().end_session(machine, request).await
                }
                CentralCall::ListApprovalsForSession(request) => {
                    dispatch_parent_approval(
                        &self.approval_delivery(),
                        machine,
                        ParentApprovalCall::new(request, ParentApprovalOperation::List),
                    )
                    .await
                }
                CentralCall::ReadApprovalForSession(request) => {
                    dispatch_parent_approval(
                        &self.approval_delivery(),
                        machine,
                        ParentApprovalCall::new(request, ParentApprovalOperation::Read),
                    )
                    .await
                }
                CentralCall::DecideApprovalAsSession(request) => {
                    dispatch_parent_approval(
                        &self.approval_delivery(),
                        machine,
                        ParentApprovalCall::new(request, ParentApprovalOperation::Decide),
                    )
                    .await
                }
                CentralCall::PassOnApproval(request) => {
                    dispatch_parent_approval(
                        &self.approval_delivery(),
                        machine,
                        ParentApprovalCall::new(request, ParentApprovalOperation::PassOn),
                    )
                    .await
                }
                CentralCall::RegisterRun(request) => {
                    let run = request.body.router_run.clone();
                    self.approval_delivery()
                        .dispatch_holder::<RegisterRunRpc>(
                            machine,
                            request,
                            (run, &carrier),
                            |_| HolderCommand::Register,
                            |target, reply| {
                                if let HolderReply::Register(body) = reply {
                                    Some(RegisterRunRpc::reply(Envelope { target, body }))
                                } else {
                                    None
                                }
                            },
                        )
                        .await
                }
                CentralCall::Raise(request) => {
                    let run = request.body.router_run().clone();
                    self.approval_delivery()
                        .dispatch_holder::<RaiseApprovalRpc>(
                            machine,
                            request,
                            (run, &carrier),
                            |body| HolderCommand::Raise(Box::new(body)),
                            |target, reply| {
                                if let HolderReply::Raise(body) = reply {
                                    Some(RaiseApprovalRpc::reply(Envelope { target, body }))
                                } else {
                                    None
                                }
                            },
                        )
                        .await
                }
                CentralCall::Stop(request) => {
                    use approval_request_protocol::StopRequest;
                    let run = match &request.body {
                        StopRequest::Provider { router_run, .. }
                        | StopRequest::UnprovenCall { router_run, .. } => router_run.clone(),
                    };
                    self.approval_delivery()
                        .dispatch_holder::<StopApprovalRpc>(
                            machine,
                            request,
                            (run, &carrier),
                            HolderCommand::Stop,
                            |target, reply| {
                                if let HolderReply::Stop(body) = reply {
                                    Some(StopApprovalRpc::reply(Envelope { target, body }))
                                } else {
                                    None
                                }
                            },
                        )
                        .await
                }
            }
        })
    }
}

struct ApplyingIds<'input>(&'input dyn IdSource);

impl central_engine_model::IdSource for ApplyingIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

struct ApplyingSecrets(FillSessionSecret);

impl SessionSecretSource for ApplyingSecrets {
    fn new_session_secret(&mut self) -> Result<SessionSecret, SessionServiceError> {
        let mut bytes = Zeroizing::new([0; 32]);
        (self.0)(&mut bytes).map_err(|_| SessionServiceError::SecretUnavailable)?;
        Ok(SessionSecret::from_bytes(&bytes))
    }
}

fn session_reply<Reply>(
    outcome: Result<SessionCallOutcome<Reply>, SessionServiceError>,
) -> Result<Reply, SessionApplyFailure> {
    match outcome? {
        SessionCallOutcome::Answered(reply) => Ok(reply),
        SessionCallOutcome::Revoked => Err(SessionApplyFailure::Terminal(
            SessionApplyFault::InconsistentEnrollment,
        )),
    }
}

fn report_application(result: SessionApplyResult) -> CentralCallDispatch {
    match result {
        Ok(SessionApplySuccess::Answered(reply)) => Ok(TypedCallResult::Answered { reply }),
        Ok(SessionApplySuccess::AuthorityLapsed(cause)) => Ok(TypedCallResult::Refused {
            reason: LinkCallRefusal::AuthorityLapsed { cause },
        }),
        Err(SessionApplyFailure::Store(failure)) => Err(failure),
        Err(failure @ SessionApplyFailure::EntropyUnavailable) => {
            tracing::warn!(?failure, "session call could not get secure entropy");
            Err(StoreFailure::Unavailable)
        }
        Err(SessionApplyFailure::Wiring(fault)) => {
            tracing::error!(?fault, "session transaction owner did not match admission");
            Err(fleet_runtime_ports::workspace_decode_error::corrupt(
                "signin_organization_identity.id",
            ))
        }
        Err(SessionApplyFailure::Terminal(fault)) => {
            tracing::error!(?fault, "session call rolled back");
            Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::CentralFault,
            })
        }
    }
}
