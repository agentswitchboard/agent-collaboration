use crate::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFailure, SessionApplyFault,
    SessionApplyFuture, SessionApplyResult,
};
use central_engine_model::{Context, EngineTime};
use central_link_protocol::{
    CentralCall, CentralLinkRpc, LinkCallRefusal, TypedCallResult,
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallDispatch, CentralLinkDispatcher, TargetCheckedCentralCall,
    ValidatedMachineConnection, dispatch_fallible_central_call,
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

type FillSessionSecret = fn(&mut [u8; 32]) -> Result<(), SessionEntropyUnavailable>;

#[derive(Clone)]
struct ApplyingInputs {
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    fill_secret: FillSessionSecret,
}

/// The shared dispatch table installed by Central's organization hosts.
pub struct SessionLinkDispatcher<Transactions> {
    transactions: Transactions,
    inputs: ApplyingInputs,
}

impl<Transactions: OrganizationSessionTransactions> SessionLinkDispatcher<Transactions> {
    pub fn new(
        transactions: Transactions,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        fill_secret: FillSessionSecret,
    ) -> Self {
        Self {
            transactions,
            inputs: ApplyingInputs {
                clock,
                ids,
                fill_secret,
            },
        }
    }
}

impl<Transactions, Db> SessionLinkDispatcher<Transactions>
where
    Transactions: OrganizationSessionTransactions,
    for<'loan> Transactions::Transaction<'loan>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    async fn apply<R>(
        &self,
        machine: &ValidatedMachineConnection,
        request: Envelope<R::Target, R::Request>,
        dispatch: impl for<'loan> FnOnce(
            TargetCheckedCentralCall<R>,
            OrganizationLoan<'loan, Transactions::Transaction<'loan>>,
            ValidatedMachineConnection,
            ApplyingInputs,
            EngineTime,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> CentralCallDispatch
    where
        R: CentralLinkRpc<Target = OrganizationId> + 'static,
    {
        let request = match TargetCheckedCentralCall::<R>::check(machine.organization_id(), request)
        {
            Ok(request) => request,
            Err(reason) => return Ok(TypedCallResult::Refused { reason }),
        };
        let machine = machine.clone();
        let inputs = self.inputs.clone();
        let result = self
            .transactions
            .apply(move |mut loan| {
                Box::pin(async move {
                    let (time, lapse) = recheck_lent_link_authority(
                        &mut loan,
                        machine.attachment_ref(),
                        inputs.clock.as_ref(),
                    )
                    .await?;
                    if let Some(cause) = lapse {
                        return Ok(TypedCallResult::Refused {
                            reason: LinkCallRefusal::AuthorityLapsed { cause },
                        });
                    }
                    let time = EngineTime::from_timestamp_millis(time.unix_millis()).ok_or(
                        SessionApplyFailure::Terminal(SessionApplyFault::ApplyingClockOutOfRange),
                    )?;
                    dispatch(request, loan, machine, inputs, time).await
                })
            })
            .await;
        report_application(result)
    }
}

impl<Transactions, Db> CentralLinkDispatcher for SessionLinkDispatcher<Transactions>
where
    Transactions: OrganizationSessionTransactions,
    for<'loan> Transactions::Transaction<'loan>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = CentralCallDispatch> + Send + 'call>> {
        Box::pin(async move {
            match request {
                CentralCall::CreateFleetSession(request) => {
                    self.apply::<CreateFleetSessionRpc>(
                        machine,
                        request,
                        |request, mut loan, machine, inputs, time| {
                            Box::pin(async move {
                                dispatch_fallible_correlated_central_call(request, |body| async {
                                    let mut ids = ApplyingIds(inputs.ids.as_ref());
                                    let mut context = Context {
                                        now: time,
                                        ids: &mut ids,
                                    };
                                    let mut service = SessionService::in_transaction(&mut loan);
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
                            })
                        },
                    )
                    .await
                }
                CentralCall::BindFleetSession(request) => {
                    self.apply::<BindFleetSessionRpc>(
                        machine,
                        request,
                        |request, mut loan, machine, inputs, time| {
                            Box::pin(async move {
                                dispatch_fallible_central_call(request, |body| async {
                                    let mut ids = ApplyingIds(inputs.ids.as_ref());
                                    let mut context = Context {
                                        now: time,
                                        ids: &mut ids,
                                    };
                                    let mut service = SessionService::in_transaction(&mut loan);
                                    session_reply(service.bind(&machine, body, &mut context).await)
                                })
                                .await
                            })
                        },
                    )
                    .await
                }
                CentralCall::EndFleetSession(request) => {
                    self.apply::<EndFleetSessionRpc>(
                        machine,
                        request,
                        |request, mut loan, machine, inputs, time| {
                            Box::pin(async move {
                                dispatch_fallible_correlated_central_call(request, |body| async {
                                    let mut ids = ApplyingIds(inputs.ids.as_ref());
                                    let mut context = Context {
                                        now: time,
                                        ids: &mut ids,
                                    };
                                    let mut service = SessionService::in_transaction(&mut loan);
                                    session_reply(service.end(&machine, body, &mut context).await)
                                })
                                .await
                            })
                        },
                    )
                    .await
                }
                CentralCall::RegisterRun(_) | CentralCall::Raise(_) | CentralCall::Stop(_) => {
                    Ok(TypedCallResult::Refused {
                        reason: LinkCallRefusal::UnknownCall,
                    })
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
        Ok(result) => Ok(result),
        Err(SessionApplyFailure::Store(failure)) => Err(failure),
        Err(failure @ SessionApplyFailure::EntropyUnavailable) => {
            tracing::warn!(?failure, "session call could not get secure entropy");
            Err(StoreFailure::Unavailable)
        }
        Err(SessionApplyFailure::Terminal(fault)) => {
            tracing::error!(?fault, "session call rolled back");
            Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::CentralFault,
            })
        }
    }
}
