use crate::{OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture};
use central_engine_model::{Context, EngineTime};
use central_link_protocol::{
    CentralCall, LinkCallRefusal, MachineLinkCloseCode, TypedCallResult,
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallDispatch, CentralLinkDispatcher, ValidatedMachineConnection,
    dispatch_fallible_central_call, dispatch_fallible_correlated_central_call,
};
use fleet_actor_identity::{
    SessionCallOutcome, SessionSecretSource, SessionService, SessionServiceError,
};
use fleet_identity_protocol::SessionSecret;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{Clock, IdSource};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::sync::Arc;
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

    async fn apply(
        &self,
        machine: &ValidatedMachineConnection,
        dispatch: impl for<'loan> FnOnce(
            OrganizationLoan<'loan, Transactions::Transaction<'loan>>,
            ValidatedMachineConnection,
            ApplyingInputs,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> CentralCallDispatch {
        let machine = machine.clone();
        let inputs = self.inputs.clone();
        self.transactions
            .apply(move |loan| dispatch(loan, machine, inputs))
            .await
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
    ) -> SessionApplyFuture<'call> {
        Box::pin(async move {
            match request {
                CentralCall::CreateFleetSession(request) => {
                    self.apply(machine, |mut loan, machine, inputs| Box::pin(async move {
                        dispatch_fallible_correlated_central_call::<CreateFleetSessionRpc, _, _>(
                            machine.organization_id(),
                            request,
                            |body| async {
                                let mut ids = ApplyingIds(inputs.ids.as_ref());
                                let mut context = inputs.context(&mut ids)?;
                                let mut service = SessionService::in_transaction(&mut loan);
                                session_reply(service.create(&machine, body, &mut context, &mut ApplyingSecrets(inputs.fill_secret)).await)
                            },
                        ).await
                    })).await
                }
                CentralCall::BindFleetSession(request) => {
                    self.apply(machine, |mut loan, machine, inputs| Box::pin(async move {
                        dispatch_fallible_central_call::<BindFleetSessionRpc, _, _>(
                            machine.organization_id(),
                            request,
                            |body| async {
                                let mut ids = ApplyingIds(inputs.ids.as_ref());
                                let mut context = inputs.context(&mut ids)?;
                                let mut service = SessionService::in_transaction(&mut loan);
                                session_reply(service.bind(&machine, body, &mut context).await)
                            },
                        ).await
                    })).await
                }
                CentralCall::EndFleetSession(request) => {
                    self.apply(machine, |mut loan, machine, inputs| Box::pin(async move {
                        dispatch_fallible_correlated_central_call::<EndFleetSessionRpc, _, _>(
                            machine.organization_id(),
                            request,
                            |body| async {
                                let mut ids = ApplyingIds(inputs.ids.as_ref());
                                let mut context = inputs.context(&mut ids)?;
                                let mut service = SessionService::in_transaction(&mut loan);
                                session_reply(service.end(&machine, body, &mut context).await)
                            },
                        ).await
                    })).await
                }
                CentralCall::RegisterRun(_) | CentralCall::Raise(_) | CentralCall::Stop(_) => {
                    Ok(TypedCallResult::Refused { reason: LinkCallRefusal::UnknownCall })
                }
                #[cfg(feature = "test-support")]
                CentralCall::CentralProbe(_) => {
                    Ok(TypedCallResult::Refused { reason: LinkCallRefusal::UnknownCall })
                }
            }
        })
    }
}

impl ApplyingInputs {
    fn context<'ids>(&self, ids: &'ids mut ApplyingIds<'_>) -> Result<Context<'ids>, StoreFailure> {
        let now = EngineTime::from_timestamp_millis(self.clock.now().unix_millis())
            .ok_or(StoreFailure::Unavailable)?;
        Ok(Context { now, ids })
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
) -> Result<Result<Reply, LinkCallRefusal>, StoreFailure> {
    match outcome {
        Ok(SessionCallOutcome::Answered(reply)) => Ok(Ok(reply)),
        // Core has already checked the detailed link cause. The session recheck
        // can observe an enrollment no longer current before the first write.
        Ok(SessionCallOutcome::Revoked) => Ok(Err(LinkCallRefusal::AuthorityLapsed {
            cause: MachineLinkCloseCode::Inactive,
        })),
        Err(SessionServiceError::Store(failure)) => Err(failure),
        Err(_) => Err(StoreFailure::Unavailable),
    }
}
