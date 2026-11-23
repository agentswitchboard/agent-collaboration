use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFailure, ApprovalTransactionFuture,
    OrganizationApprovalTransactions, OrganizationLoanFailure,
    approval_transaction::commit_approval,
};
use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Raise, RaiseReply, RegisterRunReply, RouterRunId,
    RunRefusal, StopReply, StopRequest,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, LinkCallRefusal, MachineLinkCloseCode, TypedCallResult,
    approval_rpc::ApprovalServiceTarget,
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallWork, TargetCheckedCentralCall, TypedCallCarrier, ValidatedMachineConnection,
    recheck_lent_link_authority,
};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyFailure, ApprovalApplyingStore, ApprovalCommitScope,
    ApprovalHolderAdmission, ApprovalRefusal, ApprovalReleaseFailure, ApprovalReplyAddress,
    CommittedApprovalOffer, PendingApprovalHolderEffects, PendingApprovalReply,
};
use fleet_lent_transaction::CentralTransaction;
use fleet_rpc::Envelope;
use fleet_runtime_ports::{Clock, IdSource, SyncTime, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{future::Future, pin::Pin, sync::Arc};

pub(crate) struct ApprovalLinkApplication<Transactions> {
    pub transactions: Arc<Transactions>,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
}

impl<T> Clone for ApprovalLinkApplication<T> {
    fn clone(&self) -> Self {
        Self {
            transactions: self.transactions.clone(),
            clock: self.clock.clone(),
            ids: self.ids.clone(),
        }
    }
}

pub(crate) type ApprovalStore<'loan, Transaction> =
    ApprovalApplyingStore<'loan, Transaction, ActorApprovalSessionFacts>;

pub(crate) enum HolderCommand {
    Register,
    Raise(Box<Raise>),
    Stop(StopRequest),
    DeliveryAnswer {
        request: Box<DeliverEndingRequest>,
        reply: DeliverEndingReply,
    },
    Recover,
}

pub(crate) enum HolderReply {
    Register(RegisterRunReply),
    Raise(RaiseReply),
    Stop(StopReply),
    DeliveryAnswer,
    Recover(Result<(), RunRefusal>),
}

enum PendingHolderReply {
    Register(PendingApprovalReply<RegisterRunReply>),
    Raise(PendingApprovalReply<RaiseReply>),
    Stop(PendingApprovalReply<StopReply>),
    DeliveryAnswer(PendingApprovalReply<()>),
    Recover(PendingApprovalReply<Result<(), RunRefusal>>),
}

impl PendingHolderReply {
    fn release(
        self,
        proof: &fleet_approvals::ApprovalCommitProof,
        address: &ApprovalReplyAddress,
    ) -> Result<HolderReply, ApprovalReleaseFailure> {
        match self {
            Self::Register(reply) => reply
                .take_for_recipient(proof, address)
                .map(HolderReply::Register),
            Self::Raise(reply) => reply
                .take_for_recipient(proof, address)
                .map(HolderReply::Raise),
            Self::Stop(reply) => reply
                .take_for_recipient(proof, address)
                .map(HolderReply::Stop),
            Self::DeliveryAnswer(reply) => reply
                .take_for_recipient(proof, address)
                .map(|()| HolderReply::DeliveryAnswer),
            Self::Recover(reply) => reply
                .take_for_recipient(proof, address)
                .map(HolderReply::Recover),
        }
    }
}

enum PendingHolderApply {
    Applied {
        reply: Box<PendingHolderReply>,
        address: ApprovalReplyAddress,
        effects: PendingApprovalHolderEffects,
    },
    AuthorityLapsed(MachineLinkCloseCode),
}

pub(crate) enum HolderApplied {
    Answer {
        reply: HolderReply,
        offers: Vec<CommittedApprovalOffer>,
    },
    AuthorityLapsed(MachineLinkCloseCode),
}

#[derive(Debug)]
pub(crate) enum ApprovalDispatchFailure {
    Transaction(ApprovalTransactionFailure),
    Release(ApprovalReleaseFailure),
}

impl<Transactions, Db> ApprovalLinkApplication<Transactions>
where
    Transactions: OrganizationApprovalTransactions,
    for<'connection> Transactions::Transaction<'connection>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    pub fn dispatch<'call, R: CentralLinkRpc<Target = ApprovalServiceTarget> + 'static>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: Envelope<R::Target, R::Request>,
        (claimed_run, carrier): (RouterRunId, &'call TypedCallCarrier),
        transport: Arc<dyn crate::ApprovalEndingTransport>,
        command: impl FnOnce(R::Request) -> HolderCommand + Send + 'static,
        wrap: impl FnOnce(ApprovalServiceTarget, HolderReply) -> Option<CentralCallReply>
        + Send
        + 'static,
    ) -> Pin<Box<dyn Future<Output = Result<CentralCallWork, StoreFailure>> + Send + 'call>> {
        Box::pin(async move {
            let target = ApprovalServiceTarget {
                organization_id: machine.organization_id().clone(),
            };
            let body = match TargetCheckedCentralCall::<R>::check(&target, request) {
                Ok(checked) => checked.into_body(),
                Err(reason) => return Ok(refused(reason)),
            };
            let command = command(body);
            let TypedCallCarrier::Approval { registered_run } = carrier else {
                return Err(corrupt("typed_approval_call.physical_carrier"));
            };
            let run = match (&command, registered_run) {
                (HolderCommand::Register, None) => claimed_run,
                (_, Some(bound)) if *bound == claimed_run => bound.clone(),
                (HolderCommand::Register | HolderCommand::Raise(_) | HolderCommand::Stop(_), _) => {
                    let reply = match command {
                        HolderCommand::Register => {
                            HolderReply::Register(RegisterRunReply::RunNotRegistered {})
                        }
                        HolderCommand::Raise(_) => {
                            HolderReply::Raise(RaiseReply::AuthorityRefused {
                                refusal: RunRefusal::RunNotRegistered {},
                            })
                        }
                        HolderCommand::Stop(_) => HolderReply::Stop(StopReply::AuthorityRefused {
                            refusal: RunRefusal::RunNotRegistered {},
                        }),
                        HolderCommand::Recover | HolderCommand::DeliveryAnswer { .. } => {
                            return Err(corrupt("typed_approval_call.direction"));
                        }
                    };
                    return Ok(TypedCallResult::Answered {
                        reply: wrap(target, reply).ok_or(StoreFailure::Unavailable)?,
                    }
                    .into());
                }
                (HolderCommand::Recover | HolderCommand::DeliveryAnswer { .. }, _) => {
                    return Err(corrupt("typed_approval_call.direction"));
                }
            };
            let applied = self
                .apply_holder(machine.clone(), run.clone(), command)
                .await;
            match applied {
                Ok(HolderApplied::Answer { reply, offers }) => {
                    let reply = wrap(target, reply).ok_or(StoreFailure::AnswerLost)?;
                    let registration =
                        crate::approval_delivery::recover_after_registration(&reply, machine, run);
                    let delivery = crate::approval_delivery::ApprovalDelivery {
                        application: self.clone(),
                        transport,
                    };
                    Ok(CentralCallWork {
                        result: TypedCallResult::Answered { reply },
                        completion: delivery.after_reply(offers, registration),
                    })
                }
                Ok(HolderApplied::AuthorityLapsed(cause)) => {
                    Ok(refused(LinkCallRefusal::AuthorityLapsed { cause }))
                }
                Err(failure) => report_approval_failure(failure),
            }
        })
    }

    pub fn apply_holder<'call>(
        &'call self,
        machine: ValidatedMachineConnection,
        run: RouterRunId,
        command: HolderCommand,
    ) -> Pin<Box<dyn Future<Output = Result<HolderApplied, ApprovalDispatchFailure>> + Send + 'call>>
    {
        Box::pin(async move {
            let organization = machine.organization_id().clone();
            let clock = self.clock.clone();
            let ids = self.ids.clone();
            let (proof, applied) = commit_approval(
                self.transactions.clone(),
                organization,
                ApprovalAlarmPublication::WithCommit,
                move |loan, scope| {
                    apply_checked_holder(loan, scope, machine, run, clock, ids, command)
                },
            )
            .await
            .map_err(ApprovalDispatchFailure::Transaction)?;
            match applied {
                PendingHolderApply::AuthorityLapsed(cause) => {
                    Ok(HolderApplied::AuthorityLapsed(cause))
                }
                PendingHolderApply::Applied {
                    reply,
                    address,
                    effects,
                } => Ok(HolderApplied::Answer {
                    reply: reply
                        .release(&proof, &address)
                        .map_err(ApprovalDispatchFailure::Release)?,
                    offers: effects
                        .release(&proof)
                        .map_err(ApprovalDispatchFailure::Release)?,
                }),
            }
        })
    }
}

fn apply_checked_holder<'loan, Transaction, Db>(
    loan: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    machine: ValidatedMachineConnection,
    run: RouterRunId,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    command: HolderCommand,
) -> ApprovalTransactionFuture<'loan, PendingHolderApply>
where
    Transaction: fleet_approvals::ApprovalStoreTransaction<Database = Db> + 'loan,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Box::pin(async move {
        let (applying_at, lapse) =
            recheck_lent_link_authority(loan, machine.attachment_ref(), clock.as_ref()).await?;
        if let Some(cause) = lapse {
            return Ok(PendingHolderApply::AuthorityLapsed(cause));
        }
        let holder = ApprovalHolderAdmission::for_connection(&machine, run);
        let address = ApprovalReplyAddress::Holder(holder.holder_scope().clone());
        let applying_clock = ApprovalApplyingClock(
            crate::approval_applying_time::observe_applying_time(loan, applying_at).await?,
        );
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalApplyingStore::new(
            &scope,
            loan,
            &ActorApprovalSessionFacts,
            &applying_clock,
            &mut row_ids,
        );
        let reply = match command {
            HolderCommand::Register => {
                PendingHolderReply::Register(store.register_approval_run(&holder).await?)
            }
            HolderCommand::Raise(raise) => {
                PendingHolderReply::Raise(store.raise_from_holder(&holder, raise).await?)
            }
            HolderCommand::Stop(stop) => {
                PendingHolderReply::Stop(store.stop_from_holder(&holder, stop).await?)
            }
            HolderCommand::DeliveryAnswer { request, reply } => PendingHolderReply::DeliveryAnswer(
                store
                    .record_holder_delivery(&holder, request, reply)
                    .await?,
            ),
            HolderCommand::Recover => {
                PendingHolderReply::Recover(store.recover_owed_approval_endings(&holder).await?)
            }
        };
        let effects = store.finish_holder_effects();
        Ok(PendingHolderApply::Applied {
            reply: Box::new(reply),
            address,
            effects,
        })
    })
}

pub(crate) struct ApprovalApplyingClock(pub SyncTime);

impl Clock for ApprovalApplyingClock {
    fn now(&self) -> SyncTime {
        self.0
    }
}

pub(crate) struct ApprovalApplyingIds<'clock>(pub &'clock dyn IdSource);

impl central_engine_model::IdSource for ApprovalApplyingIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

fn refused(reason: LinkCallRefusal) -> CentralCallWork {
    TypedCallResult::Refused { reason }.into()
}

pub(crate) fn report_approval_failure(
    failure: ApprovalDispatchFailure,
) -> Result<CentralCallWork, StoreFailure> {
    match failure {
        ApprovalDispatchFailure::Transaction(ApprovalTransactionFailure::Store(failure)) => {
            Err(failure)
        }
        ApprovalDispatchFailure::Transaction(ApprovalTransactionFailure::Loan(
            OrganizationLoanFailure::Store(failure),
        )) => Err(failure),
        ApprovalDispatchFailure::Transaction(ApprovalTransactionFailure::Loan(
            OrganizationLoanFailure::Wiring(fault),
        )) => {
            tracing::error!(?fault, "approval transaction owner did not match admission");
            Err(corrupt("signin_organization_identity.id"))
        }
        ApprovalDispatchFailure::Release(
            fault @ (ApprovalReleaseFailure::OtherCommit | ApprovalReleaseFailure::OtherRecipient),
        ) => {
            tracing::error!(?fault, "committed approval custody mismatch");
            Err(StoreFailure::AnswerLost)
        }
        ApprovalDispatchFailure::Transaction(ApprovalTransactionFailure::RolledBack(failure)) => {
            match failure {
                ApprovalApplyFailure::Store(
                    failure @ (StoreFailure::Unavailable
                    | StoreFailure::AnswerLost
                    | StoreFailure::AlarmUnpublishable { .. }),
                ) => Err(failure),
                terminal @ (ApprovalApplyFailure::GuardChanged
                | ApprovalApplyFailure::Store(StoreFailure::Corrupt { .. })
                | ApprovalApplyFailure::Lifecycle(
                    ApprovalRefusal::InvalidRetainedEnding
                    | ApprovalRefusal::InvalidDigest
                    | ApprovalRefusal::DeadlineOutOfRange
                    | ApprovalRefusal::ApplyingTimeOutOfRange
                    | ApprovalRefusal::InvalidInitialRoute,
                )) => {
                    tracing::error!(?terminal, "approval apply rolled back");
                    Ok(refused(LinkCallRefusal::CentralFault))
                }
            }
        }
    }
}
