use crate::{
    OrganizationApprovalTransactions,
    approval_dispatch::{
        ApprovalLinkApplication, HolderApplied, HolderCommand, HolderReply, refused,
        report_approval_failure,
    },
};
use approval_request_protocol::{
    DeliverEndingRequest, RaiseReply, RegisterRunReply, RouterRunId, RunRefusal, StopReply,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, LinkCallRefusal, TypedCallResult,
    approval_rpc::{ApprovalHolderTarget, ApprovalServiceTarget, DeliverEndingRpc},
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallWork, LinkCallCompletion, MacCallResult, TargetCheckedCentralCall, TypedCallCarrier,
    ValidatedMachineConnection,
};
use fleet_approvals::{
    ApprovalHolderAdmission, CommittedApprovalEffects, CommittedApprovalNotice,
    CommittedApprovalOffer,
};
use fleet_lent_transaction::CentralTransaction;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use futures_util::future::select;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{future::Future, pin::Pin, sync::Arc};

type EndingFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub type ApplyEndingAnswer =
    Box<dyn FnOnce(ValidatedMachineConnection) -> LinkCallCompletion + Send>;

pub struct SelectedApprovalSocket {
    pub machine: ValidatedMachineConnection,
    pub ended: LinkCallCompletion,
    pub apply_answer: Box<dyn FnOnce(ApplyEndingAnswer) -> LinkCallCompletion + Send>,
    pub send: Box<
        dyn FnOnce(Box<DeliverEndingRequest>) -> EndingFuture<MacCallResult<DeliverEndingRpc>>
            + Send,
    >,
}

/// Select a physical socket once. Its send capability cannot route through a replacement.
pub trait ApprovalDeliveryTransport: Send + Sync {
    fn offer_notice(
        &self,
        notice: CommittedApprovalNotice,
    ) -> EndingFuture<Result<(), CallFailure>>;

    #[cfg(feature = "test-support")]
    fn fixture_delivery_answer_applied(&self) -> LinkCallCompletion {
        Box::pin(async {})
    }

    fn select(
        &self,
        target: ApprovalHolderTarget,
    ) -> EndingFuture<Result<SelectedApprovalSocket, CallFailure>>;
}

pub(crate) struct ApprovalDelivery<Transactions> {
    pub application: ApprovalLinkApplication<Transactions>,
    pub transport: Arc<dyn ApprovalDeliveryTransport>,
}

impl<Transactions> Clone for ApprovalDelivery<Transactions> {
    fn clone(&self) -> Self {
        Self {
            application: self.application.clone(),
            transport: self.transport.clone(),
        }
    }
}

impl<Transactions, Db> ApprovalDelivery<Transactions>
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
    pub(crate) fn dispatch_holder<
        'call,
        R: CentralLinkRpc<Target = ApprovalServiceTarget> + 'static,
    >(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: Envelope<R::Target, R::Request>,
        (claimed_run, carrier): (RouterRunId, &'call TypedCallCarrier),
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
                .application
                .apply_holder(machine.clone(), run.clone(), command)
                .await;
            match applied {
                Ok(HolderApplied::Answer {
                    reply,
                    offers,
                    publication_failure,
                }) => {
                    let reply = wrap(target, reply).ok_or(StoreFailure::AnswerLost)?;
                    let registration = recover_after_registration(&reply, machine, run);
                    Ok(CentralCallWork {
                        result: match publication_failure {
                            None => Ok(TypedCallResult::Answered { reply }),
                            Some(failure) => Err(failure),
                        },
                        committed_delivery: self.after_reply(offers, None),
                        completion: self
                            .after_reply(CommittedApprovalEffects::default(), registration),
                    })
                }
                Ok(HolderApplied::AuthorityLapsed(cause)) => {
                    Ok(refused(LinkCallRefusal::AuthorityLapsed { cause }))
                }
                Err(failure) => report_approval_failure(failure),
            }
        })
    }

    pub fn after_reply(
        &self,
        offers: CommittedApprovalEffects,
        registration: Option<(ValidatedMachineConnection, RouterRunId)>,
    ) -> Option<LinkCallCompletion> {
        if offers.is_empty() && registration.is_none() {
            return None;
        }
        let delivery = self.clone();
        Some(Box::pin(async move {
            delivery.offer_all(offers).await;
            if let Some((machine, run)) = registration {
                match delivery
                    .application
                    .apply_holder(machine, run, HolderCommand::Recover)
                    .await
                {
                    Ok(HolderApplied::Answer {
                        reply: HolderReply::Recover(Ok(())),
                        offers,
                        ..
                    }) => delivery.offer_all(offers).await,
                    Ok(HolderApplied::Answer {
                        reply: HolderReply::Recover(Err(refusal)),
                        ..
                    }) => tracing::debug!(
                        ?refusal,
                        "registered holder no longer has approval authority"
                    ),
                    Ok(HolderApplied::Answer {
                        reply:
                            HolderReply::Register(_)
                            | HolderReply::Raise(_)
                            | HolderReply::Stop(_)
                            | HolderReply::DeliveryAnswer,
                        ..
                    }) => tracing::error!("approval recovery returned the wrong reply"),
                    Ok(HolderApplied::AuthorityLapsed(cause)) => {
                        tracing::debug!(?cause, "registered approval link authority lapsed")
                    }
                    Err(failure) => tracing::warn!(?failure, "approval ending recovery failed"),
                }
            }
        }))
    }

    pub fn offer_all<'call>(
        &'call self,
        offers: CommittedApprovalEffects,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'call>> {
        Box::pin(async move {
            for offer in offers.endings {
                self.offer_one(offer).await;
            }
            for notice in offers.notices {
                if let Err(failure) = self.transport.offer_notice(notice).await {
                    tracing::debug!(?failure, "approval notice was not acknowledged");
                }
            }
        })
    }

    async fn offer_one(&self, offer: CommittedApprovalOffer) {
        let holder = offer.holder_address();
        let target = ApprovalHolderTarget {
            organization_id: holder.organization_id.clone(),
            machine_id: holder.run.enrollment.machine_id.clone(),
            enrollment_id: holder.run.enrollment.enrollment_id.clone(),
            router_run: holder.run.router_run.clone(),
        };
        let run = target.router_run.clone();
        let selected = match self.transport.select(target).await {
            Ok(selected) => selected,
            Err(failure @ (CallFailure::NeverReached | CallFailure::AnswerLost)) => {
                tracing::debug!(?failure, "approval ending socket selection failed");
                return;
            }
        };
        let admission = ApprovalHolderAdmission::for_connection(&selected.machine, run.clone());
        let request = match offer.take_for_holder(&admission) {
            Ok(request) => request,
            Err(failure) => {
                tracing::error!(?failure, "approval ending selected a different holder");
                return;
            }
        };
        let answer = match (selected.send)(request.clone()).await {
            Ok(TypedCallResult::Answered { reply }) => reply.body,
            Ok(TypedCallResult::Refused {
                reason:
                    reason @ (LinkCallRefusal::UnknownCall
                    | LinkCallRefusal::WrongTarget
                    | LinkCallRefusal::SessionProofNotAccepted
                    | LinkCallRefusal::CentralFault
                    | LinkCallRefusal::AuthorityLapsed { .. }),
            }) => {
                tracing::debug!(?reason, "approval ending was refused by the link");
                return;
            }
            Err(failure @ (CallFailure::NeverReached | CallFailure::AnswerLost)) => {
                tracing::debug!(?failure, "approval ending was not acknowledged");
                return;
            }
        };
        let delivery = self.clone();
        let apply_answer = (selected.apply_answer)(Box::new(move |current| {
            Box::pin(async move {
                delivery
                    .apply_correlated_answer(current, run, request, answer)
                    .await;
            })
        }));
        let _ = select(selected.ended, apply_answer).await;
    }

    async fn apply_correlated_answer(
        &self,
        machine: ValidatedMachineConnection,
        run: RouterRunId,
        request: Box<DeliverEndingRequest>,
        answer: approval_request_protocol::DeliverEndingReply,
    ) {
        let apply = self.application.apply_holder(
            machine,
            run,
            HolderCommand::DeliveryAnswer {
                request,
                reply: answer,
            },
        );
        match apply.await {
            Ok(HolderApplied::Answer {
                reply: HolderReply::DeliveryAnswer,
                ..
            }) => {
                #[cfg(feature = "test-support")]
                self.transport.fixture_delivery_answer_applied().await;
            }
            Err(failure) => {
                tracing::warn!(?failure, "could not apply the approval ending answer");
            }
            Ok(HolderApplied::AuthorityLapsed(cause)) => {
                tracing::debug!(?cause, "approval ending answer lost link authority");
            }
            Ok(HolderApplied::Answer {
                reply:
                    HolderReply::Register(_)
                    | HolderReply::Raise(_)
                    | HolderReply::Stop(_)
                    | HolderReply::Recover(_),
                ..
            }) => tracing::error!("approval ending application returned the wrong reply"),
        }
    }
}

pub(crate) fn recover_after_registration(
    reply: &central_link_protocol::CentralCallReply,
    machine: &ValidatedMachineConnection,
    run: RouterRunId,
) -> Option<(ValidatedMachineConnection, RouterRunId)> {
    if matches!(reply, central_link_protocol::CentralCallReply::RegisterRun(answer)
        if matches!(answer.body, RegisterRunReply::Registered {}))
    {
        Some((machine.clone(), run))
    } else {
        None
    }
}
