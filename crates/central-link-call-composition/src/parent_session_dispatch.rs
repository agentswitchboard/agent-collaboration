//! Admit a parent approval call using the exact session proof in the organization loan.
use std::sync::Arc;

use approval_request_protocol::{
    ApprovalSessionCall, DecideApprovalAsSessionReply, DecideApprovalAsSessionRequest,
    ListApprovalsForSessionReply, ListApprovalsForSessionRequest, PassOnApprovalReply,
    PassOnApprovalRequest, ReadApprovalForSessionReply, ReadApprovalForSessionRequest,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, LinkCallRefusal, TypedCallResult,
    approval_rpc::{
        ApprovalServiceTarget, DecideApprovalAsSessionRpc, ListApprovalsForSessionRpc,
        PassOnApprovalForSessionRpc, ReadApprovalForSessionRpc,
    },
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallWork, ValidatedMachineConnection, recheck_lent_link_authority,
};
use fleet_actor_identity::SessionService;
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalCommitProof, ApprovalCommitScope, ApprovalEnrollment,
    ApprovalReplyAddress, ApprovalSession, ApprovalStoreTransaction, PendingApprovalEffects,
    PendingApprovalReply, SessionDecisionApply, SessionReadApply,
};
use fleet_identity_protocol::{PresentedSessionSecret, SessionSecret};
use fleet_lent_transaction::CentralTransaction;
use fleet_rpc::Envelope;
use fleet_runtime_ports::{Clock, IdSource};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_ids::CentralSessionId;

use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFuture, OrganizationApprovalTransactions,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{
        ApprovalApplyingClock, ApprovalApplyingIds, ApprovalDispatchFailure, ApprovalStore,
        report_approval_failure,
    },
    approval_transaction::commit_approval_with_publication,
};

pub(crate) enum ParentApprovalOperation {
    List(ListApprovalsForSessionRequest),
    Read(ReadApprovalForSessionRequest),
    Decide(DecideApprovalAsSessionRequest),
    PassOn(PassOnApprovalRequest),
}

pub(crate) struct ParentApprovalCall {
    target: ApprovalServiceTarget,
    session: CentralSessionId,
    secret: SessionSecret,
    operation: ParentApprovalOperation,
}

impl ParentApprovalCall {
    pub(crate) fn new<Input>(
        request: Envelope<ApprovalServiceTarget, ApprovalSessionCall<SessionSecret, Input>>,
        operation: impl FnOnce(Input) -> ParentApprovalOperation,
    ) -> Self {
        Self {
            target: request.target,
            session: request.body.session_id,
            secret: request.body.secret,
            operation: operation(request.body.input),
        }
    }
}

enum ParentPendingReply {
    List(PendingApprovalReply<ListApprovalsForSessionReply>),
    Read(PendingApprovalReply<ReadApprovalForSessionReply>),
    Decide(PendingApprovalReply<DecideApprovalAsSessionReply>),
    PassOn(PendingApprovalReply<PassOnApprovalReply>),
}

enum ParentPendingApply {
    Refused(LinkCallRefusal),
    PersonRequired,
    Answer {
        address: ApprovalReplyAddress,
        reply: Box<ParentPendingReply>,
        effects: PendingApprovalEffects,
    },
}

impl ParentPendingReply {
    fn release(
        self,
        proof: &ApprovalCommitProof,
        address: &ApprovalReplyAddress,
        target: ApprovalServiceTarget,
    ) -> Result<CentralCallReply, StoreFailure> {
        match self {
            Self::List(reply) => reply
                .take_for_recipient(proof, address)
                .map(|body| ListApprovalsForSessionRpc::reply(Envelope { target, body })),
            Self::Read(reply) => reply
                .take_for_recipient(proof, address)
                .map(|body| ReadApprovalForSessionRpc::reply(Envelope { target, body })),
            Self::Decide(reply) => reply
                .take_for_recipient(proof, address)
                .map(|body| DecideApprovalAsSessionRpc::reply(Envelope { target, body })),
            Self::PassOn(reply) => reply
                .take_for_recipient(proof, address)
                .map(|body| PassOnApprovalForSessionRpc::reply(Envelope { target, body })),
        }
        .map_err(|_| StoreFailure::AnswerLost)
    }
}

pub(crate) async fn dispatch_parent_approval<Transactions, Db>(
    delivery: &ApprovalDelivery<Transactions>,
    machine: &ValidatedMachineConnection,
    call: ParentApprovalCall,
) -> Result<CentralCallWork, StoreFailure>
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
    let organization = machine.organization_id();
    if &call.target.organization_id != organization {
        return Ok(TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        }
        .into());
    }
    let machine = machine.clone();
    let clock = delivery.application.clock.clone();
    let ids = delivery.application.ids.clone();
    let target = call.target.clone();
    match commit_approval_with_publication(
        delivery.application.transactions.clone(),
        organization.clone(),
        ApprovalAlarmPublication::WithCommit,
        move |loan, scope| apply_parent(loan, scope, machine, call, clock, ids),
    )
    .await
    {
        Ok((
            _,
            crate::CommittedApprovalApply {
                output: ParentPendingApply::Refused(reason),
                ..
            },
        )) => Ok(TypedCallResult::Refused { reason }.into()),
        Ok((
            _,
            crate::CommittedApprovalApply {
                output: ParentPendingApply::PersonRequired,
                ..
            },
        )) => Ok(TypedCallResult::Answered {
            reply: DecideApprovalAsSessionRpc::reply(Envelope {
                target,
                body: DecideApprovalAsSessionReply::PersonRequired {},
            }),
        }
        .into()),
        Ok((
            proof,
            crate::CommittedApprovalApply {
                output:
                    ParentPendingApply::Answer {
                        address,
                        reply,
                        effects,
                    },
                publication_failure,
            },
        )) => {
            let reply = reply.release(&proof, &address, target)?;
            let offers = effects
                .release(&proof)
                .map_err(|_| StoreFailure::AnswerLost)?;
            Ok(CentralCallWork {
                result: match publication_failure {
                    None => Ok(TypedCallResult::Answered { reply }),
                    Some(failure) => Err(failure),
                },
                committed_delivery: delivery.after_reply(offers, None),
                completion: None,
            })
        }
        Err(failure) => report_approval_failure(ApprovalDispatchFailure::Transaction(failure)),
    }
}

fn apply_parent<'loan, Transaction, Db>(
    loan: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    machine: ValidatedMachineConnection,
    call: ParentApprovalCall,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, ParentPendingApply>
where
    Transaction: ApprovalStoreTransaction<Database = Db> + 'loan,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Box::pin(async move {
        let (sampled, lapse) =
            recheck_lent_link_authority(loan, machine.attachment_ref(), clock.as_ref()).await?;
        if let Some(cause) = lapse {
            return Ok(ParentPendingApply::Refused(
                LinkCallRefusal::AuthorityLapsed { cause },
            ));
        }
        let proof = PresentedSessionSecret::Secret {
            secret: call.secret,
        };
        let Some(admitted) = SessionService::in_transaction(loan)
            .authenticate_live_session(&machine, &call.session, &proof)
            .await?
        else {
            return Ok(ParentPendingApply::Refused(
                LinkCallRefusal::SessionProofNotAccepted,
            ));
        };
        let session = admitted.scoped_session();
        let address = ApprovalReplyAddress::Session {
            organization: admitted.organization_id().clone(),
            session: ApprovalSession {
                session_id: session.session_id.clone(),
                enrollment: ApprovalEnrollment {
                    machine_id: session.machine_id.clone(),
                    enrollment_id: session.enrollment_id.clone(),
                },
            },
        };
        let time = crate::approval_applying_time::read_applying_time(loan, sampled).await?;
        let clock = ApprovalApplyingClock(time.effective());
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            loan,
            &ActorApprovalSessionFacts,
            &clock,
            &mut row_ids,
        );
        let mut publish_time = true;
        let reply = match call.operation {
            ParentApprovalOperation::List(input) => ParentPendingReply::List(
                store
                    .list_for_session(&admitted, input.cursor.as_ref())
                    .await?,
            ),
            ParentApprovalOperation::Read(input) => {
                match store.read_for_session(&admitted, &input.request_id).await? {
                    SessionReadApply::Applied(reply) => ParentPendingReply::Read(reply),
                    SessionReadApply::Unchanged(reply) => {
                        publish_time = false;
                        ParentPendingReply::Read(reply)
                    }
                }
            }
            ParentApprovalOperation::Decide(input) => {
                match store.decide_as_session(&admitted, input).await? {
                    SessionDecisionApply::Reply(reply) => ParentPendingReply::Decide(reply),
                    SessionDecisionApply::PersonRequired => {
                        return Ok(ParentPendingApply::PersonRequired);
                    }
                }
            }
            ParentApprovalOperation::PassOn(input) => {
                ParentPendingReply::PassOn(store.pass_on_as_session(&admitted, input).await?)
            }
        };
        let effects = store.finish_effects();
        if publish_time {
            time.publish(loan).await?;
        }
        Ok(ParentPendingApply::Answer {
            address,
            reply: Box::new(reply),
            effects,
        })
    })
}
