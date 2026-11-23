//! Admit a parent approval call using the exact session proof in the organization loan.
use std::sync::Arc;

use approval_request_protocol::{
    ApprovalSessionCall, DecideApprovalAsSessionRequest, ListApprovalsForSessionReply,
    ListApprovalsForSessionRequest, PassOnApprovalRequest, ReadApprovalForSessionReply,
    ReadApprovalForSessionRequest,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, LinkCallRefusal, TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, ListApprovalsForSessionRpc, ReadApprovalForSessionRpc},
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallWork, ValidatedMachineConnection, recheck_lent_link_authority,
};
use fleet_actor_identity::SessionService;
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalCommitProof, ApprovalCommitScope, ApprovalEnrollment,
    ApprovalReplyAddress, ApprovalSession, ApprovalStoreTransaction, PendingApprovalHolderEffects,
    PendingApprovalReply,
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
    ApprovalAlarmPublication, ApprovalEndingTransport, ApprovalTransactionFuture,
    OrganizationApprovalTransactions,
    approval_dispatch::{
        ApprovalApplyingClock, ApprovalApplyingIds, ApprovalDispatchFailure,
        ApprovalLinkApplication, ApprovalStore, report_approval_failure,
    },
    approval_transaction::commit_approval,
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
}

enum ParentPendingApply {
    Refused(LinkCallRefusal),
    Answer {
        address: ApprovalReplyAddress,
        reply: ParentPendingReply,
        effects: PendingApprovalHolderEffects,
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
        }
        .map_err(|_| StoreFailure::AnswerLost)
    }
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
    pub(crate) async fn dispatch_parent(
        &self,
        machine: &ValidatedMachineConnection,
        call: ParentApprovalCall,
        transport: Arc<dyn ApprovalEndingTransport>,
    ) -> Result<CentralCallWork, StoreFailure> {
        let organization = machine.organization_id();
        if &call.target.organization_id != organization {
            return Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            }
            .into());
        }
        let machine = machine.clone();
        let clock = self.clock.clone();
        let ids = self.ids.clone();
        let target = call.target.clone();
        match commit_approval(
            self.transactions.clone(),
            organization.clone(),
            ApprovalAlarmPublication::WithCommit,
            move |loan, scope| apply_parent(loan, scope, machine, call, clock, ids),
        )
        .await
        {
            Ok((_, ParentPendingApply::Refused(reason))) => {
                Ok(TypedCallResult::Refused { reason }.into())
            }
            Ok((
                proof,
                ParentPendingApply::Answer {
                    address,
                    reply,
                    effects,
                },
            )) => {
                let reply = reply.release(&proof, &address, target)?;
                let offers = effects
                    .release(&proof)
                    .map_err(|_| StoreFailure::AnswerLost)?;
                let delivery = crate::approval_delivery::ApprovalDelivery {
                    application: self.clone(),
                    transport,
                };
                Ok(CentralCallWork {
                    result: TypedCallResult::Answered { reply },
                    completion: delivery.after_reply(offers, None),
                })
            }
            Err(failure) => report_approval_failure(ApprovalDispatchFailure::Transaction(failure)),
        }
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
        let observed = crate::approval_applying_time::observe_applying_time(loan, sampled).await?;
        let clock = ApprovalApplyingClock(observed);
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            loan,
            &ActorApprovalSessionFacts,
            &clock,
            &mut row_ids,
        );
        let reply = match call.operation {
            ParentApprovalOperation::List(input) => ParentPendingReply::List(
                store
                    .list_for_session(&admitted, input.cursor.as_ref())
                    .await?,
            ),
            ParentApprovalOperation::Read(input) => ParentPendingReply::Read(
                store.read_for_session(&admitted, &input.request_id).await?,
            ),
            ParentApprovalOperation::Decide(_) | ParentApprovalOperation::PassOn(_) => {
                return Ok(ParentPendingApply::Refused(LinkCallRefusal::UnknownCall));
            }
        };
        Ok(ParentPendingApply::Answer {
            address,
            reply,
            effects: store.finish_holder_effects(),
        })
    })
}
