//! Produce the owning session's end reply and fact on the same lent transaction.
use super::{
    AppliedSessionEnd, SessionCallOutcome, SessionLookupScope, SessionService, SessionServiceError,
};
use crate::{SessionEndIdentity, SessionEvent, SessionReceipt, SessionState};
use central_engine_model::Context;
use fleet_identity_protocol::{EndFleetSession, EndFleetSessionReply, EndSessionTarget};
use fleet_lent_transaction::OrganizationTransaction;
use fleet_machine_login::link_authority::ValidatedMachineConnection;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<Transaction: OrganizationTransaction> SessionService<'_, Transaction>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<String>: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<i64>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    i64: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    pub async fn end(
        &mut self,
        connection: &ValidatedMachineConnection,
        request: EndFleetSession,
        context: &mut Context<'_>,
    ) -> Result<SessionCallOutcome<EndFleetSessionReply>, SessionServiceError> {
        Ok(
            match self.end_with_fact(connection, request, context).await? {
                SessionCallOutcome::Answered(end) => SessionCallOutcome::Answered(end.reply),
                SessionCallOutcome::Revoked => SessionCallOutcome::Revoked,
            },
        )
    }

    pub async fn end_with_fact(
        &mut self,
        connection: &ValidatedMachineConnection,
        request: EndFleetSession,
        context: &mut Context<'_>,
    ) -> Result<SessionCallOutcome<AppliedSessionEnd>, SessionServiceError> {
        if self.repository.tx.organization_id() != connection.organization_id() {
            return Ok(SessionCallOutcome::Revoked);
        }
        let caller = &SessionLookupScope {
            machine_id: connection.machine_id().clone(),
            enrollment_id: connection.enrollment_id().clone(),
        };
        if !self.is_current(caller).await? {
            return Ok(SessionCallOutcome::Revoked);
        }
        let (prior, by) = match request.target {
            EndSessionTarget::CentralId { session_id } => (
                self.repository
                    .by_id(&caller.machine_id, &session_id)
                    .await?,
                SessionEndIdentity::CentralId,
            ),
            EndSessionTarget::OperationId {
                operation_id,
                contents_digest,
            } => (
                self.repository
                    .by_operation(&caller.machine_id, &operation_id)
                    .await?,
                SessionEndIdentity::OperationDigest(contents_digest),
            ),
        };
        let ended_session = match &prior {
            SessionState::Active(bound) => Some(bound.reserved.reservation.session_id.clone()),
            SessionState::Missing
            | SessionState::Reserved(_)
            | SessionState::Abandoned { .. }
            | SessionState::Ended { .. } => None,
        };
        let historical = self.historical_end(caller, &prior).await?;
        let receipt = self
            .apply(
                prior,
                SessionEvent::End {
                    by,
                    cause: request.cause,
                },
                historical,
                context,
            )
            .await?;
        match receipt {
            SessionReceipt::End(reply) => {
                use fleet_identity_protocol::NewEndSettlement;
                let ended_session = ended_session.filter(|_| {
                    matches!(
                        reply,
                        EndFleetSessionReply::Settled {
                            settlement: NewEndSettlement::Ended { .. }
                        }
                    )
                });
                Ok(SessionCallOutcome::Answered(AppliedSessionEnd {
                    reply,
                    ended_session,
                }))
            }
            SessionReceipt::Create(_) | SessionReceipt::Bind(_) => {
                Err(corrupt("actor_sessions.end_receipt").into())
            }
        }
    }
}
