//! Supplies approval routing from the actor store in the caller's transaction.
use crate::{ApprovalEnrollment, ApprovalRouting, ApprovalSession, ApprovalSessionFacts};
use central_storage::StoreFailure;
use fleet_lent_transaction::OrganizationTransaction;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use fleet_actor_identity::{
    LiveSessionFacts, SessionAvailability, SessionLookupScope, SessionService,
};

/// Reads approval facts in the caller’s organization transaction.
pub struct ActorApprovalSessionFacts;

impl<Transaction: OrganizationTransaction + Send> ApprovalSessionFacts<Transaction>
    for ActorApprovalSessionFacts
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'query> Encode<'query, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    Option<String>: Type<Transaction::Database>
        + for<'query> Encode<'query, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    i64: Type<Transaction::Database>
        + for<'query> Encode<'query, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    Option<i64>: Type<Transaction::Database> + for<'row> Decode<'row, Transaction::Database>,
    for<'row> &'row str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    async fn session_initial_route(
        &self,
        transaction: &mut Transaction,
        requester: &ApprovalSession,
    ) -> Result<Option<ApprovalRouting>, StoreFailure> {
        let caller = session_caller(requester);
        let availability = SessionService::in_transaction(transaction)
            .session_facts(&caller, &requester.session_id)
            .await?;
        Ok(match availability {
            SessionAvailability::Unavailable => None,
            SessionAvailability::Live(LiveSessionFacts::Owner) => Some(ApprovalRouting::Owner),
            SessionAvailability::Live(LiveSessionFacts::Delegate { parent }) => {
                Some(ApprovalRouting::Delegate {
                    admitted_parent: ApprovalSession {
                        enrollment: ApprovalEnrollment {
                            machine_id: parent.machine_id,
                            enrollment_id: parent.enrollment_id,
                        },
                        session_id: parent.session_id,
                    },
                })
            }
        })
    }

    async fn session_is_live(
        &self,
        transaction: &mut Transaction,
        session: &ApprovalSession,
    ) -> Result<bool, StoreFailure> {
        let caller = session_caller(session);
        SessionService::in_transaction(transaction)
            .session_is_live(&caller, &session.session_id)
            .await
    }
}

fn session_caller(session: &ApprovalSession) -> SessionLookupScope {
    SessionLookupScope {
        machine_id: session.enrollment.machine_id.clone(),
        enrollment_id: session.enrollment.enrollment_id.clone(),
    }
}
