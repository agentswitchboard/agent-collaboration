//! Supplies approval routing from the actor store in the caller's transaction.
use central_storage::StoreFailure;
use fleet_approvals::{ApprovalEnrollment, ApprovalRouting, ApprovalSession, ApprovalSessionFacts};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use crate::{LiveSessionFacts, SessionAvailability, SessionMachineCaller, SessionService};

/// Bound by the host to the organization whose transaction it lends.
pub struct ActorApprovalSessionFacts {
    organization: OrganizationId,
}

impl ActorApprovalSessionFacts {
    pub fn for_organization(organization: OrganizationId) -> Self {
        Self { organization }
    }
}

impl<Transaction: CentralTransaction> ApprovalSessionFacts<Transaction>
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
        organization: &OrganizationId,
        requester: &ApprovalSession,
    ) -> Result<Option<ApprovalRouting>, StoreFailure> {
        if self.organization != *organization {
            return Ok(None);
        }
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
        organization: &OrganizationId,
        session: &ApprovalSession,
    ) -> Result<bool, StoreFailure> {
        if self.organization != *organization {
            return Ok(false);
        }
        let caller = session_caller(session);
        Ok(SessionService::in_transaction(transaction)
            .live_session(&caller, &session.session_id)
            .await?
            .is_some())
    }
}

fn session_caller(session: &ApprovalSession) -> SessionMachineCaller {
    SessionMachineCaller {
        machine_id: session.enrollment.machine_id.clone(),
        enrollment_id: session.enrollment.enrollment_id.clone(),
    }
}
