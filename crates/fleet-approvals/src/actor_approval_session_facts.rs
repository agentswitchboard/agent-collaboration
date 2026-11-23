//! Supplies approval routing from the actor store in the caller's transaction.
use crate::{ApprovalEnrollment, ApprovalRouting, ApprovalSession, ApprovalSessionFacts};
use central_storage::StoreFailure;
use fleet_lent_transaction::OrganizationTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::BTreeSet;

use fleet_actor_identity::{
    ApprovalChainFact, ApprovalChainFailure, ScopedSession, SessionLookupScope, SessionService,
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
        if !self.session_is_live(transaction, requester).await? {
            return Ok(None);
        }
        Ok(Some(
            self.next_approval_parent(transaction, requester)
                .await?
                .map_or(ApprovalRouting::Owner, |admitted_parent| {
                    ApprovalRouting::Delegate { admitted_parent }
                }),
        ))
    }

    async fn next_approval_parent(
        &self,
        transaction: &mut Transaction,
        session: &ApprovalSession,
    ) -> Result<Option<ApprovalSession>, StoreFailure> {
        let mut service = SessionService::in_transaction(transaction);
        let origin = service
            .approval_chain_fact(&session.session_id)
            .await
            .map_err(approval_chain_failure)?;
        if approval_session(origin.scope) != *session {
            return Err(corrupt("actor_sessions.approval_chain_scope"));
        }
        let mut parent = match origin.fact {
            ApprovalChainFact::Live { parent } | ApprovalChainFact::Ended { parent } => parent,
        };
        let mut seen = BTreeSet::from([session.session_id.clone()]);
        while let Some(parent_session) = parent {
            if !seen.insert(parent_session.clone()) {
                return Err(corrupt("actor_delegates.approval_chain_cycle"));
            }
            let ancestor = service
                .approval_chain_fact(&parent_session)
                .await
                .map_err(approval_chain_failure)?;
            // D12's step-over arrives with the session owner's adopted record.
            // Match the closed facts so that its future arm forces an explicit decision.
            match ancestor.fact {
                ApprovalChainFact::Live { .. } => {
                    return Ok(Some(approval_session(ancestor.scope)));
                }
                ApprovalChainFact::Ended { parent: recorded } => parent = recorded,
            }
        }
        Ok(None)
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

fn approval_session(scope: ScopedSession) -> ApprovalSession {
    ApprovalSession {
        enrollment: ApprovalEnrollment {
            machine_id: scope.machine_id,
            enrollment_id: scope.enrollment_id,
        },
        session_id: scope.session_id,
    }
}

fn approval_chain_failure(failure: ApprovalChainFailure) -> StoreFailure {
    match failure {
        ApprovalChainFailure::Missing | ApprovalChainFailure::Unbound => {
            corrupt("actor_sessions.approval_chain_session")
        }
        ApprovalChainFailure::Store(failure) => failure,
    }
}
