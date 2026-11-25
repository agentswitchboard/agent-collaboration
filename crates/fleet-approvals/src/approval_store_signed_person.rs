//! Executing a person command consumes the loan it was admitted on.
use central_engine_model::{EngineTime, IdSource};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_signin::{
    AdmittedApprovalFacts, AdmittedApprovalPersonOperation, RetainedDecisionRequest,
    approval_person_wire::{
        ApprovalDecideReply, ApprovalDecideRequest, ApprovalListReply, ApprovalListRequest,
        ApprovalReadReply, ApprovalReadRequest,
    },
};

use super::{ApprovalApplyFailure, ApprovalApplyingStore, person_commands::reply_address};
use crate::{
    ApprovalApplyingTime, ApprovalCommitScope, ApprovalPersonStoreTransaction,
    ApprovalReplyAddress, PendingApprovalEffects, PendingApprovalReply,
};

pub enum PersonApprovalStoreReply {
    List(PendingApprovalReply<ApprovalListReply>),
    Read(PendingApprovalReply<ApprovalReadReply>),
    Decide(PendingApprovalReply<ApprovalDecideReply>),
}

pub struct PersonApprovalStoreOutput {
    pub reply: PersonApprovalStoreReply,
    pub address: ApprovalReplyAddress,
    pub effects: PendingApprovalEffects,
}

enum PersonApprovalAction<'loan> {
    List(AdmittedApprovalFacts<'loan, ApprovalListRequest>),
    Read(AdmittedApprovalFacts<'loan, ApprovalReadRequest>),
    Decide(AdmittedApprovalFacts<'loan, ApprovalDecideRequest>),
    Retry(AdmittedApprovalFacts<'loan, RetainedDecisionRequest>),
}

/// A signed retry without permission consumes its proof only if a retained result matched.
pub async fn apply_person_command<Transaction: ApprovalPersonStoreTransaction>(
    command: AdmittedApprovalPersonOperation<'_, Transaction>,
    commit: &ApprovalCommitScope,
    row_ids: &mut dyn IdSource,
) -> Result<Option<PersonApprovalStoreOutput>, ApprovalApplyFailure> {
    let (transaction, action) = match command {
        AdmittedApprovalPersonOperation::List(command) => {
            let (loan, admitted) = command.into_application();
            (loan, PersonApprovalAction::List(admitted))
        }
        AdmittedApprovalPersonOperation::Read(command) => {
            let (loan, admitted) = command.into_application();
            (loan, PersonApprovalAction::Read(admitted))
        }
        AdmittedApprovalPersonOperation::Decide(command) => {
            let (loan, admitted) = command.into_application();
            (loan, PersonApprovalAction::Decide(admitted))
        }
        AdmittedApprovalPersonOperation::Retry(command) => {
            let (loan, admitted) = command.into_application();
            (loan, PersonApprovalAction::Retry(admitted))
        }
    };
    let sampled = match &action {
        PersonApprovalAction::List(admitted) => admitted.sampled(),
        PersonApprovalAction::Read(admitted) => admitted.sampled(),
        PersonApprovalAction::Decide(admitted) => admitted.sampled(),
        PersonApprovalAction::Retry(admitted) => admitted.sampled(),
    };
    let time = ApprovalApplyingTime::read(transaction, sampled).await?;
    let retained_at = EngineTime::from_timestamp_millis(sampled.unix_millis())
        .ok_or_else(|| corrupt("approval_store.applying_time"))?;
    let mut store = ApprovalApplyingStore::new(commit, transaction, &(), &time, row_ids);
    let (reply, address, reservation) = match action {
        PersonApprovalAction::List(admitted) => {
            let request = admitted.request();
            let reply = store
                .list_person_requests(admitted.identity(), request.filter, request.cursor.as_ref())
                .await?;
            (
                PersonApprovalStoreReply::List(reply),
                reply_address(admitted.identity()),
                admitted.into_reservation(),
            )
        }
        PersonApprovalAction::Read(admitted) => {
            let reply = store
                .read_person_request(admitted.identity(), &admitted.request().request_id)
                .await?;
            (
                PersonApprovalStoreReply::Read(reply),
                reply_address(admitted.identity()),
                admitted.into_reservation(),
            )
        }
        PersonApprovalAction::Decide(admitted) => {
            let reply = store
                .decide_person_request(admitted.identity(), admitted.request().clone(), retained_at)
                .await?;
            (
                PersonApprovalStoreReply::Decide(reply),
                reply_address(admitted.identity()),
                admitted.into_reservation(),
            )
        }
        PersonApprovalAction::Retry(admitted) => {
            let Some(reply) = store
                .replay_person_decision(&admitted.retained_decision(), retained_at)
                .await?
            else {
                return Ok(None);
            };
            (
                PersonApprovalStoreReply::Decide(reply),
                reply_address(admitted.identity()),
                admitted.into_reservation(),
            )
        }
    };
    if let Some(reservation) = reservation {
        store.execute_guarded_write(reservation).await?;
    }
    let settled_approval = !store.holder_offers.is_empty();
    let effects = store.finish_effects();
    if settled_approval {
        time.persist(transaction).await?;
    }
    Ok(Some(PersonApprovalStoreOutput {
        reply,
        address,
        effects,
    }))
}
