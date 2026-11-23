//! Executing a signed person command consumes the loan it was admitted on.
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

enum SignedAction<'loan> {
    List(AdmittedApprovalFacts<'loan, ApprovalListRequest>),
    Read(AdmittedApprovalFacts<'loan, ApprovalReadRequest>),
    Decide(AdmittedApprovalFacts<'loan, ApprovalDecideRequest>),
    Retry(AdmittedApprovalFacts<'loan, RetainedDecisionRequest>),
}

/// A retry without permission consumes the proof only if a retained result matched.
pub async fn apply_person_command<Transaction: ApprovalPersonStoreTransaction>(
    command: AdmittedApprovalPersonOperation<'_, Transaction>,
    commit: &ApprovalCommitScope,
    row_ids: &mut dyn IdSource,
) -> Result<Option<PersonApprovalStoreOutput>, ApprovalApplyFailure> {
    let (transaction, action) = match command {
        AdmittedApprovalPersonOperation::List(command) => {
            let (loan, signed) = command.into_application();
            (loan, SignedAction::List(signed))
        }
        AdmittedApprovalPersonOperation::Read(command) => {
            let (loan, signed) = command.into_application();
            (loan, SignedAction::Read(signed))
        }
        AdmittedApprovalPersonOperation::Decide(command) => {
            let (loan, signed) = command.into_application();
            (loan, SignedAction::Decide(signed))
        }
        AdmittedApprovalPersonOperation::Retry(command) => {
            let (loan, signed) = command.into_application();
            (loan, SignedAction::Retry(signed))
        }
    };
    let sampled = match &action {
        SignedAction::List(signed) => signed.sampled(),
        SignedAction::Read(signed) => signed.sampled(),
        SignedAction::Decide(signed) => signed.sampled(),
        SignedAction::Retry(signed) => signed.sampled(),
    };
    let time = ApprovalApplyingTime::read(transaction, sampled).await?;
    let retained_at = EngineTime::from_timestamp_millis(sampled.unix_millis())
        .ok_or_else(|| corrupt("approval_store.applying_time"))?;
    let mut store = ApprovalApplyingStore::new(commit, transaction, &(), &time, row_ids);
    let (reply, address, reservation) = match action {
        SignedAction::List(signed) => {
            let request = signed.request();
            let reply = store
                .list_person_requests(signed.identity(), request.filter, request.cursor.as_ref())
                .await?;
            (
                PersonApprovalStoreReply::List(reply),
                reply_address(signed.identity()),
                signed.into_reservation(),
            )
        }
        SignedAction::Read(signed) => {
            let reply = store
                .read_person_request(signed.identity(), &signed.request().request_id)
                .await?;
            (
                PersonApprovalStoreReply::Read(reply),
                reply_address(signed.identity()),
                signed.into_reservation(),
            )
        }
        SignedAction::Decide(signed) => {
            let reply = store
                .decide_person_request(signed.identity(), signed.request().clone(), retained_at)
                .await?;
            (
                PersonApprovalStoreReply::Decide(reply),
                reply_address(signed.identity()),
                signed.into_reservation(),
            )
        }
        SignedAction::Retry(signed) => {
            let Some(reply) = store
                .replay_person_decision(&signed.retained_decision(), retained_at)
                .await?
            else {
                return Ok(None);
            };
            (
                PersonApprovalStoreReply::Decide(reply),
                reply_address(signed.identity()),
                signed.into_reservation(),
            )
        }
    };
    store.execute_guarded_write(reservation).await?;
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
