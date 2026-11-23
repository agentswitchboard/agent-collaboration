//! Exercise the session service's actual admission and live approvals adapter.
use central_engine_model::EngineTime;
use fleet_actor_identity::{AuthenticatedSession, SessionService};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyingStore, ApprovalCommitScope, ApprovalHolderAdmission,
    ApprovalHolderCommand,
};
use fleet_contracts::{ClientId, MachineConnectionAttachment};
use fleet_identity_protocol::{PresentedSessionSecret, UnixSeconds};
use fleet_lent_transaction::OrganizationTransaction;
use fleet_machine_login::link_authority::{ValidatedMachineConnection, restore_fixture_link};
use serde_json::json;
use workspace_ids::CentralSessionId;

use super::fixture_commands::{SessionApprovalWitness, WitnessHolder};
use super::fixture_transaction::WorkerApprovalTransaction;
use super::{
    PendingWitnessApply, WitnessClock, WitnessFailure, WitnessIds, applied_json, decision_json,
    holder_json,
};

pub(super) async fn apply_authenticated(
    transaction: &mut WorkerApprovalTransaction,
    commit: &ApprovalCommitScope,
    at_ms: i64,
    holder: WitnessHolder,
    operation: SessionApprovalWitness,
) -> Result<PendingWitnessApply, WitnessFailure> {
    let Some(connection) = fixture_connection(transaction, &holder, at_ms).await? else {
        return Ok(not_admitted());
    };
    let admission = match &operation {
        SessionApprovalWitness::Decide {
            session_id,
            presented,
            ..
        }
        | SessionApprovalWitness::PassOn {
            session_id,
            presented,
            ..
        } => {
            let Some(session) =
                authenticate(transaction, &connection, session_id, presented).await?
            else {
                return Ok(not_admitted());
            };
            Some(session)
        }
        SessionApprovalWitness::RegisterRun | SessionApprovalWitness::Raise { .. } => None,
    };
    let clock = WitnessClock(at_ms);
    let mut ids = WitnessIds(at_ms);
    let mut store = ApprovalApplyingStore::new(
        commit,
        transaction,
        &ActorApprovalSessionFacts,
        &clock,
        &mut ids,
    );
    let holder = ApprovalHolderAdmission::for_connection(&connection, holder.run_id);
    let reply = match operation {
        SessionApprovalWitness::RegisterRun => {
            applied_json(store.register_approval_run(&holder).await?, |reply| {
                json!(reply)
            })
        }
        SessionApprovalWitness::Raise { request } => applied_json(
            store
                .apply_holder_command(&holder, ApprovalHolderCommand::Raise(request))
                .await?,
            holder_json,
        ),
        SessionApprovalWitness::Decide { request, .. } => applied_json(
            store
                .decide_session_approval(
                    admission.as_ref().ok_or(WitnessFailure::Storage)?,
                    request,
                )
                .await?,
            decision_json,
        ),
        SessionApprovalWitness::PassOn { request, .. } => applied_json(
            store
                .pass_on_session_approval(
                    admission.as_ref().ok_or(WitnessFailure::Storage)?,
                    request,
                )
                .await?,
            |reply| json!(reply),
        ),
    };
    Ok((reply, Some(store.finish_holder_effects())))
}

async fn fixture_connection(
    transaction: &mut WorkerApprovalTransaction,
    holder: &WitnessHolder,
    at_ms: i64,
) -> Result<Option<ValidatedMachineConnection>, WitnessFailure> {
    let organization = transaction.organization_id().clone();
    let now = EngineTime::from_timestamp_millis(at_ms).ok_or(WitnessFailure::Clock)?;
    let client = ClientId::try_from(holder.enrollment.as_str().to_owned())
        .map_err(|_| WitnessFailure::Storage)?;
    let attachment = MachineConnectionAttachment::from_token_claims(
        holder.organization.clone(),
        holder.machine.clone(),
        holder.enrollment.clone(),
        client,
        UnixSeconds(now.timestamp() + 3_600),
    );
    restore_fixture_link(transaction, &attachment, &organization, at_ms)
        .await
        .map(|result| result.ok())
        .map_err(|_| WitnessFailure::Storage)
}

async fn authenticate(
    transaction: &mut WorkerApprovalTransaction,
    connection: &ValidatedMachineConnection,
    session_id: &CentralSessionId,
    presented: &PresentedSessionSecret,
) -> Result<Option<AuthenticatedSession>, WitnessFailure> {
    SessionService::in_transaction(transaction)
        .authenticate_live_session(connection, session_id, presented)
        .await
        .map_err(|_| WitnessFailure::Storage)
}

fn not_admitted() -> PendingWitnessApply {
    (
        Box::new(|_, _| Ok(json!({ "result": { "kind": "notAdmitted" }, "offers": [] }))),
        None,
    )
}
