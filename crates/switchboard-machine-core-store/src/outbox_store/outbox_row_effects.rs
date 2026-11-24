//! Applies row-machine effects inside the caller's SQLite transaction.
use super::{
    OutboxStoreError,
    outbox_row_machine::{OutboxRowEffect, OutboxRowEvent, OutboxRowMachine, OutboxRowRefusal},
    outbox_row_state::{OutboxPhase, OutboxRowState},
    outbox_rows::{corrupt_row, expect_one},
};
use crate::{EndedWrite, outbox_item_state::OutboxItemState};
use state_machine_core::{StateMachine, StateTag, TransitionError};
use workspace_ids::RequestId;

pub(super) enum RowTransition {
    NotDue,
    NoChange,
    Applied,
    Ended(EndedWrite),
    NoticeUnchanged(bool),
    Removed(u64),
}

pub(super) async fn apply_row_event(
    connection: &mut sqlx::SqliteConnection,
    row: &OutboxRowState,
    event: OutboxRowEvent,
    expected: &'static str,
) -> Result<RowTransition, OutboxStoreError> {
    let (next, effects) = match OutboxRowMachine::transition(row, event) {
        Ok(transition) => transition,
        Err(TransitionError::Domain(OutboxRowRefusal::NotDue)) => return Ok(RowTransition::NotDue),
        Err(error) => return Err(store_error(&row.request, expected, error)),
    };
    let request = &row.request;
    let next_tag = next.state_tag();
    let previous_tag = row.state_tag();
    let now_ms = next.updated_at_ms;
    let mut result = RowTransition::NoChange;
    for effect in effects {
        result = match effect {
            OutboxRowEffect::Stamp => {
                let changed = sqlx::query!(
                    "UPDATE outbox_items SET item_state = ?, updated_at = ? WHERE request_id = ? AND item_state = ?",
                    next_tag, now_ms, request.as_str(), previous_tag,
                ).execute(&mut *connection).await?.rows_affected();
                expect_one(changed, request, "in its previous state")?;
                RowTransition::Applied
            }
            OutboxRowEffect::Finish { reason } => {
                let profile = next
                    .phase
                    .profile()
                    .ok_or_else(|| corrupt_row(request.as_str(), "payload_json"))?;
                let record = serde_json::to_string(profile)?;
                let changed = sqlx::query!(
                    "UPDATE outbox_items SET item_state = ?, payload_json = ?, updated_at = ?
         WHERE request_id = ? AND item_state = ?",
                    next_tag,
                    record,
                    now_ms,
                    request.as_str(),
                    previous_tag
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                expect_one(changed, request, "waiting or sent")?;
                RowTransition::Ended(EndedWrite {
                    caller: next.caller.clone(),
                    request_id: next.request.clone(),
                    target: profile.target.clone(),
                    reason,
                })
            }
            OutboxRowEffect::StoreOutcome => {
                store_outcome(connection, row, &next).await?;
                RowTransition::Applied
            }
            OutboxRowEffect::StoreLateOutcome => {
                let OutboxPhase::Unconfirmed(_, Some(settlement)) = &next.phase else {
                    return Err(corrupt_row(request.as_str(), "outcome_json"));
                };
                let outcome_json = serde_json::to_string(settlement)?;
                let changed = sqlx::query!(
                    "UPDATE outbox_items SET outcome_json = ?
             WHERE request_id = ? AND item_state = ? AND outcome_json IS NULL",
                    outcome_json,
                    request.as_str(),
                    OutboxItemState::Unconfirmed.as_str(),
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                expect_one(changed, request, "ended unconfirmed")?;
                RowTransition::Applied
            }
            OutboxRowEffect::StoreSenderOutcome => {
                let OutboxPhase::Answered(profile, accepted) = &next.phase else {
                    return Err(corrupt_row(request.as_str(), "outcome_json"));
                };
                let payload_json = serde_json::to_string(profile)?;
                let outcome_json = serde_json::to_string(accepted)?;
                sqlx::query!(
                    "UPDATE outbox_items SET item_state = ?, payload_json = ?, outcome_json = ?, updated_at = ?
         WHERE request_id = ?",
                    next_tag,
                    payload_json,
                    outcome_json,
                    now_ms,
                    request.as_str(),
                )
                .execute(&mut *connection).await?;
                RowTransition::Applied
            }
            OutboxRowEffect::EnrichSenderProfile => {
                let profile = next
                    .phase
                    .profile()
                    .ok_or_else(|| corrupt_row(request.as_str(), "payload_json"))?;
                let payload_json = serde_json::to_string(profile)?;
                sqlx::query!(
                    "UPDATE outbox_items SET payload_json = ? WHERE request_id = ?",
                    payload_json,
                    request.as_str(),
                )
                .execute(&mut *connection)
                .await?;
                RowTransition::Applied
            }
            OutboxRowEffect::SenderNoticeUnchanged { retained } => {
                RowTransition::NoticeUnchanged(retained)
            }
            OutboxRowEffect::Delete => {
                let changed = sqlx::query!(
                    "DELETE FROM outbox_items WHERE request_id = ?",
                    request.as_str()
                )
                .execute(&mut *connection)
                .await?
                .rows_affected();
                RowTransition::Removed(changed)
            }
        };
    }
    Ok(result)
}

async fn store_outcome(
    connection: &mut sqlx::SqliteConnection,
    previous: &OutboxRowState,
    next: &OutboxRowState,
) -> Result<(), OutboxStoreError> {
    let request = &previous.request;
    let request_id = request.as_str();
    let (profile, outcome_json) = match &next.phase {
        OutboxPhase::Answered(profile, reply) => (profile, serde_json::to_string(reply)?),
        OutboxPhase::Refused(profile, reason) => (profile, serde_json::to_string(reason)?),
        _ => return Err(corrupt_row(request_id, "outcome_json")),
    };
    let payload_json = serde_json::to_string(profile)?;
    let to = next.state_tag();
    let from = previous.state_tag();
    let now_ms = next.updated_at_ms;
    let changed = sqlx::query!(
        "UPDATE outbox_items SET item_state = ?, outcome_json = ?, payload_json = ?,
                    updated_at = ?
             WHERE request_id = ? AND item_state = ?",
        to,
        outcome_json,
        payload_json,
        now_ms,
        request_id,
        from
    )
    .execute(connection)
    .await?
    .rows_affected();
    expect_one(changed, request, "sent")
}

pub(super) fn unexpected(request: &RequestId, expected: &'static str) -> OutboxStoreError {
    OutboxStoreError::UnexpectedState {
        request: request.clone(),
        expected,
    }
}

fn store_error(
    request: &RequestId,
    expected: &'static str,
    error: TransitionError<OutboxRowRefusal>,
) -> OutboxStoreError {
    use OutboxRowRefusal::{
        ContradictoryFinalOutcome, CorruptPayload, DeliveryOutcomeMismatch, Digest, InvalidOutcome,
        LateOutcomeForNotSent, NotDue,
    };
    match error {
        TransitionError::Domain(CorruptPayload) => corrupt_row(request.as_str(), "payload_json"),
        TransitionError::Domain(Digest) => OutboxStoreError::Digest,
        TransitionError::Domain(InvalidOutcome) => OutboxStoreError::InvalidOutcome {
            request: request.clone(),
        },
        TransitionError::Domain(LateOutcomeForNotSent) => OutboxStoreError::LateOutcomeForNotSent {
            request: request.clone(),
        },
        TransitionError::Domain(ContradictoryFinalOutcome) => {
            OutboxStoreError::ContradictoryFinalOutcome {
                request: request.clone(),
            }
        }
        TransitionError::Domain(DeliveryOutcomeMismatch) => {
            OutboxStoreError::DeliveryOutcomeMismatch {
                request: request.clone(),
            }
        }
        TransitionError::Domain(NotDue)
        | TransitionError::NotAllowed { .. }
        | TransitionError::Stale => unexpected(request, expected),
    }
}
