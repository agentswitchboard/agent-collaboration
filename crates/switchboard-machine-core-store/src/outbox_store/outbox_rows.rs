//! Checked row decoding for the Mac outbox.
use super::OutboxStoreError;
use crate::{
    OutboxQueuedWrite, OutboxRequestStatus, OutboxWriteContent, outbox_item_state::OutboxItemState,
    outbox_receipt_profile::AnsweredWrite,
};
use central_link_protocol::ReplyPayload;
use workspace_ids::RequestId;
use workspace_model::{Caller, operation_content_digest};
pub(super) struct OutboxRow {
    pub(super) request_id: String,
    pub(super) caller_json: String,
    pub(super) payload_json: String,
    pub(super) item_state: String,
    pub(super) flush_at_ms: i64,
    pub(super) outcome_json: Option<String>,
}

/// The row put on the wire, with its creation stamp: a direct message's
/// sent_at.
pub(super) struct OnTheWireRow {
    pub(super) request_id: String,
    pub(super) caller_json: String,
    pub(super) payload_json: String,
    pub(super) item_state: String,
    pub(super) flush_at_ms: i64,
    pub(super) outcome_json: Option<String>,
    pub(super) created_at: i64,
}

impl OnTheWireRow {
    pub(super) fn split(self) -> (OutboxRow, i64) {
        (
            OutboxRow {
                request_id: self.request_id,
                caller_json: self.caller_json,
                payload_json: self.payload_json,
                item_state: self.item_state,
                flush_at_ms: self.flush_at_ms,
                outcome_json: self.outcome_json,
            },
            self.created_at,
        )
    }
}

pub(super) struct OutgoingRow {
    pub(super) request_id: String,
    pub(super) caller_json: String,
    pub(super) payload_json: String,
    pub(super) item_state: String,
    pub(super) flush_at_ms: i64,
    pub(super) outcome_json: Option<String>,
    pub(super) updated_at: i64,
}

/// `payload_json` by row state: the whole write until central answers, then
/// the body-free answered record.
pub(crate) enum StoredPayload {
    Unanswered(OutboxQueuedWrite),
    Answered(AnsweredWrite),
}

pub(super) struct DecodedRow {
    pub(super) state: OutboxItemState,
    pub(super) caller: Caller,
    pub(super) payload: StoredPayload,
    pub(super) status: OutboxRequestStatus,
}

pub(super) async fn read_decoded_outbox(
    connection: &mut sqlx::SqliteConnection,
    request: &RequestId,
) -> Result<Option<DecodedRow>, OutboxStoreError> {
    let request_id = request.as_str();
    sqlx::query_as!(
        OutboxRow,
        r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json
               FROM outbox_items WHERE request_id = ?"#,
        request_id
    )
    .fetch_optional(connection)
    .await?
    .as_ref()
    .map(decode_row)
    .transpose()
}

pub(super) fn decode_row(row: &OutboxRow) -> Result<DecodedRow, OutboxStoreError> {
    let corrupt = |field| OutboxStoreError::CorruptItem {
        request_id: row.request_id.clone(),
        field,
    };
    parse_request_id(&row.request_id)?;
    let caller = serde_json::from_str(&row.caller_json).map_err(|_| corrupt("caller_json"))?;
    let state = OutboxItemState::parse(&row.item_state).ok_or_else(|| corrupt("item_state"))?;
    let payload = match state {
        OutboxItemState::InOutbox
        | OutboxItemState::SentAnswerLost
        | OutboxItemState::Sent
        | OutboxItemState::Resent => {
            let payload: OutboxQueuedWrite =
                serde_json::from_str(&row.payload_json).map_err(|_| corrupt("payload_json"))?;
            if payload.request_id().map(RequestId::as_str) != Some(row.request_id.as_str()) {
                return Err(corrupt("payload_json"));
            }
            outbox_content(&row.request_id, &payload)?;
            StoredPayload::Unanswered(payload)
        }
        OutboxItemState::Answered
        | OutboxItemState::Refused
        | OutboxItemState::NotSent
        | OutboxItemState::Unconfirmed => {
            let answered: AnsweredWrite =
                serde_json::from_str(&row.payload_json).map_err(|_| corrupt("payload_json"))?;
            if answered.request_id.as_str() != row.request_id {
                return Err(corrupt("payload_json"));
            }
            StoredPayload::Answered(answered)
        }
    };
    let status = match (state, row.outcome_json.as_deref()) {
        (OutboxItemState::InOutbox | OutboxItemState::SentAnswerLost, None) => {
            OutboxRequestStatus::InOutbox {
                flush_at_ms: row.flush_at_ms,
            }
        }
        (OutboxItemState::Sent | OutboxItemState::Resent, None) => OutboxRequestStatus::Sent,
        (OutboxItemState::Answered, Some(outcome)) => OutboxRequestStatus::Answered(
            serde_json::from_str::<ReplyPayload>(outcome).map_err(|_| corrupt("outcome_json"))?,
        ),
        (OutboxItemState::Refused, Some(outcome)) => OutboxRequestStatus::Refused(
            serde_json::from_str(outcome).map_err(|_| corrupt("outcome_json"))?,
        ),
        (ended @ (OutboxItemState::NotSent | OutboxItemState::Unconfirmed), None) => {
            OutboxRequestStatus::Ended {
                reason: ended.end_reason().ok_or_else(|| corrupt("item_state"))?,
            }
        }
        (
            OutboxItemState::InOutbox
            | OutboxItemState::SentAnswerLost
            | OutboxItemState::Sent
            | OutboxItemState::Resent
            | OutboxItemState::NotSent
            | OutboxItemState::Unconfirmed,
            Some(_),
        )
        | (OutboxItemState::Answered | OutboxItemState::Refused, None) => {
            return Err(corrupt("outcome_json"));
        }
    };
    if let StoredPayload::Answered(profile) = &payload
        && !profile.matches_delivery_receipt(&status)
    {
        return Err(corrupt("payload_json.delivery_outcome"));
    }
    Ok(DecodedRow {
        state,
        caller,
        payload,
        status,
    })
}

/// Decodes a row read by caller and checks it really is that caller's.
pub(super) fn decode_owned_row(
    row: &OutboxRow,
    caller: &Caller,
) -> Result<DecodedRow, OutboxStoreError> {
    let stored = decode_row(row)?;
    if stored.caller != *caller {
        return Err(corrupt_row(&row.request_id, "caller_json"));
    }
    Ok(stored)
}

pub(super) fn outbox_content(
    request_id: &str,
    payload: &OutboxQueuedWrite,
) -> Result<OutboxWriteContent, OutboxStoreError> {
    payload
        .content()
        .ok_or_else(|| corrupt_row(request_id, "payload_json"))
}

/// The digest central's request log keys this write's content by.
pub(super) fn payload_content_digest(
    payload: &OutboxQueuedWrite,
) -> Result<String, OutboxStoreError> {
    let digest = match payload {
        OutboxQueuedWrite::Workspace(request) => operation_content_digest(&request.operation),
        OutboxQueuedWrite::SendMessage { message, .. } => operation_content_digest(message),
    };
    digest.map_err(|_| OutboxStoreError::Digest)
}

pub(super) fn corrupt_row(request_id: &str, field: &'static str) -> OutboxStoreError {
    OutboxStoreError::CorruptItem {
        request_id: request_id.to_owned(),
        field,
    }
}

pub(super) fn parse_request_id(value: &str) -> Result<RequestId, OutboxStoreError> {
    value.parse().map_err(|_| OutboxStoreError::CorruptItem {
        request_id: value.to_owned(),
        field: "request_id",
    })
}

pub(super) fn expect_one(
    changed: u64,
    request: &RequestId,
    expected: &'static str,
) -> Result<(), OutboxStoreError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(OutboxStoreError::UnexpectedState {
            request: request.clone(),
            expected,
        })
    }
}
