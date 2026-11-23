//! Checked row decoding for the Mac outbox.
use super::OutboxStoreError;
use crate::{
    OutboxQueuedWrite, OutboxRequestStatus, OutboxWriteContent, RowCorruption, RowCorruptionReason,
    StoreScan,
    outbox_item_state::OutboxItemState,
    outbox_receipt_profile::AnsweredWrite,
    outbox_settlement::{SettledWrite, is_permanent_answer, is_permanent_refusal},
    store_scan::diagnostic_key,
};
use workspace_ids::RequestId;
use workspace_model::{Caller, Timestamp};
pub(super) struct OutboxRow {
    pub(super) request_id: Vec<u8>,
    pub(super) caller_json: Vec<u8>,
    pub(super) payload_json: Vec<u8>,
    pub(super) item_state: Vec<u8>,
    pub(super) flush_at_ms: i64,
    pub(super) outcome_json: Option<Vec<u8>>,
    pub(super) created_at: i64,
    pub(super) expires_at_ms: i64,
    pub(super) updated_at: i64,
    pub(super) row_id: i64,
    pub(super) key_is_text: bool,
    pub(super) integer_fault: i64,
}

/// `payload_json` by row state: the whole write until central answers, then
/// the body-free answered record.
pub(crate) enum StoredPayload {
    Unanswered(OutboxQueuedWrite),
    Answered(AnsweredWrite),
}

pub(super) struct DecodedRow {
    pub(super) request: RequestId,
    pub(super) sent_at: Timestamp,
    pub(super) deadline_ms: i64,
    pub(super) flush_at_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) state: OutboxItemState,
    pub(super) caller: Caller,
    pub(super) payload: StoredPayload,
    pub(super) status: OutboxRequestStatus,
}

// Read SQLite TEXT as bytes so malformed UTF-8 reaches the per-row decoder.
macro_rules! select_outbox_rows {
    ($clause:literal $(, $argument:expr)* $(,)?) => {
        sqlx::query_as!(
            $crate::outbox_store::outbox_rows::OutboxRow,
            r#"SELECT CAST(request_id AS BLOB) AS "request_id!: Vec<u8>",
                  CAST(caller_json AS BLOB) AS "caller_json!: Vec<u8>",
                  CAST(payload_json AS BLOB) AS "payload_json!: Vec<u8>",
                  CAST(item_state AS BLOB) AS "item_state!: Vec<u8>", flush_at_ms,
                  CAST(outcome_json AS BLOB) AS "outcome_json?: Vec<u8>",
                  CAST(created_at AS INTEGER) AS "created_at!: i64",
                  expires_at_ms, updated_at, rowid AS "row_id!: i64",
                  (typeof(request_id) = 'text') AS "key_is_text!: bool",
                  CASE
                    WHEN typeof(created_at) <> 'integer' THEN 1
                    WHEN typeof(updated_at) <> 'integer' THEN 2
                    WHEN typeof(flush_at_ms) <> 'integer' THEN 3
                    WHEN typeof(expires_at_ms) <> 'integer' THEN 4
                    ELSE 0 END AS "integer_fault!: i64"
           FROM outbox_items "# + $clause
            $(, $argument)*
        )
    };
}

pub(super) use select_outbox_rows;

pub(super) async fn read_row(
    connection: &mut sqlx::SqliteConnection,
    request: &RequestId,
) -> Result<Option<OutboxRow>, OutboxStoreError> {
    Ok(
        select_outbox_rows!(r#"WHERE request_id = ?"#, request.as_str(),)
            .fetch_optional(connection)
            .await?,
    )
}

pub(super) async fn read_owned_outbox(
    connection: &mut sqlx::SqliteConnection,
    caller: &Caller,
    request: &RequestId,
) -> Result<Option<DecodedRow>, OutboxStoreError> {
    let caller_json = serde_json::to_string(caller)?;
    select_outbox_rows!(
        r#"WHERE request_id = ? AND caller_json = ?"#,
        request.as_str(),
        caller_json,
    )
    .fetch_optional(connection)
    .await?
    .as_ref()
    .map(decode_row)
    .transpose()
    .map_err(Into::into)
}

pub(super) async fn read_rows_in_states(
    connection: &mut sqlx::SqliteConnection,
    first: OutboxItemState,
    second: OutboxItemState,
) -> Result<StoreScan<Vec<DecodedRow>>, OutboxStoreError> {
    Ok(select_outbox_rows!(
        r#"WHERE item_state IN (?, ?) ORDER BY created_at, request_id"#,
        first.as_str(),
        second.as_str(),
    )
    .fetch_all(connection)
    .await?
    .iter()
    .map(decode_row)
    .collect())
}

pub(super) async fn waiting_rows(
    connection: &mut sqlx::SqliteConnection,
) -> Result<StoreScan<Vec<DecodedRow>>, OutboxStoreError> {
    read_rows_in_states(
        connection,
        OutboxItemState::InOutbox,
        OutboxItemState::SentAnswerLost,
    )
    .await
}

pub(super) async fn in_flight_rows(
    connection: &mut sqlx::SqliteConnection,
) -> Result<StoreScan<Vec<DecodedRow>>, OutboxStoreError> {
    read_rows_in_states(connection, OutboxItemState::Sent, OutboxItemState::Resent).await
}

const CLAIM_SCAN_PAGE_SIZE: i64 = 128;

pub(super) struct WaitingScanPosition {
    created_at: i64,
    request_id: Vec<u8>,
    row_id: i64,
}

pub(super) async fn waiting_page(
    connection: &mut sqlx::SqliteConnection,
    after: Option<&WaitingScanPosition>,
) -> Result<(StoreScan<Vec<DecodedRow>>, Option<WaitingScanPosition>), OutboxStoreError> {
    let last_created_at = after.map(|position| position.created_at);
    let last_request_id = after.map(|position| position.request_id.as_slice());
    let last_row_id = after.map(|position| position.row_id);
    let rows = select_outbox_rows!(
        r#"WHERE item_state IN (?, ?)
               AND (? IS NULL OR (CAST(created_at AS INTEGER), CAST(request_id AS BLOB), rowid) > (?, ?, ?))
           ORDER BY CAST(created_at AS INTEGER), CAST(request_id AS BLOB), rowid LIMIT ?"#,
        OutboxItemState::InOutbox.as_str(),
        OutboxItemState::SentAnswerLost.as_str(),
        last_created_at,
        last_created_at,
        last_request_id,
        last_row_id,
        CLAIM_SCAN_PAGE_SIZE,
    )
    .fetch_all(connection)
    .await?;
    // The cursor comes from the last physical row, even when decoding fails.
    let next = rows.last().map(|row| WaitingScanPosition {
        created_at: row.created_at,
        request_id: row.request_id.clone(),
        row_id: row.row_id,
    });
    Ok((rows.iter().map(decode_row).collect(), next))
}

pub(super) fn decode_row(row: &OutboxRow) -> Result<DecodedRow, RowCorruption> {
    use RowCorruptionReason::{
        DeadlineMismatch, IncompatibleOutcome, InconsistentState, InvalidIdentifier, InvalidJson,
        InvalidStorageType, KeyMismatch, TimestampOutOfRange, UnknownState, UnsupportedOperation,
    };
    let corrupt = |field, reason| RowCorruption {
        key: diagnostic_key(&row.request_id),
        field,
        reason,
    };
    if !row.key_is_text {
        return Err(corrupt("request_id", InvalidIdentifier));
    }
    let key = std::str::from_utf8(&row.request_id)
        .map_err(|_| corrupt("request_id", InvalidIdentifier))?;
    let request: RequestId = key
        .parse()
        .map_err(|_| corrupt("request_id", InvalidIdentifier))?;
    let invalid_integer = match row.integer_fault {
        1 => Some("created_at"),
        2 => Some("updated_at"),
        3 => Some("flush_at_ms"),
        4 => Some("expires_at_ms"),
        _ => None,
    };
    if let Some(field) = invalid_integer {
        return Err(corrupt(field, InvalidStorageType));
    }
    let sent_at = Timestamp::from_timestamp_millis(row.created_at)
        .ok_or_else(|| corrupt("created_at", TimestampOutOfRange))?;
    for (value, field) in [
        (row.flush_at_ms, "flush_at_ms"),
        (row.updated_at, "updated_at"),
    ] {
        Timestamp::from_timestamp_millis(value)
            .ok_or_else(|| corrupt(field, TimestampOutOfRange))?;
    }
    let deadline_ms = crate::outbox_deadline::expires_at_ms(row.created_at);
    if row.expires_at_ms != deadline_ms {
        return Err(corrupt(
            "expires_at_ms",
            DeadlineMismatch {
                expected_ms: deadline_ms,
                stored_ms: row.expires_at_ms,
            },
        ));
    }
    let caller = serde_json::from_slice(&row.caller_json)
        .map_err(|_| corrupt("caller_json", InvalidJson))?;
    let state = std::str::from_utf8(&row.item_state)
        .ok()
        .and_then(OutboxItemState::parse)
        .ok_or_else(|| corrupt("item_state", UnknownState))?;
    let payload = match state {
        OutboxItemState::InOutbox
        | OutboxItemState::SentAnswerLost
        | OutboxItemState::Sent
        | OutboxItemState::Resent => {
            let payload: OutboxQueuedWrite = serde_json::from_slice(&row.payload_json)
                .map_err(|_| corrupt("payload_json", InvalidJson))?;
            if payload.request_id().map(RequestId::as_str) != Some(key) {
                return Err(corrupt("payload_json", KeyMismatch));
            }
            payload
                .content()
                .ok_or_else(|| corrupt("payload_json", UnsupportedOperation))?;
            StoredPayload::Unanswered(payload)
        }
        OutboxItemState::Answered
        | OutboxItemState::Refused
        | OutboxItemState::NotSent
        | OutboxItemState::Unconfirmed => {
            let answered: AnsweredWrite = serde_json::from_slice(&row.payload_json)
                .map_err(|_| corrupt("payload_json", InvalidJson))?;
            if answered.request_id != request {
                return Err(corrupt("payload_json", KeyMismatch));
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
            serde_json::from_slice(outcome).map_err(|_| corrupt("outcome_json", InvalidJson))?,
        ),
        (OutboxItemState::Refused, Some(outcome)) => OutboxRequestStatus::Refused(
            serde_json::from_slice(outcome).map_err(|_| corrupt("outcome_json", InvalidJson))?,
        ),
        (OutboxItemState::Unconfirmed, Some(outcome)) => {
            serde_json::from_slice::<SettledWrite>(outcome)
                .map_err(|_| corrupt("outcome_json", InvalidJson))?
                .into_request_status()
        }
        (ended @ (OutboxItemState::NotSent | OutboxItemState::Unconfirmed), None) => {
            OutboxRequestStatus::Ended {
                reason: ended
                    .end_reason()
                    .ok_or_else(|| corrupt("item_state", InconsistentState))?,
            }
        }
        (
            OutboxItemState::InOutbox
            | OutboxItemState::SentAnswerLost
            | OutboxItemState::Sent
            | OutboxItemState::Resent
            | OutboxItemState::NotSent,
            Some(_),
        )
        | (OutboxItemState::Answered | OutboxItemState::Refused, None) => {
            return Err(corrupt("outcome_json", InconsistentState));
        }
    };
    if let StoredPayload::Answered(profile) = &payload {
        let valid_outcome = match &status {
            OutboxRequestStatus::Answered(reply) => is_permanent_answer(&profile.target, reply),
            OutboxRequestStatus::Refused(reason) => is_permanent_refusal(&profile.target, reason),
            _ => true,
        };
        if !valid_outcome {
            return Err(corrupt("outcome_json", IncompatibleOutcome));
        }
        if !profile.matches_delivery_receipt(&status) {
            return Err(corrupt(
                "payload_json.delivery_outcome",
                IncompatibleOutcome,
            ));
        }
    }
    Ok(DecodedRow {
        request,
        sent_at,
        deadline_ms,
        flush_at_ms: row.flush_at_ms,
        updated_at_ms: row.updated_at,
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
        return Err(corrupt_row(stored.request.as_str(), "caller_json"));
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
    payload
        .content_digest()
        .map_err(|_| OutboxStoreError::Digest)
}

pub(super) fn corrupt_row(request_id: &str, field: &'static str) -> OutboxStoreError {
    OutboxStoreError::CorruptItem {
        request_id: request_id.to_owned(),
        field,
    }
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
