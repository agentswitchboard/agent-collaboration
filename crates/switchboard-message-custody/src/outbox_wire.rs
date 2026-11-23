//! The one projection from Part A's claimed write to the existing wire request.

use central_link_client::{
    LINK_LANE_FRAME_BYTES, LaneFrameSizeError, WRITE_BATCH_ENVELOPE_BYTES, encoded_request_size,
};
use central_link_protocol::{FreshCorrelationId, LinkRequest, ProtocolError, RequestPayload};
use switchboard_machine_core_store::OutboxQueuedWrite;
use workspace_model::{
    Caller, Timestamp,
    central_mail::{CentralMailOperation, CentralMailRequest},
};

pub(crate) fn request_for_write(
    caller: &Caller,
    write: &OutboxQueuedWrite,
    sent_at: Timestamp,
    correlation_time_ms: i64,
) -> Result<LinkRequest, ProtocolError> {
    let payload = match write {
        OutboxQueuedWrite::Workspace(request) => RequestPayload::Workspace(request.clone()),
        OutboxQueuedWrite::SendMessage {
            request_id,
            message,
        } => RequestPayload::Mail(CentralMailRequest {
            request_id: request_id.clone(),
            operation: CentralMailOperation::SendMessage {
                to: message.to.clone(),
                body: message.body.clone(),
                delivery: message.delivery,
                reply_to: message.reply_to.clone(),
                sent_at,
            },
        }),
    };
    let correlation = FreshCorrelationId::mint_at(correlation_time_ms)
        .wire_id()
        .clone();
    LinkRequest::new(correlation, caller.clone(), payload)
}

pub(crate) fn fits_one_member(request: &LinkRequest) -> Result<bool, LaneFrameSizeError> {
    let encoded = encoded_request_size(request)?;
    Ok(WRITE_BATCH_ENVELOPE_BYTES.saturating_add(encoded) <= LINK_LANE_FRAME_BYTES)
}
