use crate::{
    CorrelationId, DeliveryMessage, LinkRequest, LinkRequestBatch, MachineTokenText, ProtocolError,
    ReceiptBatch, ReceiptResult, ReplyPayload, RequestStatusAnswer, RequestStatusQuery, TimeLeft,
    TokenRenewalResult,
};
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;
use workspace_model::{Caller, ResolvedSender, Timestamp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameDirection {
    ToCentral,
    ToRouter,
}

/// Heartbeats are literal text, so Cloudflare can answer them without waking Rust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkFrame {
    Ping,
    Pong,
    Data(DataFrame),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DataFrame {
    Welcome {
        machine_id: MachineId,
        central_time: Timestamp,
        effective_expires_at: Timestamp,
    },
    Request {
        request: Box<LinkRequest>,
    },
    Batch {
        requests: LinkRequestBatch,
    },
    Reply {
        correlation_id: CorrelationId,
        result: Box<ReplyPayload>,
    },
    RequestStatus {
        correlation_id: CorrelationId,
        caller: Caller,
        query: RequestStatusQuery,
    },
    RequestStatusReply {
        correlation_id: CorrelationId,
        result: Box<RequestStatusAnswer>,
    },
    /// Central to the recipient's Router: a queued direct message, pushed at
    /// once when it is accepted and again on reconnect while inside its window.
    Deliver {
        message: Box<DeliveryMessage>,
        sender: ResolvedSender,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_left: Option<TimeLeft>,
    },
    Receipts {
        correlation_id: CorrelationId,
        receipts: ReceiptBatch,
    },
    ReceiptResults {
        correlation_id: CorrelationId,
        results: Vec<ReceiptResult>,
    },
    /// Router to central: a fresh machine token for the open connection.
    RenewMachineToken {
        correlation_id: CorrelationId,
        token: MachineTokenText,
    },
    /// Central to Router: the answer to one RenewMachineToken.
    MachineTokenRenewal {
        correlation_id: CorrelationId,
        result: TokenRenewalResult,
    },
}

impl LinkFrame {
    pub fn check_direction(&self, direction: FrameDirection) -> Result<(), ProtocolError> {
        let valid = match self {
            Self::Ping => direction == FrameDirection::ToCentral,
            Self::Pong => direction == FrameDirection::ToRouter,
            Self::Data(data) => match data {
                DataFrame::Request { .. }
                | DataFrame::Batch { .. }
                | DataFrame::RequestStatus { .. }
                | DataFrame::Receipts { .. }
                | DataFrame::RenewMachineToken { .. } => direction == FrameDirection::ToCentral,
                DataFrame::Welcome { .. }
                | DataFrame::Reply { .. }
                | DataFrame::RequestStatusReply { .. }
                | DataFrame::Deliver { .. }
                | DataFrame::ReceiptResults { .. }
                | DataFrame::MachineTokenRenewal { .. } => direction == FrameDirection::ToRouter,
            },
        };
        if valid {
            if let Self::Data(DataFrame::Deliver {
                message, time_left, ..
            }) = self
            {
                TimeLeft::validate_offer(message, *time_left)?;
            }
            Ok(())
        } else {
            Err(ProtocolError::WrongDirection)
        }
    }
}

pub fn decode_text(text: &str, direction: FrameDirection) -> Result<LinkFrame, ProtocolError> {
    let frame = match text {
        "ping" => LinkFrame::Ping,
        "pong" => LinkFrame::Pong,
        _ => LinkFrame::Data(serde_json::from_str(text)?),
    };
    frame.check_direction(direction)?;
    Ok(frame)
}

pub fn encode_text(frame: &LinkFrame, direction: FrameDirection) -> Result<String, ProtocolError> {
    frame.check_direction(direction)?;
    match frame {
        LinkFrame::Ping => Ok("ping".to_owned()),
        LinkFrame::Pong => Ok("pong".to_owned()),
        LinkFrame::Data(data) => Ok(serde_json::to_string(data)?),
    }
}

#[cfg(test)]
#[path = "link_codec_test.rs"]
mod tests;
