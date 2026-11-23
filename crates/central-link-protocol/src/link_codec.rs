use crate::{
    CentralCall, CentralCallReply, CorrelationId, DeliveryMessage, FrameDirection, LinkRequest,
    LinkRequestBatch, MacCall, MacCallReply, MachineTokenText, ProtocolError, ReceiptBatch,
    ReceiptResult, ReplyPayload, RequestStatusAnswer, RequestStatusQuery, TimeLeft,
    TokenRenewalResult, TypedCallResult,
    link_typed_calls::{named_map, unregistered_call},
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};
use std::fmt;
use workspace_ids::MachineId;
use workspace_model::{Caller, ResolvedSender, Timestamp};

/// Heartbeats are literal text, so Cloudflare can answer them without waking Rust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkFrame {
    Ping,
    Pong,
    Data(DataFrame),
}

macro_rules! define_data_frame {
    ($($variants:tt)*) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
        #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
        pub enum DataFrame { $($variants)* }

        #[derive(Deserialize)]
        #[serde(remote = "DataFrame", tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
        enum DataFrameReader { $($variants)* }

        impl<'de> Deserialize<'de> for DataFrame {
            fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
                struct NamedFrame;
                impl<'de> Visitor<'de> for NamedFrame {
                    type Value = DataFrame;
                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("named frame fields") }
                    fn visit_map<Fields: MapAccess<'de>>(self, fields: Fields) -> Result<Self::Value, Fields::Error> {
                        DataFrameReader::deserialize(MapAccessDeserializer::new(fields))
                    }
                }
                input.deserialize_map(NamedFrame)
            }
        }
    };
}

define_data_frame! {
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
    CallCentral {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        request: Box<CentralCall>,
    },
    CentralCallReply {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        result: Box<TypedCallResult<CentralCallReply>>,
    },
    CallMac {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        request: Box<MacCall>,
    },
    MacCallReply {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        result: Box<TypedCallResult<MacCallReply>>,
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
                DataFrame::CallCentral { .. }
                | DataFrame::MacCallReply { .. }
                | DataFrame::Request { .. }
                | DataFrame::Batch { .. }
                | DataFrame::RequestStatus { .. }
                | DataFrame::Receipts { .. }
                | DataFrame::RenewMachineToken { .. } => direction == FrameDirection::ToCentral,
                DataFrame::CallMac { .. }
                | DataFrame::CentralCallReply { .. }
                | DataFrame::Welcome { .. }
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
        _ => {
            let mut parser = serde_json::Deserializer::from_str(text);
            let data = match named_map(&mut parser) {
                Ok(data) => data,
                Err(error) => match unregistered_call(text) {
                    Some((call_direction, correlation_id)) if call_direction == direction => {
                        return Err(ProtocolError::UnknownTypedCall { correlation_id });
                    }
                    Some(_) => return Err(ProtocolError::WrongDirection),
                    None => return Err(error.into()),
                },
            };
            parser.end()?;
            LinkFrame::Data(data)
        }
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
