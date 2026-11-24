//! Legacy Central probe frames are decoded only by the explicit link fixture.
use crate::link_typed_calls::{
    named_map,
    test_calls::{ProbeReply, ProbeRequest},
};
use crate::{CorrelationId, TypedCallResult};
use fleet_rpc::Envelope;
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;

pub type ProbeRequestEnvelope = Envelope<MachineId, ProbeRequest>;
pub type ProbeReplyEnvelope = Envelope<MachineId, ProbeReply>;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "call", rename_all = "camelCase", deny_unknown_fields)]
#[serde(bound(deserialize = "Body: Deserialize<'de>"))]
enum ProbeFrame<Body> {
    CentralProbe {
        #[serde(deserialize_with = "named_map")]
        envelope: Body,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ProbeRequestFrame {
    CallCentral {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        request: ProbeFrame<ProbeRequestEnvelope>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ProbeReplyFrame {
    CentralCallReply {
        correlation_id: CorrelationId,
        #[serde(deserialize_with = "named_map")]
        result: TypedCallResult<ProbeFrame<ProbeReplyEnvelope>>,
    },
}

pub fn decode_probe_call(
    text: &str,
) -> Result<(CorrelationId, ProbeRequestEnvelope), serde_json::Error> {
    let mut decoder = serde_json::Deserializer::from_str(text);
    let ProbeRequestFrame::CallCentral {
        correlation_id,
        request: ProbeFrame::CentralProbe { envelope },
    } = named_map(&mut decoder)?;
    decoder.end()?;
    Ok((correlation_id, envelope))
}

pub fn encode_probe_call(
    correlation_id: CorrelationId,
    envelope: ProbeRequestEnvelope,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&ProbeRequestFrame::CallCentral {
        correlation_id,
        request: ProbeFrame::CentralProbe { envelope },
    })
}

pub fn decode_probe_reply(
    text: &str,
) -> Result<(CorrelationId, TypedCallResult<ProbeReplyEnvelope>), serde_json::Error> {
    let mut decoder = serde_json::Deserializer::from_str(text);
    let ProbeReplyFrame::CentralCallReply {
        correlation_id,
        result,
    } = named_map(&mut decoder)?;
    decoder.end()?;
    let result = match result {
        TypedCallResult::Answered {
            reply: ProbeFrame::CentralProbe { envelope },
        } => TypedCallResult::Answered { reply: envelope },
        TypedCallResult::Refused { reason } => TypedCallResult::Refused { reason },
    };
    Ok((correlation_id, result))
}

pub fn encode_probe_reply(
    correlation_id: CorrelationId,
    result: TypedCallResult<ProbeReplyEnvelope>,
) -> Result<String, serde_json::Error> {
    let result = match result {
        TypedCallResult::Answered { reply } => TypedCallResult::Answered {
            reply: ProbeFrame::CentralProbe { envelope: reply },
        },
        TypedCallResult::Refused { reason } => TypedCallResult::Refused { reason },
    };
    serde_json::to_string(&ProbeReplyFrame::CentralCallReply {
        correlation_id,
        result,
    })
}
