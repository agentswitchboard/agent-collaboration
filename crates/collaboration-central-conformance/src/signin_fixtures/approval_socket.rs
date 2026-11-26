//! Drive the production person socket with independent wire inputs and exact correlation.

use futures_util::SinkExt;
use reqwest_websocket::{Message, WebSocket};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

use super::{Person, fresh_request_id, person_link};
use crate::ConformanceResult;

pub const LIST: &str = "listPersonApprovals";
pub const READ: &str = "readPersonApproval";
pub const DECIDE: &str = "decidePersonApproval";

pub struct PersonApprovalEndpoint<'endpoint> {
    pub client: &'endpoint reqwest::Client,
    pub origin: &'endpoint str,
    pub now_seconds: i64,
}

impl PersonApprovalEndpoint<'_> {
    pub async fn connect(&self, person: &Person) -> ConformanceResult<PersonApprovalSocket> {
        self.connect_with_lifetime(person, 300).await
    }

    pub async fn connect_with_lifetime(
        &self,
        person: &Person,
        lifetime_seconds: i64,
    ) -> ConformanceResult<PersonApprovalSocket> {
        let socket = person_link::dial(
            self.client,
            &format!(
                "{}/v1/organizations/{}/person-link",
                self.origin, person.organization
            ),
            &person_link::person_link_bearer(
                person,
                self.now_seconds,
                lifetime_seconds,
                lifetime_seconds,
            )?,
        )
        .await?;
        Ok(PersonApprovalSocket {
            socket,
            now_seconds: self.now_seconds,
        })
    }
}

pub struct PersonApprovalSocket {
    pub socket: WebSocket,
    now_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketApprovalReply {
    Answer(Vec<u8>),
    Refused { reason: String },
    Closed { code: u16 },
}

impl SocketApprovalReply {
    pub fn answer(self) -> ConformanceResult<Vec<u8>> {
        match self {
            Self::Answer(body) => Ok(body),
            refusal => Err(format!("expected a person approval answer, got {refusal:?}").into()),
        }
    }
}

impl PersonApprovalSocket {
    pub async fn call(
        &mut self,
        call: &str,
        organization: &str,
        body: &Value,
    ) -> ConformanceResult<SocketApprovalReply> {
        let correlation = fresh_request_id(self.now_seconds)?;
        exchange(&mut self.socket, &correlation, call, organization, body).await
    }
}

pub async fn exchange(
    socket: &mut WebSocket,
    correlation: &str,
    call: &str,
    organization: &str,
    body: &Value,
) -> ConformanceResult<SocketApprovalReply> {
    let request = json!({
        "kind": "callCentral",
        "correlationId": correlation,
        "request": {"call":call, "envelope":{"target": organization, "body":body}},
    });
    socket.send(Message::Text(request.to_string())).await?;
    let frame = person_link::read(socket, "correlated person approval reply").await?;
    let wire = match frame {
        Message::Text(wire) => wire,
        Message::Close { code, .. } => {
            return Ok(SocketApprovalReply::Closed { code: code.into() });
        }
        other => return Err(format!("unexpected person approval frame: {other:?}").into()),
    };
    // Read the outer tag and the raw outcome separately. Internally tagged Serde
    // enums buffer the content and require a second read to preserve raw JSON.
    let fields: Value = serde_json::from_str(&wire)?;
    if fields.get("kind").and_then(Value::as_str) != Some("centralCallReply")
        || fields.get("correlationId").and_then(Value::as_str) != Some(correlation)
    {
        return Err(format!(
            "expected correlated person approval result for {correlation}: {wire}"
        )
        .into());
    }
    match fields.pointer("/result/kind").and_then(Value::as_str) {
        Some("answered") => {
            if fields.pointer("/result/reply/call").and_then(Value::as_str) != Some(call)
                || fields
                    .pointer("/result/reply/envelope/target")
                    .and_then(Value::as_str)
                    != Some(organization)
            {
                return Err(format!("expected {call} result for {organization}: {wire}").into());
            }
            let raw: RawApprovalFrame<'_> = serde_json::from_str(&wire)?;
            Ok(SocketApprovalReply::Answer(
                raw.result.reply.envelope.body.get().as_bytes().to_vec(),
            ))
        }
        Some("refused") => {
            let reason = fields
                .pointer("/result/reason/kind")
                .and_then(Value::as_str)
                .ok_or("missing person approval refusal reason")?;
            Ok(SocketApprovalReply::Refused {
                reason: reason.to_owned(),
            })
        }
        _ => Err(format!("unrecognized person approval result: {wire}").into()),
    }
}

#[derive(Deserialize)]
struct RawApprovalFrame<'wire> {
    #[serde(borrow)]
    result: RawApprovalResult<'wire>,
}

#[derive(Deserialize)]
struct RawApprovalResult<'wire> {
    #[serde(borrow)]
    reply: RawApprovalReply<'wire>,
}

#[derive(Deserialize)]
struct RawApprovalReply<'wire> {
    #[serde(borrow)]
    envelope: RawApprovalBody<'wire>,
}

#[derive(Deserialize)]
struct RawApprovalBody<'wire> {
    #[serde(borrow)]
    body: &'wire RawValue,
}
