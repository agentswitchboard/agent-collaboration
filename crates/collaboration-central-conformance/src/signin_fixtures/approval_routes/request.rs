//! Independent C3 intent fixtures for the signed HTTP scenarios.
use serde_json::{Value, json};

use crate::ConformanceResult;
use crate::signin_fixtures::approval_harness::PersonApprovalHarness;
use crate::signin_fixtures::{NOW_SECONDS, ORGANIZATION, Person, fresh_request_id};

const LIST: &str = "/organization-admin/v1/approval-list";
pub(super) const READ: &str = "/organization-admin/v1/approval-read";
const DECIDE: &str = "/organization-admin/v1/approval-decide";

pub(super) struct SignedRequest {
    pub(super) path: &'static str,
    pub(super) body: Value,
    intent: Value,
}

impl SignedRequest {
    pub(super) fn headers(
        &self,
        person: &Person,
        jti: &str,
        adjust: &dyn Fn(&mut Value),
    ) -> ConformanceResult<Vec<(&'static str, String)>> {
        person.headers_with(self.path, &self.intent, jti, NOW_SECONDS, adjust)
    }

    pub(super) async fn send(
        &self,
        host: &impl PersonApprovalHarness,
        person: &Person,
    ) -> ConformanceResult<(u16, Vec<u8>)> {
        #[cfg(feature = "session-link-test-support")]
        if let Some(endpoint) = host.person_socket() {
            return self.send_on_socket(endpoint, person).await;
        }
        host.post(
            self.path,
            &self.body,
            self.headers(person, &fresh_request_id(NOW_SECONDS)?, &|_| {})?,
        )
        .await
    }

    #[cfg(feature = "session-link-test-support")]
    async fn send_on_socket(
        &self,
        endpoint: super::super::approval_socket::PersonApprovalEndpoint<'_>,
        person: &Person,
    ) -> ConformanceResult<(u16, Vec<u8>)> {
        use super::super::approval_socket::{self, SocketApprovalReply};

        let call = match self.path {
            LIST => approval_socket::LIST,
            READ => approval_socket::READ,
            DECIDE => approval_socket::DECIDE,
            _ => return Err("unknown conformance approval operation".into()),
        };
        let mut body = self.body.clone();
        let organization = body
            .as_object_mut()
            .and_then(|fields| fields.remove("organization"))
            .ok_or("missing approval operation target")?;
        let organization = organization
            .as_str()
            .ok_or("invalid approval operation target")?;
        let mut socket = endpoint.connect(person).await?;

        // The moved domain cases share their existing body and store assertions.
        // Translate only the equivalent transport outcomes they used on HTTP.
        match socket.call(call, organization, &body).await? {
            SocketApprovalReply::Answer(body) => Ok((200, body)),
            SocketApprovalReply::Refused { reason } if reason == "notPermitted" => {
                Ok((200, serde_json::to_vec(&super::refusal(&reason))?))
            }
            SocketApprovalReply::Refused { reason } if reason == "centralFault" => {
                Ok((503, Vec::new()))
            }
            SocketApprovalReply::Closed { code: 1011 } => Ok((503, Vec::new())),
            response => Err(format!("{call}: unexpected socket outcome {response:?}").into()),
        }
    }
}

pub(super) fn list(filter: &str, cursor: Option<&str>) -> SignedRequest {
    let mut body = json!({"organization":ORGANIZATION,"filter":filter});
    if let (Some(cursor), Some(fields)) = (cursor, body.as_object_mut()) {
        fields.insert("cursor".to_owned(), json!(cursor));
    }
    SignedRequest {
        path: LIST,
        body,
        intent: json!({"v":1,"operation":"ApprovalList","organization":ORGANIZATION,
            "target":ORGANIZATION,"operationId":null,"params":{"filter":filter,"cursor":cursor}}),
    }
}

pub(super) fn read(request: &str) -> SignedRequest {
    SignedRequest {
        path: READ,
        body: json!({"organization":ORGANIZATION,"requestId":request}),
        intent: json!({"v":1,"operation":"ApprovalRead","organization":ORGANIZATION,
            "target":request,"operationId":null,"params":{}}),
    }
}

pub(super) fn decide(request: &str, operation: &str, answer: &Value) -> SignedRequest {
    decide_in_organization(ORGANIZATION, request, operation, answer)
}

pub(super) fn decide_in_organization(
    organization: &str,
    request: &str,
    operation: &str,
    answer: &Value,
) -> SignedRequest {
    SignedRequest {
        path: DECIDE,
        body: json!({
            "organization": organization, "requestId": request, "operationId": operation,
            "answer": answer,
        }),
        intent: json!({
            "v": 1, "operation": "ApprovalDecide", "organization": organization,
            "target": request, "operationId": operation, "params": {"answer": answer},
        }),
    }
}
