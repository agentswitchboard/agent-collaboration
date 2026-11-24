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
        host.post(
            self.path,
            &self.body,
            self.headers(person, &fresh_request_id(NOW_SECONDS)?, &|_| {})?,
        )
        .await
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
