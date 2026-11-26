//! Approval calls made through the person's WebSocket.

use serde_json::{Value, json};

use crate::ConformanceResult;
use crate::signin_fixtures::{
    ORGANIZATION, Person,
    approval_harness::PersonApprovalHarness,
    approval_socket::{DECIDE, LIST, PersonApprovalSocket, READ, SocketApprovalReply},
};

pub(super) struct ApprovalCall {
    method: &'static str,
    body: Value,
}

impl ApprovalCall {
    pub(super) async fn send(
        &self,
        host: &impl PersonApprovalHarness,
        person: &Person,
    ) -> ConformanceResult<SocketApprovalReply> {
        let mut socket = host.person_socket().connect(person).await?;
        self.call(&mut socket).await
    }

    pub(super) async fn call(
        &self,
        socket: &mut PersonApprovalSocket,
    ) -> ConformanceResult<SocketApprovalReply> {
        socket.call(self.method, ORGANIZATION, &self.body).await
    }
}

pub(super) fn list(filter: &str, cursor: Option<&str>) -> ApprovalCall {
    let mut body = json!({"filter":filter});
    if let (Some(cursor), Some(fields)) = (cursor, body.as_object_mut()) {
        fields.insert("cursor".to_owned(), json!(cursor));
    }
    ApprovalCall { method: LIST, body }
}

pub(super) fn read(request: &str) -> ApprovalCall {
    ApprovalCall {
        method: READ,
        body: json!({"requestId":request}),
    }
}

pub(super) fn decide(request: &str, operation: &str, answer: &Value) -> ApprovalCall {
    ApprovalCall {
        method: DECIDE,
        body: json!({"requestId":request,"operationId":operation,"answer":answer}),
    }
}
