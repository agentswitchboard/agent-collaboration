//! Person approval assertions at the socket and retained answer boundaries.

use central_link_protocol::PersonLinkCloseCode;
use reqwest_websocket::Message;
use serde_json::json;

use super::{
    AnswerClass, InitialApproval, NOW_SECONDS, ORGANIZATION, Person, PersonApprovalHarness,
    assert_count, decide, expect, expect_status, id, publish_person_floor, saved, seed,
};
use crate::{
    ConformanceResult,
    signin_fixtures::{
        approval_socket::{DECIDE, LIST, READ, SocketApprovalReply},
        fresh_request_id, person_link,
    },
};

pub(super) async fn one_connection(host: &impl PersonApprovalHarness) -> ConformanceResult<()> {
    let owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut socket = endpoint.connect(&owner).await?;

    let page = socket
        .call(LIST, ORGANIZATION, &json!({"filter":"waiting"}))
        .await?
        .answer()?;
    let page: serde_json::Value = serde_json::from_slice(&page)?;
    assert_eq!(
        page.pointer("/items/0/requestId")
            .and_then(serde_json::Value::as_str),
        Some(request.as_str())
    );

    let read = socket
        .call(READ, ORGANIZATION, &json!({"requestId":request}))
        .await?
        .answer()?;
    let read: serde_json::Value = serde_json::from_slice(&read)?;
    assert_eq!(
        read.pointer("/view/requestId")
            .and_then(serde_json::Value::as_str),
        Some(request.as_str())
    );

    let answer = AnswerClass::Once.answer();
    let body = json!({"requestId":request,"operationId":id(100),"answer":answer});
    let decided = socket.call(DECIDE, ORGANIZATION, &body).await?.answer()?;
    expect((200, decided), 200, saved(&answer))?;

    let page = socket
        .call(LIST, ORGANIZATION, &json!({"filter":"waiting"}))
        .await?
        .answer()?;
    expect(
        (200, page),
        200,
        json!({"kind":"page","items":[],"next":null}),
    )?;
    assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 0).await
}

pub(super) async fn scoped_authority(host: &impl PersonApprovalHarness) -> ConformanceResult<()> {
    const FOREIGN_ORGANIZATION: &str = "019f0000-0000-7000-8000-00000000e201";
    let mut owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut socket = endpoint.connect(&owner).await?;

    let commands = [
        (LIST, json!({"filter":"all"})),
        (READ, json!({"requestId":request})),
        (
            DECIDE,
            json!({"requestId":request,"operationId":id(100),"answer":AnswerClass::Once.answer()}),
        ),
    ];
    for (call, body) in &commands {
        assert_eq!(
            socket.call(call, FOREIGN_ORGANIZATION, body).await?,
            SocketApprovalReply::Refused {
                reason: "wrongTarget".to_owned()
            },
        );
    }

    publish_person_floor(host, 5, "active").await?;
    assert!(matches!(
        person_link::read(&mut socket.socket, "revision closes the prior person socket").await?,
        Message::Close { code, .. } if u16::from(code) == PersonLinkCloseCode::Reconnect.code()
    ));

    owner.revision = 5;
    owner.permissions.retain(|grant| grant != "approval:decide");
    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut socket = endpoint.connect(&owner).await?;
    for (call, body) in &commands {
        assert_eq!(
            socket.call(call, ORGANIZATION, body).await?,
            SocketApprovalReply::Refused {
                reason: "notPermitted".to_owned()
            },
        );
    }
    assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 0).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 0).await?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state='waiting'",
        1,
    )
    .await
}

pub(super) async fn shared_retained_answer(
    host: &impl PersonApprovalHarness,
) -> ConformanceResult<()> {
    let mut owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let answer = AnswerClass::Once.answer();
    let operation = id(100);
    let signed = decide(&request, &operation, &answer);
    let headers = signed.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
    let first = host.post(signed.path, &signed.body, headers).await?;
    let first = expect_status(first, 200)?;

    publish_person_floor(host, 5, "active").await?;
    owner.revision = 5;
    owner.permissions.retain(|grant| grant != "approval:decide");
    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut socket = endpoint.connect(&owner).await?;
    let answer_body = json!({"requestId":request,"operationId":operation,"answer":answer});
    let replay = socket
        .call(DECIDE, ORGANIZATION, &answer_body)
        .await?
        .answer()?;
    assert_eq!(replay, first);
    expect((200, replay), 200, saved(&answer))?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await?;
    assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await
}

pub(super) async fn expired_at_applying_sample(
    host: &impl PersonApprovalHarness,
) -> ConformanceResult<()> {
    let owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let answer = AnswerClass::Once.answer();
    let body = json!({"requestId":request,"operationId":id(100),"answer":answer});
    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut socket = endpoint.connect_with_lifetime(&owner, 1).await?;

    let reply = host
        .advance_at_approval_sample(socket.call(DECIDE, ORGANIZATION, &body), 1000)
        .await?;
    assert_eq!(
        reply,
        SocketApprovalReply::Closed {
            code: PersonLinkCloseCode::Reconnect.code()
        },
    );
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state='waiting'",
        1,
    )
    .await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 0).await?;

    let endpoint = host
        .person_socket()
        .ok_or("person socket fixture is required")?;
    let mut fresh = endpoint.connect(&owner).await?;
    let reply = fresh.call(DECIDE, ORGANIZATION, &body).await?.answer()?;
    expect((200, reply), 200, saved(&answer))
}
