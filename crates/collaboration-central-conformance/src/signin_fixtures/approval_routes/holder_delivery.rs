//! Observe each committed person decision through the real machine holder link.

use approval_request_protocol::{Ending, SessionCallDecision};
use central_storage::{SqlValue, Statement};
use serde_json::{Value, json};

use super::{
    AnswerClass, NOW_SECONDS, OWNER, OWNER_SUBJECT, Person, REVISIONS, decide_in_organization,
    expect, expect_status, fresh_request_id, saved,
};
use crate::{
    ConformanceResult,
    approval_link_scenarios::{
        ApprovalLinkFixture, heartbeat, receive_decision, registered_waiting_request,
    },
    signin_fixtures::{
        POLICY,
        approval_socket::{self, PersonApprovalEndpoint, SocketApprovalReply},
    },
};

pub async fn check_socket_holder_delivery(
    fixture: &impl ApprovalLinkFixture,
    endpoint: PersonApprovalEndpoint<'_>,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> ConformanceResult<(u16, Vec<u8>)>,
) -> ConformanceResult<()> {
    let (target, mut holder, request) = registered_waiting_request(fixture).await?;
    let organization = target.organization_id.as_str();
    let now = fixture.now_seconds();
    let person = Person::owner(organization, now)?;
    publish_floors(organization, &post).await?;

    let operation = fresh_request_id(now)?;
    let answer = AnswerClass::HeldStartAllow.answer();
    let body = json!({"requestId":request.request_id(),"operationId":operation,"answer":answer});
    for (call, input) in [
        (approval_socket::LIST, json!({"filter":"all"})),
        (
            approval_socket::READ,
            json!({"requestId":request.request_id()}),
        ),
        (approval_socket::DECIDE, body.clone()),
    ] {
        assert_eq!(
            approval_socket::exchange(
                &mut holder,
                &fresh_request_id(now)?,
                call,
                organization,
                &input
            )
            .await?,
            SocketApprovalReply::Refused {
                reason: "wrongTarget".to_owned()
            },
        );
    }

    let mut socket = endpoint.connect(&person).await?;
    let first = socket
        .call(approval_socket::DECIDE, organization, &body)
        .await?
        .answer()?;
    expect((200, first.clone()), 200, saved(&answer))?;
    receive_decision(
        fixture,
        &mut holder,
        target.clone(),
        &request,
        person_ending()?,
    )
    .await?;
    let replay = socket
        .call(approval_socket::DECIDE, organization, &body)
        .await?
        .answer()?;
    assert_eq!(replay, first);
    heartbeat(&mut holder).await
}

pub async fn check_signed_holder_delivery_after_publication_failure(
    fixture: &impl ApprovalLinkFixture,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> ConformanceResult<(u16, Vec<u8>)>,
    arm_publication_failure: impl AsyncFnOnce() -> ConformanceResult<()>,
    observe_ending: impl EndingObserver,
) -> ConformanceResult<()> {
    check_publication_failure(fixture, post, arm_publication_failure, observe_ending).await
}

type HolderTarget = central_link_protocol::approval_rpc::ApprovalHolderTarget;

pub trait EndingObserver:
    for<'socket, 'request> AsyncFnOnce(
    &'socket mut reqwest_websocket::WebSocket,
    HolderTarget,
    &'request approval_request_protocol::Raise,
    Ending,
) -> ConformanceResult<()>
{
}

impl<Observer> EndingObserver for Observer where
    Observer: for<'socket, 'request> AsyncFnOnce(
        &'socket mut reqwest_websocket::WebSocket,
        HolderTarget,
        &'request approval_request_protocol::Raise,
        Ending,
    ) -> ConformanceResult<()>
{
}

async fn check_publication_failure(
    fixture: &impl ApprovalLinkFixture,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> ConformanceResult<(u16, Vec<u8>)>,
    before_decide: impl AsyncFnOnce() -> ConformanceResult<()>,
    observe_ending: impl EndingObserver,
) -> ConformanceResult<()> {
    let (target, mut socket, request) = registered_waiting_request(fixture).await?;
    let organization = target.organization_id.as_str();
    let owner = Person::owner(organization, NOW_SECONDS)?;
    publish_floors(organization, &post).await?;

    let answer = AnswerClass::HeldStartAllow.answer();
    let operation = fresh_request_id(NOW_SECONDS)?;
    let signed = decide_in_organization(
        organization,
        request.request_id().as_str(),
        &operation,
        &answer,
    );
    before_decide().await?;

    let headers = signed.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
    let first_reply = post(signed.path, &signed.body, headers).await?;
    if !expect_status(first_reply, 503)?.is_empty() {
        return Err("a failed publication must return an empty 503".into());
    }

    // The first call must have released the ending. Do not retry to prompt delivery.
    observe_ending(&mut socket, target, &request, person_ending()?).await?;

    let answer_query = Statement {
        sql: "SELECT answer FROM runtime_request_log WHERE request_id = ?1",
        args: vec![SqlValue::Text(operation)],
    };
    let retained = fixture.read(answer_query.clone()).await?;
    let [row] = retained.rows.as_slice() else {
        return Err("the committed operation is missing its retained answer".into());
    };
    let [SqlValue::Text(answer_bytes)] = row.as_slice() else {
        return Err("the retained answer is not stored as text".into());
    };
    let headers = signed.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
    let replay = post(signed.path, &signed.body, headers).await?;
    if replay.1.as_slice() != answer_bytes.as_bytes() {
        return Err("a fresh proof must return exactly the committed response bytes".into());
    }
    expect(replay, 200, saved(&answer))?;
    if fixture.read(answer_query).await?.rows != retained.rows {
        return Err("the exact replay changed the retained result".into());
    }
    heartbeat(&mut socket).await
}

fn person_ending() -> ConformanceResult<Ending> {
    Ok(Ending::UnprovenCallDecided {
        decision: SessionCallDecision::Allow,
        person_id: OWNER.parse()?,
    })
}

async fn publish_floors(
    organization: &str,
    post: &impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> ConformanceResult<(u16, Vec<u8>)>,
) -> ConformanceResult<()> {
    for floor in [
        json!({
            "kind": "organization", "organizationId": organization, "revision": 3,
            "state": "active", "permissionPolicyDigest": POLICY,
        }),
        json!({
            "kind": "person", "organizationId": organization, "personId": OWNER,
            "subjectId": OWNER_SUBJECT, "revision": 4, "state": "active",
        }),
    ] {
        expect_status(post(REVISIONS, &floor, Vec::new()).await?, 200)?;
    }
    Ok(())
}
