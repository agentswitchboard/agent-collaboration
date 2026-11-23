//! Exercise a signed C3 decision from the private route through the real machine link.

use approval_request_protocol::{Ending, SessionCallDecision};
use serde_json::{Value, json};

use super::{
    AnswerClass, NOW_SECONDS, OWNER, OWNER_SUBJECT, Person, REVISIONS, decide_in_organization,
    expect, expect_status, fresh_request_id, saved,
};
use crate::{
    ConformanceResult,
    approval_link_scenarios::{ApprovalLinkFixture, receive_decision, registered_waiting_request},
    signin_fixtures::POLICY,
};

pub async fn check_signed_holder_delivery(
    fixture: &impl ApprovalLinkFixture,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> ConformanceResult<(u16, Vec<u8>)>,
) -> ConformanceResult<()> {
    let (target, mut socket, request) = registered_waiting_request(fixture).await?;
    let organization = target.organization_id.as_str();
    let owner = Person::owner(organization, NOW_SECONDS)?;
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

    let answer = AnswerClass::HeldStartAllow.answer();
    let signed = decide_in_organization(
        organization,
        request.request_id().as_str(),
        &fresh_request_id(NOW_SECONDS)?,
        &answer,
    );
    let headers = signed.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
    expect(
        post(signed.path, &signed.body, headers).await?,
        200,
        saved(&answer),
    )?;

    receive_decision(
        fixture,
        &mut socket,
        target,
        &request,
        Ending::UnprovenCallDecided {
            decision: SessionCallDecision::Allow,
            person_id: OWNER.parse()?,
        },
    )
    .await
}
