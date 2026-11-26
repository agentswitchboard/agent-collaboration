use std::collections::VecDeque;

use approval_request_protocol::SessionCallDecision;
use central_link_protocol::{
    CentralCall, CentralLinkRpc, DataFrame, FrameDirection, LinkFrame, TypedCallResult,
    decode_text, encode_text, person_link_rpc::DecidePersonApprovalRpc,
};
use router_clock::ManualClock;
use serde_json::json;

use crate::test_support::fixture_authority::{
    ADMIN_AUDIENCE, CLIENT_ID, FixtureCredential, INITIAL_SECONDS, PUBLIC_ORIGIN,
};

use super::*;

enum ReadyReplies {
    Matching,
    StaleThenMatching,
    StaleThenClosed,
}

fn peer_replies(mode: &ReadyReplies, text: &str) -> VecDeque<String> {
    let LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    }) = decode_text(text, FrameDirection::ToCentral).unwrap()
    else {
        return VecDeque::new();
    };
    let CentralCall::DecidePersonApproval(request) = *request else {
        return VecDeque::new();
    };
    let reply = |correlation, body| {
        let reply = DecidePersonApprovalRpc::reply(Envelope {
            target: request.target.clone(),
            body,
        });
        encode_text(
            &LinkFrame::Data(DataFrame::CentralCallReply {
                correlation_id: correlation,
                result: Box::new(TypedCallResult::Answered { reply }),
            }),
            FrameDirection::ToRouter,
        )
        .unwrap()
    };
    let mut replies = VecDeque::new();
    if !matches!(mode, ReadyReplies::Matching) {
        let stale = "019f0000-0000-7000-8000-00000000c815".parse().unwrap();
        assert_ne!(stale, correlation_id);
        replies.push_back(reply(stale, ApprovalDecideReply::NotVisible {}));
    }
    if !matches!(mode, ReadyReplies::StaleThenClosed) {
        replies.push_back(reply(correlation_id, ApprovalDecideReply::TooLate {}));
    }
    replies
}

async fn decide_with_failed_write(mode: ReadyReplies) -> (ApprovalDecisionOutcome, RequestId) {
    let target = FleetTarget::from_json(
        &serde_json::to_vec(&json!({
            "issuer":"https://issuer.example.invalid",
            "organizationId":"019f0000-0000-7000-8000-00000000c801",
            "centralAdminAudience":ADMIN_AUDIENCE,
            "ownerFleetCliClientId":CLIENT_ID,
            "centralPublicOrigin":PUBLIC_ORIGIN,
        }))
        .unwrap(),
    )
    .unwrap();
    let clock = Arc::new(ManualClock::new(
        chrono::DateTime::from_timestamp(INITIAL_SECONDS, 0).unwrap(),
    ));
    let mut client = PersonLinkClient::new(target, FixtureCredential::new(), clock).unwrap();
    client.state = Some(ConnectionState::Connected(None));
    client.socket = Some(PersonSocket::FailedWrite {
        on_write: Box::new(move |text| peer_replies(&mode, text)),
        replies: VecDeque::new(),
    });
    let request = "019f0000-0000-7000-8000-00000000c813".parse().unwrap();
    let operation_id: RequestId = "019f0000-0000-7000-8000-00000000c814".parse().unwrap();
    let answer = Answer::UnprovenCall {
        decision: SessionCallDecision::Allow,
    };
    let outcome = client
        .decide_approval(request, operation_id.clone(), answer)
        .await
        .unwrap();
    (outcome, operation_id)
}

#[tokio::test]
async fn failed_write_recovers_a_readable_matching_decide_reply() {
    let (outcome, _) = decide_with_failed_write(ReadyReplies::Matching).await;

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::TooLate {})
    );
}

#[tokio::test]
async fn failed_write_ignores_a_stale_reply_before_the_matching_answer() {
    let (outcome, _) = decide_with_failed_write(ReadyReplies::StaleThenMatching).await;

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::TooLate {})
    );
}

#[tokio::test]
async fn failed_write_preserves_its_cause_when_reading_fails_after_a_stale_reply() {
    let (outcome, operation_id) = decide_with_failed_write(ReadyReplies::StaleThenClosed).await;

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Unknown {
            operation_id,
            cause: PersonLinkError::Socket
        }
    );
}
