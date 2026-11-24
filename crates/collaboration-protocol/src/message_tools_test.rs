use std::collections::BTreeSet;

use serde_json::json;
use workspace_model::central_mail::CentralMailRefusal;

use super::*;

#[test]
fn message_contract_keeps_exact_pair_and_distinct_outcomes() {
    let parsed: MessageSendArguments = serde_json::from_value(json!({
        "callerSessionId":"fixture-caller",
        "to":{"machineId":"019a0000-0000-7000-8000-000000000001","sessionId":"fixture-target"},
        "body":"hello",
        "requestId":"019a0000-0000-7000-8000-000000000002"
    }))
    .unwrap();
    let parsed_json = serde_json::to_value(&parsed).unwrap();
    for invalid in [
        json!({"endpoint":{"serviceId":"fixture","endpointId":"fixture"},"sessionId":"fixture-target"}),
        json!({"machineId":"019a0000-0000-7000-8000-000000000001","sessionId":"fixture-target","endpointId":"extra"}),
    ] {
        let mut trial = parsed_json.clone();
        trial["to"] = invalid;
        assert!(serde_json::from_value::<MessageSendArguments>(trial).is_err());
    }
    let mut invalid = parsed_json.clone();
    invalid["sessionProof"] = json!({"kind":"notYet"});
    assert!(serde_json::from_value::<MessageSendArguments>(invalid).is_err());
    let mut invalid = parsed_json;
    invalid["requestId"] = json!("00000000-0000-4000-8000-000000000002");
    assert!(serde_json::from_value::<MessageSendArguments>(invalid).is_err());

    let front = [
        MessageFrontRefusal::AwaitingFirstRead,
        MessageFrontRefusal::NotEnrolled,
        MessageFrontRefusal::EnrollmentEnded,
        MessageFrontRefusal::MetadataUnreadable,
        MessageFrontRefusal::Starting,
        MessageFrontRefusal::Restarting,
        MessageFrontRefusal::EnrollmentMismatch,
        MessageFrontRefusal::ShuttingDown,
        MessageFrontRefusal::CouldNotStart,
    ]
    .map(MessageToolRefusal::Front);
    let custody = [
        MessageSubmitRefusal::CentralUnavailable,
        MessageSubmitRefusal::OutboxUnavailable,
        MessageSubmitRefusal::RequestIdConflict,
        MessageSubmitRefusal::UnstampedMessageRequired,
        MessageSubmitRefusal::TooLargeToSend,
        MessageSubmitRefusal::InvalidSendTime,
        MessageSubmitRefusal::UnmeasurableWrite,
        MessageSubmitRefusal::InvalidRequest,
    ]
    .map(MessageToolRefusal::Submission);
    let central = [
        CentralMailRefusal::NotSession,
        CentralMailRefusal::UnknownSession { target: parsed.to },
    ]
    .map(|reason| MessageToolRefusal::Recorded(MessageWriteRefusal::CentralMail(reason)));
    let mut text = BTreeSet::new();
    let mut wire = BTreeSet::new();
    for refusal in front.into_iter().chain(custody).chain(central) {
        assert!(
            text.insert(refusal.to_string()),
            "duplicate refusal words: {refusal:?}"
        );
        let encoded = serde_json::to_string(&refusal).unwrap();
        assert_eq!(
            serde_json::from_str::<MessageToolRefusal>(&encoded).unwrap(),
            refusal
        );
        assert!(wire.insert(encoded));
    }
    let mark = "2026-11-23T02:00:00Z".parse().unwrap();
    let status = MessageToolResult::Returned(MessageWriteStatus::InOutbox { flush_at: mark });
    assert_eq!(
        message_status_text(&status),
        "In outbox. Leaves at 2026-11-23T02:00:00+00:00."
    );
    assert_eq!(
        message_status_text(&MessageToolResult::Returned(MessageWriteStatus::InFlight)),
        "Being sent to central. No answer yet.",
    );
    assert_eq!(
        message_status_text(&MessageToolResult::Returned(
            MessageWriteStatus::StatusUnavailable
        )),
        "Status unavailable.",
    );
}

#[test]
fn accepted_write_words_report_what_central_has_confirmed() {
    let message_id: MessageId = "019a0000-0000-7000-8000-000000000003".parse().unwrap();
    let at = "2026-11-23T02:00:00Z".parse().unwrap();
    for (delivery, words) in [
        (
            MessageDeliveryObservation::Unknown,
            "Delivery not known yet.",
        ),
        (
            MessageDeliveryObservation::Checked(MessageStatus::Sent {
                deadline_passed: false,
            }),
            "Central has no delivery confirmation from the recipient's Mac.",
        ),
        (
            MessageDeliveryObservation::Checked(MessageStatus::Delivered {
                arrived_at: at,
                deadline_passed: false,
            }),
            "Delivered to the recipient's Mac.",
        ),
        (
            MessageDeliveryObservation::Checked(MessageStatus::Seen { completed_at: at }),
            "Seen: the recipient session was told.",
        ),
        (
            MessageDeliveryObservation::Checked(MessageStatus::NotFound {}),
            "Central has no record of this message.",
        ),
    ] {
        let result = MessageToolResult::Returned(MessageWriteStatus::Answered {
            message_id: message_id.clone(),
            delivery,
        });
        assert_eq!(
            message_status_text(&result),
            format!("Accepted by central. Message id {message_id}.\n{words}")
        );
    }
}
