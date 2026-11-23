use super::OutboxQueuedWrite;
use workspace_model::central_mail::SendMessageRequest;

#[test]
fn queued_direct_message_rejects_a_caller_timestamp() {
    let mut value = serde_json::json!({
        "to": {
            "machineId": "019f0000-0000-7000-8000-00000000c402",
            "sessionId": "recipient",
        },
        "body": "message text",
        "delivery": "queue",
        "replyTo": null,
    });
    let input: SendMessageRequest = serde_json::from_value(value.clone()).unwrap();
    value.as_object_mut().unwrap().insert(
        "sentAt".to_owned(),
        serde_json::json!("2030-01-01T00:00:01Z"),
    );
    assert!(serde_json::from_value::<SendMessageRequest>(value).is_err());
    let queued = OutboxQueuedWrite::SendMessage {
        request_id: "019f0000-0000-7000-8000-00000000c401".parse().unwrap(),
        message: input,
    };
    assert!(!serde_json::to_string(&queued).unwrap().contains("sentAt"));
}
