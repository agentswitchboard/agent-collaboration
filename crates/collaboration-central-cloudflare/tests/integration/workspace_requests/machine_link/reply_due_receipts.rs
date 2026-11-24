use super::*;

#[tokio::test]
async fn the_recorded_receipt_result_leaves_before_the_first_due_pass() -> Result<()> {
    use central_link_protocol::{Receipt, ReceiptBatch, ReceiptKind, ReceiptResult};
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal};

    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime();
    let mut link = ConnectedLink::open(&fixture).await?;
    let workspace_model::Caller::Session(target) = super::super::fixture::session_caller()? else {
        return Err("no fixture session".into());
    };
    let mail: CentralMailRequest = serde_json::from_value(serde_json::json!({
        "requestId": "019f0000-0000-7000-8000-00000000db36",
        "operation": {"kind":"sendMessage", "to":target, "body":"reply due receipt", "delivery":"auto",
            "replyTo": null, "sentAt":fixture.clock.utc_now()},
    }))?;
    let (_, replies) = link.batch(vec![RequestPayload::Mail(mail)]).await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("expected a mail reply".into());
    };
    let ReplyPayload::Mail(mail) = result.as_ref() else {
        return Err("expected a mail result".into());
    };
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = mail else {
        return Err("fixture mail did not queue".into());
    };
    assert!(
        matches!(super::super::fixture::receive(&mut link.socket).await?, DataFrame::Deliver { message, .. } if message.message_id().as_str() == message_id.as_str())
    );
    let key: uuid::Uuid = "019f0000-0000-7000-8000-00000000db37".parse()?;
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    runtime
        .seed_probe(SeedProbe {
            key,
            group: key,
            due: Some(due),
            claim: "receipt writer".into(),
        })
        .await?;
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let _: bool = runtime.control("hold-central", &BEFORE_DEFERRED).await?;
    let correlation_id: central_link_protocol::CorrelationId =
        "019f0000-0000-7000-8000-00000000db38".parse()?;
    let frame = DataFrame::Receipts {
        correlation_id: correlation_id.clone(),
        receipts: ReceiptBatch::new(vec![Receipt {
            message_id: message_id.clone(),
            kind: ReceiptKind::Refused {
                reason: MessageRefusal::UnknownSession,
            },
        }])?,
    };
    super::super::fixture::send(&mut link.socket, &frame).await?;
    assert!(
        matches!(super::super::fixture::receive(&mut link.socket).await?, DataFrame::ReceiptResults { correlation_id: answered, results }
        if answered == correlation_id && results == [ReceiptResult::Recorded])
    );
    let _: bool = runtime
        .control("until-held-central", &BEFORE_DEFERRED)
        .await?;
    let store = &*runtime;
    let effects = || async {
        store
            .read(Statement {
                sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                args: vec![SqlValue::Text(key.to_string())],
            })
            .await
            .map(|answer| answer.rows)
    };
    assert_eq!(effects().await?, [vec![SqlValue::Integer(0.try_into()?)]]);
    let _: bool = runtime.control("release-central", &BEFORE_DEFERRED).await?;
    assert!(
        observe_event(|| async {
            Ok(runtime
                .control::<(u64, u64)>("raw-reply-due-activity", &())
                .await?
                == (activity.0 + 1, 0))
        })
        .await?
    );
    assert_eq!(effects().await?, [vec![SqlValue::Integer(1.try_into()?)]]);
    fixture.close().await
}
