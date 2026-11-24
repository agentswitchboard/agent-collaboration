use super::*;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn the_recorded_receipt_result_leaves_before_the_first_due_pass() -> Result<()> {
    use central_link_protocol::{
        DataFrame, LinkFrame, Receipt, ReceiptBatch, ReceiptKind, ReceiptResult,
    };
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal};

    let fixture = start_fixture().await?;
    let runtime = fixture.host.central(&fixture.organization_id).await?;
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    let target = workspace_ids::SessionRef {
        machine_id: super::super::fixture::INSTALLATION.parse()?,
        session_id: "reply-due-receipt".to_owned().try_into()?,
    };
    let mail = super::super::delivery::mail_reply(
        &mut socket,
        &target,
        76,
        serde_json::json!({
            "kind":"sendMessage", "to":target, "body":"reply due receipt", "delivery":"auto",
            "replyTo":null, "sentAt":fixture.clock.utc_now(),
        }),
    )
    .await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = mail else {
        return Err("fixture mail did not queue".into());
    };
    assert!(
        matches!(receive(&mut socket).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id().as_str() == message_id.as_str())
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
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold_central(PausePoint::BeforeDeferredDueWork);
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
    send(&mut socket, &LinkFrame::Data(frame)).await?;
    assert!(
        matches!(receive(&mut socket).await?, LinkFrame::Data(DataFrame::ReceiptResults { correlation_id: answered, results })
        if answered == correlation_id && results == [ReceiptResult::Recorded])
    );
    held.until_held().await;
    let store = runtime.runtime().statement_host();
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
    held.release();
    runtime.runtime().until_reply_drains_settle().await;
    assert_eq!(effects().await?, [vec![SqlValue::Integer(1.try_into()?)]]);
    fixture.close().await
}
