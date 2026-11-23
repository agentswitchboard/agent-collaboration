//! Applying receipt authority samples the clock after the enrollment read.
use super::fixture::{Fixture, INSTALLATION, receive, send, start_fixture};
use central_link_protocol::{
    CorrelationId, DataFrame, LinkFrame, MachineLinkCloseCode, Receipt, ReceiptBatch, ReceiptKind,
    ReceiptResult,
};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use futures_util::StreamExt;
use reqwest_websocket::Message;
use workspace_ids::SessionRef;
use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal};

#[tokio::test]
async fn receipt_enrollment_read_records_the_later_clock_sample() -> Result<()> {
    check_clock_move(AfterEnrollmentRead::BeforeExpiry).await
}

#[tokio::test]
async fn receipt_enrollment_read_crosses_expiry_without_a_write() -> Result<()> {
    check_clock_move(AfterEnrollmentRead::AtExpiry).await
}

#[derive(Clone, Copy)]
enum AfterEnrollmentRead {
    BeforeExpiry,
    AtExpiry,
}

async fn check_clock_move(boundary: AfterEnrollmentRead) -> Result<()> {
    let fixture = start_fixture().await?;
    let result = clock_move(&fixture, boundary).await;
    fixture.close().await?;
    result
}

async fn clock_move(fixture: &Fixture, boundary: AfterEnrollmentRead) -> Result<()> {
    let mut socket = fixture.connect().await?;
    let LinkFrame::Data(DataFrame::Welcome {
        effective_expires_at,
        ..
    }) = receive(&mut socket).await?
    else {
        return Err("expected granted socket expiry in Welcome".into());
    };
    let sent_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    let recipient = SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "receipt-enrollment-clock".to_owned().try_into()?,
    };
    let result = super::delivery::mail_reply(
        &mut socket,
        &recipient,
        84,
        serde_json::json!({
            "kind":"sendMessage", "to":recipient, "body":"receipt with a later authority sample",
            "delivery":"auto", "replyTo":null, "sentAt":sent_at
        }),
    )
    .await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = result else {
        return Err("sending the receipt fixture message was refused".into());
    };
    if !matches!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::Deliver { .. })
    ) {
        return Err("receipt fixture message was not offered".into());
    }
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let central = fixture.host.central(&organization).await?;
    let store = central.runtime().statement_host();
    let read_receipt = || {
        Statement {
        sql: "SELECT state,failure_reason,updated_at,
            (SELECT min(outcome.updated_at) FROM mail_messages AS outcome
             WHERE outcome.message_kind='deliveryOutcome' AND json_extract(outcome.body,'$.messageId')=message.message_id)
            FROM mail_messages AS message WHERE message_id=?",
        args: vec![SqlValue::Text(message_id.to_string())],
    }
    };
    let before = store.read(read_receipt()).await?.rows;
    let prior = vec![vec![
        SqlValue::Text("queued".into()),
        SqlValue::Null,
        SqlValue::Text(sent_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        SqlValue::Null,
    ]];
    if before != prior {
        return Err(format!("expected a queued message before its receipt, got {before:?}").into());
    }
    let later = match boundary {
        AfterEnrollmentRead::BeforeExpiry => sent_at + chrono::Duration::milliseconds(456),
        AfterEnrollmentRead::AtExpiry => effective_expires_at,
    };
    let clock = fixture.clock.clone();
    store.move_clock_after_receipt_enrollment_read(move || clock.set_utc(later));
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ce98".parse()?;
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::Receipts {
            correlation_id: correlation.clone(),
            receipts: ReceiptBatch::new(vec![Receipt {
                message_id: message_id.clone(),
                kind: ReceiptKind::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
            }])?,
        }),
    )
    .await?;

    let expected_rows = match boundary {
        AfterEnrollmentRead::BeforeExpiry => {
            loop {
                match receive(&mut socket).await? {
                    LinkFrame::Data(DataFrame::Deliver { .. }) => {}
                    LinkFrame::Data(DataFrame::ReceiptResults {
                        correlation_id,
                        results,
                    }) if correlation_id == correlation && results == [ReceiptResult::Recorded] => {
                        break;
                    }
                    unexpected => {
                        return Err(format!("unexpected receipt frame: {unexpected:?}").into());
                    }
                }
            }
            let time = later.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            vec![vec![
                SqlValue::Text("refused".into()),
                SqlValue::Text("unknownSession".into()),
                SqlValue::Text(time.clone()),
                SqlValue::Text(time),
            ]]
        }
        AfterEnrollmentRead::AtExpiry => {
            let reply = socket.next().await;
            if !matches!(
                reply,
                Some(Ok(Message::Close { code, .. }))
                    if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Expired)
            ) {
                return Err(format!(
                    "receipt must close with Expired at the read boundary: {reply:?}"
                )
                .into());
            }
            before
        }
    };
    let after = store.read(read_receipt()).await?.rows;
    if after != expected_rows {
        return Err(format!("expected receipt rows {expected_rows:?}, got {after:?}").into());
    }
    Ok(())
}
