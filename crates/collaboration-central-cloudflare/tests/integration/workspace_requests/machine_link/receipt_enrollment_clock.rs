//! Workerd's receipt authority samples the owning clock after the enrollment read.
use super::fixture::{self, ConnectedLink, Fixture};
use central_link_protocol::{
    CorrelationId, DataFrame, MachineLinkCloseCode, Receipt, ReceiptBatch, ReceiptKind,
    ReceiptResult, ReplyPayload, RequestPayload,
};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use futures_util::StreamExt;
use reqwest_websocket::Message;
use workspace_model::{
    Caller,
    central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal},
};

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
    let fixture = Fixture::new().await?;
    let result = clock_move(&fixture, boundary).await;
    fixture.close().await?;
    result
}

async fn clock_move(fixture: &Fixture, boundary: AfterEnrollmentRead) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    let DataFrame::Welcome {
        effective_expires_at,
        ..
    } = link.welcome
    else {
        return Err("expected granted socket expiry in Welcome".into());
    };
    let sent_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    let Caller::Session(recipient) = fixture::session_caller()? else {
        return Err("receipt fixture needs a session".into());
    };
    let request = serde_json::from_value(serde_json::json!({
        "requestId":"019f0000-0000-7000-8000-00000000ce84",
        "operation":{"kind":"sendMessage", "to":recipient, "body":"receipt with a later authority sample",
            "delivery":"auto", "replyTo":null,"sentAt":sent_at}
    }))?;
    let (_, replies) = link.batch(vec![RequestPayload::Mail(request)]).await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("sending the receipt fixture message did not answer".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        result.as_ref()
    else {
        return Err("sending the receipt fixture message was refused".into());
    };
    if !matches!(
        fixture::receive(&mut link.socket).await?,
        DataFrame::Deliver { .. }
    ) {
        return Err("receipt fixture message was not offered".into());
    }
    let store = fixture.runtime();
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
    if !store
        .control::<bool>(
            "receipt-clock-after-enrollment-read",
            &later.timestamp_millis(),
        )
        .await?
    {
        return Err("could not arm Workerd's receipt enrollment-read clock move".into());
    }
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ce98".parse()?;
    fixture::send(
        &mut link.socket,
        &DataFrame::Receipts {
            correlation_id: correlation.clone(),
            receipts: ReceiptBatch::new(vec![Receipt {
                message_id: message_id.clone(),
                kind: ReceiptKind::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
            }])?,
        },
    )
    .await?;

    let expected_rows = match boundary {
        AfterEnrollmentRead::BeforeExpiry => {
            loop {
                match fixture::receive(&mut link.socket).await? {
                    DataFrame::Deliver { .. } => {}
                    DataFrame::ReceiptResults {
                        correlation_id,
                        results,
                    } if correlation_id == correlation && results == [ReceiptResult::Recorded] => {
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
            let reply = link.socket.next().await;
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
