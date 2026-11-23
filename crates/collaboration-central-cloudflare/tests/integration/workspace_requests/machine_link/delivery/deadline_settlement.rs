use super::*;
use central_link_protocol::{
    CorrelationId, DeliveryMessage, FrameDirection, LinkFrame, Receipt, ReceiptBatch, ReceiptKind,
    ReceiptResult,
};
use central_storage::{SqlValue, Statement};
use reqwest_websocket::WebSocket;
use workspace_ids::MessageId;
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageOutcome, MessageRefusal},
};

#[tokio::test]
async fn socket_direct_mail_retains_receipts_and_tells_the_sender_once_at_its_deadline()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let result = settlement_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn settlement_journey(fixture: &Fixture) -> Result<()> {
    let Caller::Session(session) = fixture::session_caller()? else {
        return Err("fixture must have a session".into());
    };
    let accepted_at = Timestamp::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    // This fixture starts 200 seconds before a grid mark, within one admitted link lease.
    let deadline = accepted_at + chrono::Duration::seconds(200);
    let sent_at = deadline - chrono::Duration::minutes(15);
    let mut socket = ConnectedLink::open(fixture).await?;
    let mut message_ids = Vec::new();
    for request_number in 200..203 {
        let request = serde_json::from_value(json!({
            "requestId":format!("019f0000-0000-7000-8000-{request_number:012x}"),
            "operation":{
                "kind":"sendMessage", "to":session, "body":"direct deadline witness",
                "delivery":"queue", "replyTo":null, "sentAt":sent_at,
            },
        }))?;
        let (_, replies) = socket.batch(vec![RequestPayload::Mail(request)]).await?;
        let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
            return Err("expected a direct send reply".into());
        };
        let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
            result.as_ref()
        else {
            return Err("the fixture could not queue its direct send".into());
        };
        let frame = receive(&mut socket.socket).await?;
        assert!(
            matches!(frame, LinkFrame::Data(DataFrame::Deliver { message, time_left: Some(time_left), .. })
            if message.message_id() == message_id && u32::from(time_left) == 200_000)
        );
        message_ids.push(message_id.clone());
    }
    let [seen, delivered, unreceived] = message_ids.as_slice() else {
        return Err("expected three admitted fixture messages".into());
    };
    let central = fixture.runtime();
    let store = central.statement_host();

    record_receipt(
        &mut socket.socket,
        seen,
        ReceiptKind::Delivered {
            arrived_at: accepted_at,
        },
        210,
    )
    .await?;
    assert_eq!(
        store.read(read_evidence(seen)).await?.rows,
        [[text("delivered"), SqlValue::Null]]
    );
    record_receipt(
        &mut socket.socket,
        seen,
        ReceiptKind::Seen {
            completed_at: accepted_at + chrono::Duration::seconds(1),
        },
        211,
    )
    .await?;
    record_receipt(
        &mut socket.socket,
        delivered,
        ReceiptKind::Delivered {
            arrived_at: accepted_at,
        },
        212,
    )
    .await?;
    assert_eq!(
        store.read(read_evidence(seen)).await?.rows,
        [[text("seen"), SqlValue::Null]]
    );
    drop(socket);

    set_clock(fixture, deadline - chrono::Duration::seconds(40)).await?;
    central.after_event().await?;
    assert!(store.read(read_sender_outcomes()).await?.rows.is_empty());
    set_clock(fixture, deadline).await?;
    central.after_event().await?;
    let first_outcomes = store.read(read_sender_outcomes()).await?;
    assert_eq!(first_outcomes.rows.len(), 2);
    assert_eq!(
        store.read(read_evidence(delivered)).await?.rows,
        [[
            text("delivered"),
            text(deadline.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        ]]
    );
    assert_eq!(
        store.read(read_evidence(unreceived)).await?.rows,
        [[
            text("queued"),
            text(deadline.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        ]]
    );

    let mut sender_socket = ConnectedLink::open(fixture).await?;
    assert_sender_outcomes(
        &mut sender_socket.socket,
        delivered,
        unreceived,
        accepted_at,
    )
    .await?;
    drop(sender_socket);

    // The next reconnect observes the same outcome rows with corrected clock time.
    set_clock(fixture, deadline - chrono::Duration::seconds(1)).await?;
    let mut reopened = ConnectedLink::open(fixture).await?;
    assert_sender_outcomes(&mut reopened.socket, delivered, unreceived, accepted_at).await?;

    record_receipt(
        &mut reopened.socket,
        delivered,
        ReceiptKind::Seen {
            completed_at: deadline,
        },
        213,
    )
    .await?;
    record_receipt(
        &mut reopened.socket,
        unreceived,
        ReceiptKind::Refused {
            reason: MessageRefusal::AmbiguousSession,
        },
        214,
    )
    .await?;
    send(&mut reopened.socket, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut reopened.socket).await?, LinkFrame::Pong);
    assert_eq!(
        store.read(read_evidence(delivered)).await?.rows[0][0],
        text("seen")
    );
    assert_eq!(
        store.read(read_evidence(unreceived)).await?.rows[0][0],
        text("refused")
    );
    set_clock(fixture, deadline + chrono::Duration::seconds(31)).await?;
    central.after_event().await?;
    assert_eq!(store.read(read_sender_outcomes()).await?, first_outcomes);
    Ok(())
}

async fn record_receipt(
    socket: &mut WebSocket,
    message_id: &MessageId,
    kind: ReceiptKind,
    correlation_suffix: u16,
) -> Result<()> {
    let correlation_id: CorrelationId =
        format!("019f0000-0000-7000-8000-{correlation_suffix:012x}").parse()?;
    send(
        socket,
        &LinkFrame::Data(DataFrame::Receipts {
            correlation_id: correlation_id.clone(),
            receipts: ReceiptBatch::new(vec![Receipt {
                message_id: message_id.clone(),
                kind,
            }])?,
        }),
    )
    .await?;
    assert_eq!(
        receive(socket).await?,
        LinkFrame::Data(DataFrame::ReceiptResults {
            correlation_id,
            results: vec![ReceiptResult::Recorded],
        })
    );
    Ok(())
}

async fn assert_sender_outcomes(
    socket: &mut WebSocket,
    delivered: &MessageId,
    unreceived: &MessageId,
    arrived_at: Timestamp,
) -> Result<()> {
    let mut outcomes = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let LinkFrame::Data(DataFrame::Deliver {
            message,
            time_left: None,
            ..
        }) = receive(socket).await?
        else {
            return Err("expected a sender outcome in the reconnect backlog".into());
        };
        let DeliveryMessage::System(message) = *message else {
            return Err("a settled direct message was offered again".into());
        };
        let MessageBody::DeliveryOutcome {
            message_id,
            outcome,
            ..
        } = message.body
        else {
            return Err("expected a sender outcome body".into());
        };
        assert!(outcomes.insert(message_id, outcome).is_none());
    }
    assert_eq!(
        outcomes,
        [
            (
                delivered.clone(),
                MessageOutcome::DeliveredSeenNotConfirmed { arrived_at }
            ),
            (unreceived.clone(), MessageOutcome::DeliveryNotConfirmed),
        ]
        .into_iter()
        .collect()
    );
    send(socket, &LinkFrame::Ping).await?;
    assert_eq!(receive(socket).await?, LinkFrame::Pong);
    Ok(())
}

fn read_evidence(message_id: &MessageId) -> Statement {
    Statement {
        sql: "SELECT state,deadline_settled_at FROM mail_messages WHERE message_id=?",
        args: vec![text(message_id)],
    }
}

fn read_sender_outcomes() -> Statement {
    Statement {
        sql: "SELECT message_id,body FROM mail_messages WHERE message_kind='deliveryOutcome' ORDER BY message_id",
        args: vec![],
    }
}

fn text(value: impl ToString) -> SqlValue {
    SqlValue::Text(value.to_string())
}

async fn set_clock(fixture: &Fixture, now: Timestamp) -> Result<()> {
    fixture.clock.set_utc(now);
    fixture
        .host
        .set_external_time_ms(now.timestamp_millis())
        .await?;
    assert!(
        fixture
            .runtime()
            .control::<bool>("clock", &now.timestamp_millis())
            .await?
    );
    Ok(())
}

async fn send(socket: &mut WebSocket, frame: &LinkFrame) -> Result<()> {
    let encoded = central_link_protocol::encode_text(frame, FrameDirection::ToCentral)?;
    socket.send(Message::Text(encoded)).await?;
    Ok(())
}

async fn receive(socket: &mut WebSocket) -> Result<LinkFrame> {
    let Some(Message::Text(encoded)) = socket.next().await.transpose()? else {
        return Err("expected a Central text frame".into());
    };
    Ok(central_link_protocol::decode_text(
        &encoded,
        FrameDirection::ToRouter,
    )?)
}
