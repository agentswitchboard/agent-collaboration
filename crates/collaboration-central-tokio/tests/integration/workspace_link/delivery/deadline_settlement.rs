use super::*;
use central_link_protocol::{DeliveryMessage, Receipt, ReceiptBatch, ReceiptKind, ReceiptResult};
use central_storage::{SqlValue, Statement};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageOutcome, MessageRefusal},
};

#[tokio::test]
async fn socket_direct_mail_retains_receipts_and_tells_the_sender_once_at_its_deadline()
-> Result<()> {
    let fixture = start_fixture().await?;
    let result = settlement_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn settlement_journey(fixture: &Fixture) -> Result<()> {
    let session = SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "direct-deadline-witness".to_owned().try_into()?,
    };
    let accepted_at = Timestamp::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    // This fixture starts 200 seconds before a grid mark, within one admitted link lease.
    let deadline = accepted_at + chrono::Duration::seconds(200);
    let cutoff = deadline - chrono::Duration::seconds(45);
    let sent_at = cutoff - chrono::Duration::minutes(15);
    let mut socket = fixture.connect().await?;
    assert!(matches!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    let mut message_ids = Vec::new();
    for request_number in 200..203 {
        let reply = mail_reply(
            &mut socket,
            &session,
            request_number,
            json!({
                "kind":"sendMessage", "to":session, "body":"direct deadline witness",
                "delivery":"queue", "replyTo":null, "sentAt":sent_at
            }),
        )
        .await?;
        let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = reply else {
            return Err("the fixture could not queue its direct send".into());
        };
        let frame = receive(&mut socket).await?;
        assert!(
            matches!(frame, LinkFrame::Data(DataFrame::Deliver { message, time_left: Some(time_left), .. })
            if message.message_id() == &message_id && u32::from(time_left) == 155_000)
        );
        message_ids.push(message_id);
    }
    let [seen, delivered, unreceived] = message_ids.as_slice() else {
        return Err("expected three admitted fixture messages".into());
    };
    let central = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let store = central.runtime().statement_host();

    record_receipt(
        &mut socket,
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
        &mut socket,
        seen,
        ReceiptKind::Seen {
            completed_at: accepted_at + chrono::Duration::seconds(1),
        },
        211,
    )
    .await?;
    assert_eq!(
        store.read(read_evidence(seen)).await?.rows,
        [[text("seen"), SqlValue::Null]]
    );

    fixture
        .clock
        .set_utc(deadline - chrono::Duration::seconds(40));
    let reply = mail_reply(
        &mut socket,
        &session,
        215,
        json!({
            "kind":"setNoticeCadence","cadence":"m15",
        }),
    )
    .await?;
    assert!(matches!(
        reply,
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })
    ));
    assert!(store.read(read_sender_outcomes()).await?.rows.is_empty());
    record_receipt(
        &mut socket,
        delivered,
        ReceiptKind::Delivered {
            arrived_at: accepted_at,
        },
        212,
    )
    .await?;
    assert!(store.read(read_sender_outcomes()).await?.rows.is_empty());
    drop(socket);
    fixture.clock.set_utc(deadline);
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

    let mut sender_socket = fixture.connect().await?;
    assert!(matches!(
        receive(&mut sender_socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    assert_sender_outcomes(&mut sender_socket, delivered, unreceived, accepted_at).await?;
    drop(sender_socket);

    // The next reconnect observes the same outcome rows with corrected clock time.
    fixture.clock.set_utc(cutoff - chrono::Duration::seconds(1));
    let mut reopened = fixture.connect().await?;
    assert!(matches!(
        receive(&mut reopened).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    assert_sender_outcomes(&mut reopened, delivered, unreceived, accepted_at).await?;

    record_receipt(
        &mut reopened,
        delivered,
        ReceiptKind::Seen {
            completed_at: deadline,
        },
        213,
    )
    .await?;
    record_receipt(
        &mut reopened,
        unreceived,
        ReceiptKind::Refused {
            reason: MessageRefusal::AmbiguousSession,
        },
        214,
    )
    .await?;
    send(&mut reopened, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut reopened).await?, LinkFrame::Pong);
    let settled_at = text(deadline.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    assert_eq!(
        store.read(read_evidence(delivered)).await?.rows,
        [[text("seen"), settled_at.clone()]]
    );
    assert_eq!(
        store.read(read_evidence(unreceived)).await?.rows,
        [[text("refused"), settled_at]]
    );
    fixture
        .clock
        .set_utc(deadline + chrono::Duration::seconds(31));
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

#[tokio::test]
async fn fractional_wire_cutoff_just_after_the_grid_waits_for_the_next_mark() -> Result<()> {
    let fixture = start_fixture().await?;
    let result = fractional_cutoff_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn fractional_cutoff_journey(fixture: &Fixture) -> Result<()> {
    let accepted = Timestamp::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    let grid = accepted + chrono::Duration::seconds(200);
    let sent_at = grid - chrono::Duration::minutes(15) + chrono::Duration::microseconds(500);
    let session = SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "fractional-deadline".to_owned().try_into()?,
    };
    let mut socket = fixture.connect().await?;
    assert!(matches!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    let operation = json!({
        "kind":"sendMessage", "to":session, "body":"fractional timestamp witness",
        "delivery":"queue", "replyTo":null, "sentAt":sent_at,
    });
    let reply = mail_reply(&mut socket, &session, 219, operation).await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = reply else {
        return Err("send refused".into());
    };
    let central = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let store = central.runtime().statement_host();
    let stored = store.read(Statement {
        sql: "SELECT delivery_window_ends_at,cancel_due_at FROM mail_messages WHERE message_id=?",
        args: vec![text(message_id)],
    }).await?;
    let stored_time =
        |time: Timestamp| text(time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    assert_eq!(
        stored.rows,
        [[
            stored_time(grid + chrono::Duration::milliseconds(1)),
            stored_time(grid + chrono::Duration::minutes(5)),
        ]]
    );
    Ok(())
}
