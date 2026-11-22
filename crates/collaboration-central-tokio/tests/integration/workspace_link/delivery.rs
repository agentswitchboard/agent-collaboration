use super::*;
use central_link_protocol::{CorrelationId, LinkRequest};
use reqwest_websocket::WebSocket;
use workspace_model::{ResolvedSender, central_mail::CentralMailAnswer};

#[tokio::test]
async fn live_direct_mail_pushes_then_reconnect_owns_the_backlog_and_receipt() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = delivery_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn delivery_journey(fixture: &Fixture) -> Result<()> {
    let session = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "workspace-link-delivery".to_owned().try_into()?,
    };
    let mut original = fixture.connect().await?;
    receive(&mut original).await?;
    let admitted = mail_reply(
        &mut original,
        &session,
        1,
        json!({"kind":"setNoticeCadence","cadence":"m15"}),
    )
    .await?;
    if !matches!(
        admitted,
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })
    ) {
        return Err("fixture session did not register with central".into());
    }
    let queued = mail_reply(
        &mut original,
        &session,
        2,
        json!({
            "kind":"sendMessage","to":session,"body":"deliver on the link",
            "delivery":"steer","replyTo":null,
            "sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?
        }),
    )
    .await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = queued else {
        return Err("direct message was not accepted".into());
    };
    let first_offer = receive(&mut original).await?;
    if !matches!(&first_offer, LinkFrame::Data(DataFrame::Deliver {
        message, sender: ResolvedSender::Session { session: sender, display_name: None, machine_label: Some(label) }, time_left: Some(time_left),
    }) if message.message_id() == &message_id && message.target() == &session && sender == &session
        && u32::from(*time_left) == 900_000 && label.as_str() == "Link test machine")
    {
        return Err("live delivery did not carry the accepted id and resolved sender".into());
    }
    let private = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(fixture.signin_socket.clone())
        .build()?;
    let other_token = signed::enroll_another_machine(
        &private,
        "http://localhost",
        signed::ORGANIZATION,
        OTHER_MACHINE,
        "019f0000-0000-7000-8000-00000000e401",
        signed::NOW_SECONDS,
    )
    .await?;
    let mut wrong_machine = fixture
        .client()?
        .get(fixture.url(signed::ORGANIZATION))
        .bearer_auth(other_token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    if !matches!(receive(&mut wrong_machine).await?, LinkFrame::Data(DataFrame::Welcome { machine_id, .. }) if machine_id.as_str()==OTHER_MACHINE)
    {
        return Err("the wrong-recipient probe needs an independently admitted machine".into());
    }
    receipt_result(
        &mut wrong_machine,
        message_id.clone(),
        central_link_protocol::ReceiptResult::NoSuchMessage,
    )
    .await?;
    send(&mut wrong_machine, &LinkFrame::Ping).await?;
    if receive(&mut wrong_machine).await? != LinkFrame::Pong {
        return Err("a wrong-machine receipt must keep the socket open".into());
    }
    wrong_machine
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;

    let mut replacement = fixture.connect().await?;
    if !matches!(
        receive(&mut replacement).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("replacement did not receive Welcome first".into());
    }
    if receive(&mut replacement).await? != first_offer {
        return Err("replacement did not receive the same unacknowledged message".into());
    }
    drop(original);
    receipt_result(
        &mut replacement,
        message_id.clone(),
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    send(&mut replacement, &LinkFrame::Ping).await?;
    if receive(&mut replacement).await? != LinkFrame::Pong {
        return Err("replacement link did not handle the receipt and remain usable".into());
    }
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let rows = runtime
        .runtime()
        .statement_host()
        .read(central_storage::Statement {
            sql: "SELECT state FROM mail_messages WHERE message_id=?",
            args: vec![central_storage::SqlValue::Text(message_id.to_string())],
        })
        .await?;
    if rows.rows != vec![vec![central_storage::SqlValue::Text("delivered".into())]] {
        return Err("the receipt did not settle the owning message row".into());
    }
    let next = mail_reply(
        &mut replacement,
        &session,
        3,
        json!({
            "kind":"sendMessage","to":session,"body":"deliver after the old socket closes",
            "delivery":"queue","replyTo":null,
            "sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?
        }),
    )
    .await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued {
        message_id: next_message,
    }) = next
    else {
        return Err("the replacement did not accept its next message".into());
    };
    if !matches!(receive(&mut replacement).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == &next_message)
    {
        return Err("closing the previous socket lost the registered replacement".into());
    }
    receipt_result(
        &mut replacement,
        next_message,
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    send(&mut replacement, &LinkFrame::Ping).await?;
    if receive(&mut replacement).await? != LinkFrame::Pong {
        return Err("second receipt did not leave the machine socket usable".into());
    }
    let settled_rows = || central_storage::Statement {
        sql: "SELECT message_id,message_kind,state,updated_at FROM mail_messages ORDER BY message_id",
        args: vec![],
    };
    let before = runtime
        .runtime()
        .statement_host()
        .read(settled_rows())
        .await?;
    fixture.clock.advance(Duration::from_secs(1))?;
    let mut reconnected = fixture.connect().await?;
    if !matches!(
        receive(&mut reconnected).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("settled delivery reconnect must start with Welcome".into());
    }
    receipt_result(
        &mut reconnected,
        message_id,
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    send(&mut reconnected, &LinkFrame::Ping).await?;
    if receive(&mut reconnected).await? != LinkFrame::Pong {
        return Err("the replayed receipt must remain silent and keep the socket open".into());
    }
    if runtime
        .runtime()
        .statement_host()
        .read(settled_rows())
        .await?
        != before
    {
        return Err("the replayed receipt rewrote a settled delivery or added an outcome".into());
    }
    Ok(())
}

pub(super) async fn mail_reply(
    socket: &mut WebSocket,
    session: &workspace_ids::SessionRef,
    index: u32,
    operation: serde_json::Value,
) -> Result<CentralMailResult> {
    let correlation: CorrelationId = format!("019f0000-0000-7000-8000-{index:012x}").parse()?;
    let request = LinkRequest::new(
        correlation.clone(),
        Caller::Session(session.clone()),
        RequestPayload::Mail(serde_json::from_value(json!({
            "requestId":format!("019f0000-0000-7000-8000-{:012x}",index+20),"operation":operation,
        }))?),
    )?;
    send(
        socket,
        &LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::Reply {
        correlation_id,
        result,
    }) = receive(socket).await?
    else {
        return Err("expected central mail reply".into());
    };
    match *result {
        ReplyPayload::Mail(result) if correlation_id == correlation => Ok(result),
        _ => Err("expected matching mail reply".into()),
    }
}

#[tokio::test]
async fn zero_time_direct_mail_is_never_offered_after_clock_rollback() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = zero_time_offer_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn zero_time_offer_journey(fixture: &Fixture) -> Result<()> {
    let session = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "workspace-link-zero-time".to_owned().try_into()?,
    };
    let mut live = fixture.connect().await?;
    assert!(matches!(
        receive(&mut live).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    fixture.clock.advance(Duration::from_secs(2))?;
    let accepted =
        chrono::DateTime::from_timestamp(signed::NOW_SECONDS + 2, 0).ok_or("fixture UTC")?;
    let send_at = |sent_at| {
        json!({
            "kind":"sendMessage", "to":session, "body":"zero time offer probe",
            "delivery":"queue", "replyTo":null, "sentAt":sent_at
        })
    };
    let queued = mail_reply(
        &mut live,
        &session,
        70,
        send_at(accepted - chrono::Duration::minutes(16)),
    )
    .await?;
    assert!(matches!(
        queued,
        CentralMailResult::Done(CentralMailAnswer::Queued { .. })
    ));
    let corrected = accepted - chrono::Duration::seconds(1);
    fixture.clock.set_utc(corrected);
    let positive = mail_reply(&mut live, &session, 71, send_at(corrected)).await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = positive else {
        return Err("the positive control was not accepted".into());
    };
    assert!(
        matches!(receive(&mut live).await?, LinkFrame::Data(DataFrame::Deliver { message, time_left: Some(time_left), .. }) if message.message_id() == &message_id && u32::from(time_left) == 900_000)
    );
    send(&mut live, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut live).await?, LinkFrame::Pong);

    let mut reopened = fixture.connect().await?;
    assert!(matches!(
        receive(&mut reopened).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ));
    assert!(
        matches!(receive(&mut reopened).await?, LinkFrame::Data(DataFrame::Deliver { message, time_left: Some(time_left), .. }) if message.message_id() == &message_id && u32::from(time_left) == 900_000)
    );
    send(&mut reopened, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut reopened).await?, LinkFrame::Pong);
    Ok(())
}

#[tokio::test]
async fn reconnect_samples_time_left_across_the_backlog_page_boundary() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = backlog_page_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn backlog_page_journey(fixture: &Fixture) -> Result<()> {
    use central_storage::{
        AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, StatementBatch,
    };

    let sent_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    let destination = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "backlog-page-boundary".to_owned().try_into()?,
    };
    let answer = mail_reply(
        &mut socket,
        &destination,
        72,
        json!({
            "kind":"sendMessage","to":destination,"body":"backlog template","delivery":"queue",
            "replyTo":null,"sentAt":sent_at,
        }),
    )
    .await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = answer else {
        return Err("backlog template was not queued".into());
    };
    if !matches!(receive(&mut socket).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == &message_id)
    {
        return Err("backlog template did not have its live offer".into());
    }
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let runtime = fixture.host.central(&organization).await?;
    let store = runtime.runtime().statement_host();
    let page_size = fleet_contracts::mail_link::WELCOME_PAGE_SIZE;
    let seed = store.run_batch(StatementBatch {
        statements: vec![GuardedStatement {
            statement: central_storage::Statement {
                sql: "WITH RECURSIVE seed(idx) AS (SELECT 1 UNION ALL SELECT idx+1 FROM seed WHERE idx<?) INSERT INTO mail_messages (message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at,sender_request_id,delivery_window_ends_at) SELECT printf('019f0000-0000-7000-8000-%012x',idx),sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at,sender_request_id,delivery_window_ends_at FROM mail_messages JOIN seed WHERE message_id=?",
                args: vec![SqlValue::Integer(i64::try_from(page_size)?.try_into()?), SqlValue::Text(message_id.to_string())],
            },
            expect: RowExpectation::Any,
        }],
        alarm: AlarmUpdate::Unchanged,
    }).await?;
    if !matches!(seed, BatchOutcome::Applied { .. }) {
        return Err("the test could not seed a backlog larger than one page".into());
    }
    fixture.clock.advance(Duration::from_secs(62))?;
    let mut reopened = fixture.connect().await?;
    if !matches!(
        receive(&mut reopened).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("expected Welcome before the backlog".into());
    }
    let mut offered = std::collections::BTreeSet::new();
    for _ in 0..=page_size {
        let Some(Ok(Message::Text(offer_text))) = reopened.next().await else {
            return Err("expected a direct offer as raw socket text".into());
        };
        let offer_json: serde_json::Value = serde_json::from_str(&offer_text)?;
        let offer_keys = offer_json
            .get("message")
            .and_then(serde_json::Value::as_object)
            .ok_or("missing direct offer payload")?
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        if offer_keys
            != std::collections::BTreeSet::from([
                "body",
                "messageId",
                "sender",
                "source",
                "state",
                "target",
            ])
            || offer_json
                .get("timeLeft")
                .and_then(serde_json::Value::as_u64)
                != Some(838_000)
        {
            return Err(
                format!("unexpected direct offer fields or time left: {offer_text}").into(),
            );
        }
        let central_link_protocol::LinkFrame::Data(DataFrame::Deliver {
            message,
            time_left: Some(time_left),
            ..
        }) = central_link_protocol::decode_text(
            &offer_text,
            central_link_protocol::FrameDirection::ToRouter,
        )?
        else {
            return Err("expected another direct offer across the backlog page boundary".into());
        };
        if u32::from(time_left) != 838_000 || !offered.insert(message.message_id().clone()) {
            return Err("a backlog offer had stale time left or repeated a message".into());
        }
    }
    if !offered.contains(&message_id) {
        return Err("backlog omitted its original queued message".into());
    }
    send(&mut reopened, &LinkFrame::Ping).await?;
    if receive(&mut reopened).await? != LinkFrame::Pong {
        return Err("a second backlog page repeated an offer".into());
    }
    Ok(())
}
