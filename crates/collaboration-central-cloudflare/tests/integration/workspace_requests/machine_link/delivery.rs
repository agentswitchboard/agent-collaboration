use super::*;
use central_link_protocol::RequestPayload;
use serde_json::json;
use workspace_model::{
    ResolvedSender, central_mail::CentralMailAnswer, central_mail::CentralMailResult,
};

#[tokio::test]
async fn worker_pushes_direct_mail_and_replacement_delivers_the_same_backlog_row() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = delivery_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn delivery_journey(fixture: &Fixture) -> Result<()> {
    let mut original = ConnectedLink::open(fixture).await?;
    let Caller::Session(session) = fixture::session_caller()? else {
        return Err("fixture must be a session".into());
    };
    let operations = [
        json!({
            "requestId":"019f0000-0000-7000-8000-00000000cf31",
            "operation":{"kind":"setNoticeCadence","cadence":"m15"},
        }),
        json!({
            "requestId":"019f0000-0000-7000-8000-00000000cf32",
            "operation":{
                "kind":"sendMessage","to":session,"body":"worker direct delivery","delivery":"queue",
                "replyTo":null,"sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?
            },
        }),
    ];
    let requests = operations
        .into_iter()
        .map(|value| serde_json::from_value(value).map(RequestPayload::Mail))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let (_, replies) = original.batch(requests).await?;
    let [_, DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("expected both mail replies before delivery".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        result.as_ref()
    else {
        return Err("the direct message was not accepted".into());
    };
    let offered = fixture::receive(&mut original.socket).await?;
    if !matches!(&offered, DataFrame::Deliver {
        message, sender: ResolvedSender::Session { session: sender, display_name: None, machine_label: Some(label) }, time_left: Some(time_left),
    } if message.message_id() == message_id && message.target() == &session && sender == &session && u32::from(*time_left) == 900_000 && !label.as_str().is_empty())
    {
        return Err("the Worker did not push the accepted record with the resolved sender".into());
    }
    let wrong_recipient = "019f0000-0000-7000-8000-00000000cf0e";
    let other_token = signed::enroll_another_machine(
        &fixture.client,
        &fixture.host.signin_origin(),
        ORGANIZATION,
        wrong_recipient,
        "019f0000-0000-7000-8000-00000000e402",
        signed::NOW_SECONDS,
    )
    .await?;
    let mut wrong_machine = fixture
        .client
        .get(fixture.link_url(ORGANIZATION))
        .bearer_auth(other_token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    if !matches!(fixture::receive(&mut wrong_machine).await?, DataFrame::Welcome { machine_id, .. } if machine_id.as_str()==wrong_recipient)
    {
        return Err("the wrong-recipient probe needs an independently admitted machine".into());
    }
    receipt_result(
        &mut wrong_machine,
        message_id.clone(),
        central_link_protocol::ReceiptResult::NoSuchMessage,
    )
    .await?;
    wrong_machine.send(Message::Text("ping".to_owned())).await?;
    if !matches!(wrong_machine.next().await, Some(Ok(Message::Text(pong))) if pong=="pong") {
        return Err("a wrong-machine receipt must keep the socket open".into());
    }
    wrong_machine
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;
    let mut replacement = ConnectedLink::open(fixture).await?;
    if fixture::receive(&mut replacement.socket).await? != offered {
        return Err("replacement did not receive the original unacknowledged record".into());
    }
    drop(original);
    receipt_result(
        &mut replacement.socket,
        message_id.clone(),
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    let status_limit = 1.try_into()?;
    let status_query = |message_id| WorkspaceRequest {
        request_id: None,
        workspace_id: None,
        operation: WorkspaceOperation::List {
            query: workspace_model::ListQuery::MessageStatus { message_id },
            cursor: workspace_model::ListCursor::start(workspace_model::ListOrder::OldestFirst),
            limit: status_limit,
        },
    };
    let status = replacement.call(status_query(message_id.clone())).await?;
    if !matches!(
        workspace_reply(&status.reply)?.result,
        WorkspaceResult::Done(WorkspaceAnswer::MessageStatus {
            status: workspace_model::central_mail::MessageStatus::Delivered {
                deadline_passed: false,
                ..
            }
        })
    ) {
        return Err("the receipt did not settle the owning message row".into());
    }
    let another = serde_json::from_value(json!({
        "requestId":"019f0000-0000-7000-8000-00000000cf33",
        "operation":{
            "kind":"sendMessage","to":session,"body":"push after the old socket closes",
            "delivery":"queue","replyTo":null,"sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?
        },
    }))?;
    let (_, replies) = replacement
        .batch(vec![RequestPayload::Mail(another)])
        .await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("missing reply after the older socket closes".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
        message_id: next_message,
    })) = result.as_ref()
    else {
        return Err("the replacement's next send was not accepted".into());
    };
    if !matches!(fixture::receive(&mut replacement.socket).await?, DataFrame::Deliver { message, .. } if message.message_id() == next_message)
    {
        return Err("closing the previous socket lost the registered replacement".into());
    }
    receipt_result(
        &mut replacement.socket,
        next_message.clone(),
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    if !matches!(
        workspace_reply(
            &replacement
                .call(status_query(next_message.clone()))
                .await?
                .reply
        )?
        .result,
        WorkspaceResult::Done(WorkspaceAnswer::MessageStatus {
            status: workspace_model::central_mail::MessageStatus::Delivered {
                deadline_passed: false,
                ..
            }
        })
    ) {
        return Err("second receipt must settle before reconnect".into());
    }
    let settled_rows = || Statement {
        sql: "SELECT message_id,message_kind,state,updated_at FROM mail_messages ORDER BY message_id",
        args: vec![],
    };
    let before = fixture.runtime().read(settled_rows()).await?;
    super::renewal::advance(fixture, 1).await?;
    let mut reconnected = ConnectedLink::open(fixture).await?;
    receipt_result(
        &mut reconnected.socket,
        message_id.clone(),
        central_link_protocol::ReceiptResult::Recorded,
    )
    .await?;
    if !matches!(
        workspace_reply(
            &reconnected
                .call(status_query(message_id.clone()))
                .await?
                .reply
        )?
        .result,
        WorkspaceResult::Done(WorkspaceAnswer::MessageStatus {
            status: workspace_model::central_mail::MessageStatus::Delivered {
                deadline_passed: false,
                ..
            }
        })
    ) {
        return Err("the replayed receipt must remain silent and keep the socket open".into());
    }
    if fixture.runtime().read(settled_rows()).await? != before {
        return Err("the replayed receipt rewrote a settled delivery or added an outcome".into());
    }
    reconnected.close().await
}

#[tokio::test]
async fn zero_time_direct_mail_is_never_offered_after_clock_rollback() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = zero_time_offer_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn zero_time_offer_journey(fixture: &Fixture) -> Result<()> {
    use runtime_primitives::Clock as _;

    let mut live = ConnectedLink::open(fixture).await?;
    let Caller::Session(session) = fixture::session_caller()? else {
        return Err("fixture must be a session".into());
    };
    super::renewal::advance(fixture, 2).await?;
    let accepted =
        chrono::DateTime::from_timestamp(signed::NOW_SECONDS + 2, 0).ok_or("fixture UTC")?;
    let offered_request = |id, sent_at| {
        serde_json::from_value(json!({
            "requestId":id,
            "operation":{
                "kind":"sendMessage","to":session,"body":"zero time offer probe",
                "delivery":"queue","replyTo":null,"sentAt":sent_at,
            }
        }))
        .map(RequestPayload::Mail)
    };
    let zero = offered_request(
        "019f0000-0000-7000-8000-00000000cfa0",
        accepted - chrono::Duration::minutes(16),
    )?;
    let (_, replies) = live.batch(vec![zero]).await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("the zero-time send did not answer".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
        message_id: zero_message,
    })) = result.as_ref()
    else {
        return Err("the zero-time send was not admitted".into());
    };
    let central = fixture.runtime();
    let store = central.statement_host();
    use central_storage::{SqlValue, Statement};
    let stored_mark = store.read(Statement {
        sql: "SELECT created_at,delivery_window_ends_at,cancel_due_at FROM mail_messages WHERE message_id=?",
        args: vec![SqlValue::Text(zero_message.to_string())],
    }).await?.rows;
    let [record] = stored_mark.as_slice() else {
        return Err("missing zero-time send".into());
    };
    let [
        SqlValue::Text(stored_acceptance),
        SqlValue::Text(stored_cutoff),
        SqlValue::Text(stored_mark),
    ] = record.as_slice()
    else {
        return Err("expected acceptance, cutoff and mark from the stored row".into());
    };
    if stored_acceptance != stored_cutoff
        || stored_acceptance.parse::<workspace_model::Timestamp>()? != accepted
    {
        return Err("the admitted zero-time send did not store cutoff equal to acceptance".into());
    }
    let mark: workspace_model::Timestamp = stored_mark.parse()?;

    let corrected = accepted - chrono::Duration::seconds(1);
    fixture.clock.set_utc(corrected);
    let now = fixture.clock.now().unix_millis();
    fixture.host.set_external_time_ms(now).await?;
    assert!(fixture.runtime().control::<bool>("clock", &now).await?);

    let positive = offered_request("019f0000-0000-7000-8000-00000000cfa1", corrected)?;
    let (_, replies) = live.batch(vec![positive]).await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("the positive control did not answer".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        result.as_ref()
    else {
        return Err("the positive control was not accepted".into());
    };
    assert!(
        matches!(fixture::receive(&mut live.socket).await?, DataFrame::Deliver { message, time_left: Some(time_left), .. } if message.message_id() == message_id && u32::from(time_left) == 900_000)
    );
    live.socket.send(Message::Text("ping".into())).await?;
    assert!(matches!(live.socket.next().await, Some(Ok(Message::Text(pong))) if pong=="pong"));

    let mut reopened = ConnectedLink::open(fixture).await?;
    assert!(
        matches!(fixture::receive(&mut reopened.socket).await?, DataFrame::Deliver { message, time_left: Some(time_left), .. } if message.message_id() == message_id && u32::from(time_left) == 900_000)
    );
    reopened.socket.send(Message::Text("ping".into())).await?;
    assert!(matches!(reopened.socket.next().await, Some(Ok(Message::Text(pong))) if pong=="pong"));
    fixture.clock.set_utc(mark);
    fixture
        .host
        .set_external_time_ms(mark.timestamp_millis())
        .await?;
    central.after_event().await?;
    let facts = store.read(Statement {
        sql: "SELECT state, (SELECT count(*) FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=m.message_id), (SELECT json_extract(body,'$.outcome.kind') FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=m.message_id) FROM mail_messages m WHERE message_id=?",
        args: vec![SqlValue::Text(zero_message.to_string())],
    }).await?.rows;
    if facts
        != [vec![
            SqlValue::Text("pastTheMark".into()),
            SqlValue::Integer(1.try_into()?),
            SqlValue::Text("deliveryNotConfirmed".into()),
        ]]
    {
        return Err(format!(
            "the zero-window send did not produce one unconfirmed outcome at its mark: {facts:?}"
        )
        .into());
    }
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
    let mut socket = ConnectedLink::open(fixture).await?;
    let Caller::Session(destination) = fixture::session_caller()? else {
        return Err("fixture must be a session".into());
    };
    let request = serde_json::from_value(json!({
        "requestId":"019f0000-0000-7000-8000-00000000cfb2",
        "operation":{
            "kind":"sendMessage","to":destination,"body":"backlog template","delivery":"queue",
            "replyTo":null,"sentAt":sent_at,
        }
    }))?;
    let (_, replies) = socket.batch(vec![RequestPayload::Mail(request)]).await?;
    let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
        return Err("backlog template did not reply".into());
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        result.as_ref()
    else {
        return Err("backlog template was not queued".into());
    };
    if !matches!(fixture::receive(&mut socket.socket).await?, DataFrame::Deliver { message, .. } if message.message_id() == message_id)
    {
        return Err("backlog template did not have its live offer".into());
    }
    let central = fixture.runtime();
    let store = central.statement_host();
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
    super::renewal::advance(fixture, 62).await?;
    let mut reopened = ConnectedLink::open(fixture).await?;
    let mut offered = std::collections::BTreeSet::new();
    for _ in 0..=page_size {
        let Some(Ok(Message::Text(offer_text))) = reopened.socket.next().await else {
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
    if !offered.contains(message_id) {
        return Err("backlog omitted its original queued message".into());
    }
    reopened.socket.send(Message::Text("ping".into())).await?;
    if !matches!(reopened.socket.next().await, Some(Ok(Message::Text(pong))) if pong=="pong") {
        return Err("a second backlog page repeated an offer".into());
    }
    Ok(())
}
