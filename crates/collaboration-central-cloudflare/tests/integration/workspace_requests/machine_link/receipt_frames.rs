use super::*;
use central_link_protocol::{Receipt, ReceiptBatch, ReceiptKind, ReceiptResult};

#[tokio::test]
async fn a_real_link_answers_one_and_sixty_four_receipts_and_refuses_invalid_frames() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let correlation_id: CorrelationId = "019f0000-0000-7000-8000-00000000ce91".parse()?;
        let message_id = "019f0000-0000-7000-8000-00000000ce92".parse()?;
        let receipt = Receipt {
            message_id,
            kind: ReceiptKind::Refused { reason: workspace_model::central_mail::MessageRefusal::UnknownSession },
        };
        for count in [1, 64] {
            let receipts = ReceiptBatch::new(vec![receipt.clone(); count])?;
            fixture::send(&mut link.socket, &DataFrame::Receipts { correlation_id: correlation_id.clone(), receipts }).await?;
            let expected = DataFrame::ReceiptResults { correlation_id: correlation_id.clone(), results: vec![ReceiptResult::NoSuchMessage; count] };
            if fixture::receive(&mut link.socket).await? != expected {
                return Err("receipt result length, correlation or position changed".into());
            }
        }

        let invalid = [
            serde_json::json!({"kind":"receipts", "correlationId":correlation_id, "receipts":[]}),
            serde_json::json!({"kind":"receipts", "correlationId":correlation_id, "receipts":vec![receipt; 65]}),
            serde_json::json!({"kind":"ack","messageId":"019f0000-0000-7000-8000-00000000ce92","answer":{"kind":"accepted"}}),
        ];
        for frame in invalid {
            let mut link = ConnectedLink::open(&fixture).await?;
            link.socket.send(Message::Text(frame.to_string())).await?;
            if !matches!(link.socket.next().await, Some(Ok(Message::Close { .. }))) {
                return Err("malformed or old receipt frame must close without results".into());
            }
        }
        Ok(())
    }.await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn a_mid_frame_revocation_preserves_only_the_committed_receipt_prefix() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = revoke_between_receipts(&fixture).await;
    fixture.close().await?;
    result
}

async fn revoke_between_receipts(fixture: &Fixture) -> Result<()> {
    use central_link_protocol::RequestPayload;
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal};
    let mut link = ConnectedLink::open(fixture).await?;
    let Caller::Session(target) = fixture::session_caller()? else {
        return Err("receipt fixture needs a session".into());
    };
    let mut messages = Vec::new();
    for request_id in [
        "019f0000-0000-7000-8000-00000000ce81",
        "019f0000-0000-7000-8000-00000000ce82",
    ] {
        let request = serde_json::from_value(serde_json::json!({
            "requestId":request_id,
            "operation": {
                "kind":"sendMessage","to":target,"body":"send before revocation","delivery":"auto",
                "replyTo":null,"sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?,
            }
        }))?;
        let (_, replies) = link.batch(vec![RequestPayload::Mail(request)]).await?;
        let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
            return Err("mail reply missing".into());
        };
        let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
            result.as_ref()
        else {
            return Err("test send did not queue".into());
        };
        if !matches!(fixture::receive(&mut link.socket).await?, DataFrame::Deliver { message, .. } if &message.message_id == message_id)
        {
            return Err("test send did not produce its own offer".into());
        }
        messages.push(message_id.clone());
    }
    let runtime = fixture.runtime();
    let origin = fixture.host.signin_origin();
    let revocation = signed::RevocationJourney {
        client: &fixture.client,
        origin: &origin,
        organization: ORGANIZATION,
        now: signed::NOW_SECONDS,
    };
    let owner = revocation.prepare().await?;
    let hold = runtime
        .hold_engine(
            "afterEngineCommit",
            messages
                .first()
                .ok_or("the receipt prefix must have a message")?
                .as_str(),
        )
        .await;
    let receipts = ReceiptBatch::new(
        messages
            .iter()
            .cloned()
            .map(|message_id| Receipt {
                message_id,
                kind: ReceiptKind::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
            })
            .collect(),
    )?;
    fixture::send(
        &mut link.socket,
        &DataFrame::Receipts {
            correlation_id: "019f0000-0000-7000-8000-00000000ce98".parse()?,
            receipts,
        },
    )
    .await?;
    runtime.until_held_engine(hold).await;
    let revoked = revocation
        .expect(
            &owner,
            fixture.enrollment.as_str(),
            serde_json::json!({"result":"revoked"}),
        )
        .await;
    runtime.release_engine(hold).await;
    revoked?;
    loop {
        let next = link.socket.next().await;
        match next {
            Some(Ok(Message::Close { .. })) => break,
            Some(Ok(Message::Text(text))) => {
                let frame = central_link_protocol::decode_text(
                    &text,
                    central_link_protocol::FrameDirection::ToRouter,
                )?;
                if !matches!(
                    frame,
                    central_link_protocol::LinkFrame::Data(DataFrame::Deliver { .. })
                ) {
                    return Err(
                        format!("unexpected frame from revoked receipt batch: {frame:?}").into(),
                    );
                }
            }
            other => {
                return Err(
                    format!("expected a close after the committed prefix, got {other:?}").into(),
                );
            }
        }
    }
    fixture.host.c2_evict(&fixture.organization).await?;
    let rows = runtime.read(Statement {
        sql: "SELECT state, failure_reason FROM mail_messages WHERE message_id IN (?,?) ORDER BY message_id",
        args: messages.iter().map(|id| SqlValue::Text(id.to_string())).collect(),
    }).await?;
    if rows.rows
        != [
            vec![
                SqlValue::Text("refused".into()),
                SqlValue::Text("unknownSession".into()),
            ],
            vec![SqlValue::Text("queued".into()), SqlValue::Null],
        ]
    {
        return Err("only the committed receipt prefix may change state".into());
    }
    Ok(())
}

#[tokio::test]
async fn reconnect_replays_the_receipt_frame_after_its_answer_is_lost() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = replay_after_lost_answer(&fixture).await;
    fixture.close().await?;
    result
}

async fn replay_after_lost_answer(fixture: &Fixture) -> Result<()> {
    use central_link_protocol::RequestPayload;
    use central_storage::SqlValue as Cell;
    use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult, MessageRefusal};

    let mut socket = ConnectedLink::open(fixture).await?;
    let Caller::Session(target) = fixture::session_caller()? else {
        return Err("fixture must be a session".into());
    };
    let mut messages = Vec::new();
    for request_id in [
        "019f0000-0000-7000-8000-00000000ce81",
        "019f0000-0000-7000-8000-00000000ce82",
    ] {
        let request = serde_json::from_value(serde_json::json!({
            "requestId":request_id,
            "operation": {
                "kind":"sendMessage","to":target,"body":"send before reply loss","delivery":"auto",
                "replyTo":null,"sentAt":chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?,
            }
        }))?;
        let (_, replies) = socket.batch(vec![RequestPayload::Mail(request)]).await?;
        let [DataFrame::Reply { result, .. }] = replies.as_slice() else {
            return Err("mail reply missing".into());
        };
        let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
            result.as_ref()
        else {
            return Err("test send did not queue".into());
        };
        if !matches!(fixture::receive(&mut socket.socket).await?, DataFrame::Deliver { message, .. } if &message.message_id == message_id)
        {
            return Err("test send did not produce its own offer".into());
        }
        messages.push(message_id.clone());
    }

    let [first, second] = messages.as_slice() else {
        return Err("the lost-answer proof requires two messages".into());
    };
    let runtime = fixture.runtime();
    let hold = runtime
        .hold_engine("afterEngineCommit", first.as_str())
        .await;
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ce99".parse()?;
    let frame = DataFrame::Receipts {
        correlation_id: correlation.clone(),
        receipts: ReceiptBatch::new(
            messages
                .iter()
                .cloned()
                .map(|message_id| Receipt {
                    message_id,
                    kind: ReceiptKind::Refused {
                        reason: MessageRefusal::UnknownSession,
                    },
                })
                .collect(),
        )?,
    };
    fixture::send(&mut socket.socket, &frame).await?;
    runtime.until_held_engine(hold).await;
    let facts = |message: &workspace_ids::MessageId| central_storage::Statement {
        sql: "SELECT state,(SELECT COUNT(*) FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=m.message_id) FROM mail_messages m WHERE m.message_id=?",
        args: vec![Cell::Text(message.to_string())],
    };
    let expected = |state: &str, outcomes: i64| -> Result<Vec<Vec<Cell>>> {
        Ok(vec![vec![
            Cell::Text(state.to_owned()),
            Cell::Integer(outcomes.try_into()?),
        ]])
    };
    let evidence = async {
        if runtime.read(facts(first)).await?.rows != expected("refused", 1)?
            || runtime.read(facts(second)).await?.rows != expected("queued", 0)?
        {
            return Err("the first receipt must commit before the second receipt begins".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    drop(socket);
    runtime.release_engine(hold).await;
    evidence?;

    let mut reopened = ConnectedLink::open(fixture).await?;
    fixture::send(&mut reopened.socket, &frame).await?;
    loop {
        let reply = fixture::receive(&mut reopened.socket).await?;
        match reply {
            DataFrame::Deliver { .. } => {}
            DataFrame::ReceiptResults {
                correlation_id,
                results,
            } if correlation_id == correlation && results == [ReceiptResult::Recorded; 2] => break,
            other => return Err(format!("unexpected replay frame: {other:?}").into()),
        }
    }
    for message in [first, second] {
        if runtime.read(facts(message)).await?.rows != expected("refused", 1)? {
            return Err("replay must settle both receipts with one outcome each".into());
        }
    }
    Ok(())
}
