use super::*;
use central_link_protocol::{
    CorrelationId, MachineLinkCloseCode, Receipt, ReceiptBatch, ReceiptKind, ReceiptResult,
};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};

#[tokio::test]
async fn a_real_link_answers_one_and_sixty_four_receipts_and_refuses_invalid_frames() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = async {
        let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
        let correlation_id: CorrelationId = "019f0000-0000-7000-8000-00000000ce91".parse()?;
        let message_id = "019f0000-0000-7000-8000-00000000ce92".parse()?;
        let receipt = Receipt {
            message_id,
            kind: ReceiptKind::Refused { reason: workspace_model::central_mail::MessageRefusal::UnknownSession },
        };
        for count in [1, 64] {
            let receipts = ReceiptBatch::new(vec![receipt.clone(); count])?;
            send(&mut socket, &LinkFrame::Data(DataFrame::Receipts { correlation_id: correlation_id.clone(), receipts })).await?;
            let expected = LinkFrame::Data(DataFrame::ReceiptResults { correlation_id: correlation_id.clone(), results: vec![ReceiptResult::NoSuchMessage; count] });
            if receive(&mut socket).await? != expected {
                return Err("receipt result length, correlation or position changed".into());
            }
        }

        let invalid = [
            serde_json::json!({"kind":"receipts", "correlationId":correlation_id, "receipts":[]}),
            serde_json::json!({"kind":"receipts", "correlationId":correlation_id, "receipts":vec![receipt; 65]}),
            serde_json::json!({"kind":"ack","messageId":"019f0000-0000-7000-8000-00000000ce92","answer":{"kind":"accepted"}}),
        ];
        for frame in invalid {
            let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
            socket.send(Message::Text(frame.to_string())).await?;
            if !matches!(socket.next().await, Some(Ok(Message::Close { .. }))) {
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
    use workspace_model::central_mail::{CentralMailAnswer, MessageRefusal};
    let target = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "receipt-revocation".to_owned().try_into()?,
    };
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    let mut messages = Vec::new();
    for index in [81, 82] {
        let queued = super::delivery::mail_reply(&mut socket, &target, index, serde_json::json!({
            "kind":"sendMessage", "to":target, "body":"send before revocation", "delivery":"auto", "replyTo":null,
            "sentAt": chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?,
        })).await?;
        let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = queued else {
            return Err("test send did not queue".into());
        };
        if !matches!(receive(&mut socket).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == &message_id)
        {
            return Err("test send did not produce its own offer".into());
        }
        messages.push(message_id);
    }
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let central = fixture.host.central(&organization).await?;
    let private = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(fixture.signin_socket.clone())
        .build()?;
    let revocation = signed::RevocationJourney {
        client: &private,
        origin: "http://localhost",
        organization: signed::ORGANIZATION,
        now: signed::NOW_SECONDS,
    };
    let owner = revocation.prepare().await?;
    let mut held = central.runtime().pause_points().hold_engine(
        fleet_runtime::PausePoint::AfterEngineCommit,
        messages
            .first()
            .ok_or("the receipt prefix must have a message")?
            .as_str(),
    );
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
    send(
        &mut socket,
        &LinkFrame::Data(DataFrame::Receipts {
            correlation_id: "019f0000-0000-7000-8000-00000000ce98".parse()?,
            receipts,
        }),
    )
    .await?;
    held.until_held().await;
    let revoked = revocation
        .expect(
            &owner,
            fixture.enrollment_id.as_str(),
            serde_json::json!({"result":"revoked"}),
        )
        .await;
    held.release();
    revoked?;
    loop {
        let next = socket.next().await;
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
    let rows = central.runtime().statement_host().read(central_storage::Statement {
        sql: "SELECT state, failure_reason FROM mail_messages WHERE message_id IN (?,?) ORDER BY message_id",
        args: messages.iter().map(|id| central_storage::SqlValue::Text(id.to_string())).collect(),
    }).await?;
    if rows.rows
        != [
            vec![
                central_storage::SqlValue::Text("refused".into()),
                central_storage::SqlValue::Text("unknownSession".into()),
            ],
            vec![
                central_storage::SqlValue::Text("queued".into()),
                central_storage::SqlValue::Null,
            ],
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
    use central_storage::SqlValue as Cell;
    use workspace_model::central_mail::{CentralMailAnswer, MessageRefusal};

    let target = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "receipt-replay".to_owned().try_into()?,
    };
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    let mut messages = Vec::new();
    for index in [81, 82] {
        let queued = super::delivery::mail_reply(&mut socket, &target, index, serde_json::json!({
            "kind":"sendMessage", "to":target, "body":"send before reply loss", "delivery":"auto", "replyTo":null,
            "sentAt": chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?,
        })).await?;
        let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = queued else {
            return Err("test send did not queue".into());
        };
        if !matches!(receive(&mut socket).await?, LinkFrame::Data(DataFrame::Deliver { message, .. }) if message.message_id() == &message_id)
        {
            return Err("test send did not produce its own offer".into());
        }
        messages.push(message_id);
    }

    let [first, second] = messages.as_slice() else {
        return Err("the lost-answer proof requires two messages".into());
    };
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let central = fixture.host.central(&organization).await?;
    let refuse = |message_id| Receipt {
        message_id,
        kind: ReceiptKind::Refused {
            reason: MessageRefusal::UnknownSession,
        },
    };
    let setup_correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ce97".parse()?;
    let setup_frame = DataFrame::Receipts {
        correlation_id: setup_correlation.clone(),
        receipts: ReceiptBatch::new(vec![refuse(second.clone())])?,
    };
    send(&mut socket, &LinkFrame::Data(setup_frame)).await?;
    loop {
        let LinkFrame::Data(reply) = receive(&mut socket).await? else {
            return Err("unexpected control frame while settling the earlier message".into());
        };
        match reply {
            DataFrame::Deliver { .. } => {}
            DataFrame::ReceiptResults {
                correlation_id,
                results,
            } if correlation_id == setup_correlation && results == [ReceiptResult::Recorded] => {
                break;
            }
            other => return Err(format!("could not settle the earlier message: {other:?}").into()),
        }
    }

    let prior_outcome = central.runtime().statement_host().read(central_storage::Statement {
        sql: "SELECT message_id FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=?",
        args: vec![Cell::Text(second.to_string())],
    }).await?.rows;
    let [outcome_row] = prior_outcome.as_slice() else {
        return Err("expected the earlier message's single outcome".into());
    };
    let [Cell::Text(outcome_id)] = outcome_row.as_slice() else {
        return Err("expected an outcome message id".into());
    };
    let system_message: workspace_ids::MessageId = outcome_id.parse()?;
    let completed_at =
        chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture time")?;
    let mut hold = central
        .runtime()
        .pause_points()
        .hold_engine(fleet_runtime::PausePoint::AfterEngineCommit, first.as_str());
    let correlation: CorrelationId = "019f0000-0000-7000-8000-00000000ce99".parse()?;
    let frame = DataFrame::Receipts {
        correlation_id: correlation.clone(),
        receipts: ReceiptBatch::new(vec![
            refuse(first.clone()),
            Receipt {
                message_id: second.clone(),
                kind: ReceiptKind::Seen { completed_at },
            },
            Receipt {
                message_id: system_message.clone(),
                kind: ReceiptKind::Seen { completed_at },
            },
            Receipt {
                message_id: "019f0000-0000-7000-8000-00000000ce96".parse()?,
                kind: ReceiptKind::Seen { completed_at },
            },
        ])?,
    };
    send(&mut socket, &LinkFrame::Data(frame.clone())).await?;
    hold.until_held().await;
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
        if central
            .runtime()
            .statement_host()
            .read(facts(first))
            .await?
            .rows
            != expected("refused", 1)?
            || central
                .runtime()
                .statement_host()
                .read(facts(second))
                .await?
                .rows
                != expected("refused", 1)?
        {
            return Err(
                "the first receipt must commit without changing the already settled message".into(),
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    drop(socket);
    hold.release();
    evidence?;

    let mut reopened = fixture.connect().await?;
    receive(&mut reopened).await?;
    let mut answers = Vec::new();
    for _ in 0..2 {
        send(&mut reopened, &LinkFrame::Data(frame.clone())).await?;
        let positions = loop {
            let LinkFrame::Data(reply) = receive(&mut reopened).await? else {
                return Err("the receipt replay received an unexpected control frame".into());
            };
            match reply {
                DataFrame::Deliver { .. } => {}
                DataFrame::ReceiptResults {
                    correlation_id,
                    results,
                } if correlation_id == correlation => break results,
                other => return Err(format!("unexpected replay frame: {other:?}").into()),
            }
        };
        answers.push(positions);
    }
    let [original, repeated] = answers.as_slice() else {
        return Err("expected an answer to each complete frame".into());
    };
    if original.as_slice()
        != [
            ReceiptResult::Recorded,
            ReceiptResult::AlreadySettled,
            ReceiptResult::NotApplicable,
            ReceiptResult::NoSuchMessage,
        ]
        || repeated != original
    {
        return Err(format!("receipt answers changed by position: {answers:?}").into());
    }
    for message in [first, second] {
        if central
            .runtime()
            .statement_host()
            .read(facts(message))
            .await?
            .rows
            != expected("refused", 1)?
        {
            return Err("replay must settle both receipts with one outcome each".into());
        }
    }
    if central
        .runtime()
        .statement_host()
        .read(facts(&system_message))
        .await?
        .rows
        != expected("queued", 0)?
    {
        return Err("the inapplicable receipt changed the earlier outcome".into());
    }
    Ok(())
}

#[tokio::test]
async fn an_applying_receipt_checks_expiry_after_waiting_for_its_transaction() -> Result<()> {
    check_receipt_authority_at_apply(EnrollmentAtApply::StillEnrolled).await
}

#[tokio::test]
async fn an_applying_receipt_reports_owner_revocation_over_its_expired_transport() -> Result<()> {
    check_receipt_authority_at_apply(EnrollmentAtApply::OwnerRevoked).await
}

#[tokio::test]
async fn an_applying_receipt_reports_idle_ending_over_its_expired_transport() -> Result<()> {
    check_receipt_authority_at_apply(EnrollmentAtApply::EndedForIdle).await
}

pub(super) enum EnrollmentAtApply {
    StillEnrolled,
    OwnerRevoked,
    EndedForIdle,
}

impl EnrollmentAtApply {
    pub(super) fn stored_end_reason(&self) -> Option<&'static str> {
        match self {
            Self::StillEnrolled => None,
            Self::OwnerRevoked => Some("owner"),
            Self::EndedForIdle => Some("idle"),
        }
    }

    pub(super) fn expected_close(&self) -> MachineLinkCloseCode {
        match self {
            Self::StillEnrolled => MachineLinkCloseCode::Expired,
            Self::OwnerRevoked => MachineLinkCloseCode::Revoked,
            Self::EndedForIdle => MachineLinkCloseCode::Inactive,
        }
    }
}

async fn check_receipt_authority_at_apply(enrollment: EnrollmentAtApply) -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = receipt_expiry_journey(&fixture, enrollment).await;
    fixture.close().await?;
    result
}

async fn receipt_expiry_journey(fixture: &Fixture, enrollment: EnrollmentAtApply) -> Result<()> {
    use fleet_runtime::PausePoint;
    use workspace_model::central_mail::{CentralMailAnswer, MessageRefusal};
    let sent_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0).ok_or("fixture UTC")?;
    let recipient = workspace_ids::SessionRef {
        machine_id: INSTALLATION.parse()?,
        session_id: "receipt-expiry-barrier".to_owned().try_into()?,
    };
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    let send_result = super::delivery::mail_reply(&mut socket, &recipient, 83, serde_json::json!({
        "kind":"sendMessage","to":recipient,"body":"held receipt","delivery":"auto","replyTo":null,"sentAt":sent_at,
    })).await?;
    let CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) = send_result else {
        return Err("the receipt test send was not admitted".into());
    };
    receive(&mut socket).await?;
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let central = fixture.host.central(&organization).await?;
    let mut hold = central
        .runtime()
        .pause_points()
        .hold_central(PausePoint::BeforeReceiptTransaction);
    let receipt_frame = DataFrame::Receipts {
        correlation_id: "019f0000-0000-7000-8000-00000000ce94".parse()?,
        receipts: ReceiptBatch::new(vec![Receipt {
            message_id: message_id.clone(),
            kind: ReceiptKind::Refused {
                reason: MessageRefusal::UnknownSession,
            },
        }])?,
    };
    send(&mut socket, &LinkFrame::Data(receipt_frame)).await?;
    tokio::select! {
        () = hold.until_held() => {}
        early = socket.next() => {
            return Err(format!("receipt emitted a frame before the transaction barrier: {early:?}").into());
        }
    }
    let store = central.runtime().statement_host();
    // Commit a coherent ending through SQLite while the already-admitted receipt
    // holds Central's turn. A private revoke would wait for the same turn and
    // its socket observer would obscure what the transaction recheck reports.
    let applying: Result<()> = async {
        fixture.clock.advance(Duration::from_secs(600))?;
        if let Some(reason) = enrollment.stored_end_reason() {
            let ending = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
            let outcome = store.run_batch(ending).await?;
            if !matches!(outcome, BatchOutcome::Applied { .. }) {
                return Err(format!(
                    "could not persist the held receipt's enrollment ending: {outcome:?}"
                )
                .into());
            }
        }
        Ok(())
    }
    .await;
    hold.release();
    applying?;
    let reply = socket.next().await;
    let expected_close = enrollment.expected_close();
    if !matches!(reply, Some(Ok(Message::Close { code, .. })) if MachineLinkCloseCode::from_code(code.into()) == Some(expected_close))
    {
        return Err(
            format!("applying receipt must close with {expected_close:?}, got {reply:?}").into(),
        );
    }
    let rows = store.read(central_storage::Statement {
        sql: "SELECT state,failure_reason,(SELECT count(*) FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=message.message_id) FROM mail_messages AS message WHERE message_id=?",
        args: vec![SqlValue::Text(message_id.to_string())],
    }).await?.rows;
    if rows
        != [vec![
            SqlValue::Text("queued".into()),
            SqlValue::Null,
            SqlValue::Integer(0.try_into()?),
        ]]
    {
        return Err(format!("refused receipt changed owning rows: {rows:?}").into());
    }
    Ok(())
}

pub(super) fn stored_ending_batch(
    machine_id: &str,
    approval_id: &str,
    reason: &str,
) -> Result<StatementBatch> {
    let ended_at = chrono::DateTime::from_timestamp(signed::NOW_SECONDS + 600, 0)
        .ok_or("fixture transport deadline")?
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    Ok(StatementBatch {
        statements: vec![
            GuardedStatement {
                statement: Statement {
                    sql: "UPDATE login_machine_approvals SET state='ended',end_reason=?,last_grant_at=NULL,ended_at=?,updated_at=? WHERE approval_id=? AND machine_id=? AND state='enrolled' AND machine_client_id=approval_id",
                    args: vec![
                        SqlValue::Text(reason.to_owned()),
                        SqlValue::Text(ended_at.clone()),
                        SqlValue::Text(ended_at.clone()),
                        SqlValue::Text(approval_id.to_owned()),
                        SqlValue::Text(machine_id.to_owned()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            },
            GuardedStatement {
                statement: Statement {
                    sql: "UPDATE login_machines SET state='revoked',enrollment_generation=enrollment_generation+1,updated_at=? WHERE machine_id=? AND state='enrolled'",
                    args: vec![
                        SqlValue::Text(ended_at),
                        SqlValue::Text(machine_id.to_owned()),
                    ],
                },
                expect: RowExpectation::Exactly(1),
            },
        ],
        alarm: AlarmUpdate::Unchanged,
    })
}
