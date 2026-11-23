use super::*;

#[tokio::test]
async fn a_seen_receipt_cannot_settle_a_notice_and_ambiguous_session_keeps_its_reason() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let notice_id: workspace_ids::MessageId = id(981005);
    let recipient = session(&fixture.caller);
    raw(&central,
        "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        vec![
            text(&notice_id),
            text(serde_json::to_string(&MessageSender::Central).unwrap()),
            text(&recipient.machine_id),
            text(serde_json::to_string(&recipient).unwrap()),
            text("notice"),
            text(serde_json::to_string(&MessageBody::Notice { items: vec![] }).unwrap()),
            text("queued"),
            text(now.to_rfc3339()),
            text(now.to_rfc3339()),
        ],
    ).await;
    let stored_state = || async {
        central
            .read(Statement {
                sql: "SELECT state,failure_reason FROM mail_messages WHERE message_id=?",
                args: vec![text(&notice_id)],
            })
            .await
            .unwrap()
            .rows
    };
    let submit = async |message_id: workspace_ids::MessageId, receipt| {
        self::apply_fixture_receipt(&fixture, &central, &message_id, receipt).await
    };
    assert_eq!(
        submit(notice_id.clone(), ReceiptKind::Seen { completed_at: now }).await,
        MailReceiptSettlement::Stop(ReceiptResult::NotApplicable)
    );
    assert_eq!(
        stored_state().await,
        [vec![text("queued"), central_storage::SqlValue::Null]]
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Refused {
                reason: MessageRefusal::AmbiguousSession
            }
        )
        .await,
        MailReceiptSettlement::Applied
    );
    assert_eq!(
        stored_state().await,
        [vec![text("failed"), text("ambiguousSession")]]
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Refused {
                reason: MessageRefusal::AmbiguousSession
            }
        )
        .await,
        MailReceiptSettlement::Stop(ReceiptResult::Recorded)
    );
    assert_eq!(
        submit(
            notice_id.clone(),
            ReceiptKind::Delivered { arrived_at: now }
        )
        .await,
        MailReceiptSettlement::Stop(ReceiptResult::AlreadySettled)
    );
    assert_eq!(
        stored_state().await,
        [vec![text("failed"), text("ambiguousSession")]]
    );
    for (tag, reason) in [("failed", ""), ("cancelled", "ambiguousSession")] {
        raw(
            &central,
            "UPDATE mail_messages SET state=?,failure_reason=? WHERE message_id=?",
            vec![text(tag), text(reason), text(&notice_id)],
        )
        .await;
        assert_eq!(
            submit(notice_id.clone(), ReceiptKind::Seen { completed_at: now }).await,
            MailReceiptSettlement::Stop(ReceiptResult::NotApplicable)
        );
        let receipt = ReceiptKind::Delivered { arrived_at: now };
        let rejected = submit_receipt(&fixture, &central, &notice_id, receipt).await;
        assert!(
            matches!(rejected, Err(StoreFailure::Corrupt { .. })),
            "{rejected:?}"
        );
        assert_eq!(stored_state().await, [vec![text(tag), text(reason)]]);
    }
    fixture.host.close().await;
}
