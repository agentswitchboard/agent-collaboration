use super::*;

fn stored() -> StoredDirectState {
    StoredDirectState {
        message_id: "01890f2e-7b4c-7cc0-98c4-0000000000a1".into(),
        sender: r#"{"kind":"actor","value":"01890f2e-7b4c-7cc0-98c4-0000000000c1"}"#.into(),
        message_kind: "direct".into(),
        state: "queued".into(),
        failure_reason: None,
        cancel_due_at: Some("2100-01-01T00:20:00Z".into()),
        sender_request_id: Some("01890f2e-7b4c-7cc0-98c4-0000000000b1".into()),
        created_at: "2100-01-01T00:01:00Z".into(),
        updated_at: "2100-01-01T00:01:00Z".into(),
        target_machine_id: "01890f2e-7b4c-7cc0-98c4-000000000001".into(),
        delivery_window_ends_at: Some("2100-01-01T00:16:00Z".into()),
        delivered_arrived_at: None,
        delivered_waiting_reason: None,
        seen_completed_at: None,
        not_seen_waiting_reason: None,
    }
}

#[test]
fn stored_direct_state_requires_the_evidence_its_tag_claims() {
    let mut row = stored();
    assert_eq!(
        row.decode_direct_message().unwrap().lifecycle,
        DirectLifecycle::Queued
    );
    row.state = "delivered".into();
    assert!(row.decode_direct_message().is_err());
    row.delivered_arrived_at = Some("2100-01-01T00:01:30Z".into());
    assert!(matches!(
        row.decode_direct_message().unwrap().lifecycle,
        DirectLifecycle::Delivered(_)
    ));
    row.state = "seen".into();
    assert!(row.decode_direct_message().is_err());
    row.seen_completed_at = Some("2100-01-01T00:01:30Z".into());
    assert!(matches!(
        row.decode_direct_message().unwrap().lifecycle,
        DirectLifecycle::Final {
            receipt: FinalReceipt::Seen { .. },
            ..
        }
    ));
    row.failure_reason = Some("sessionClosed".into());
    assert!(row.decode_direct_message().is_err());
    row.seen_completed_at = None;
    row.state = "refused".into();
    assert!(matches!(
        row.decode_direct_message().unwrap().lifecycle,
        DirectLifecycle::Final {
            receipt: FinalReceipt::Refused {
                reason: mail::MessageRefusal::SessionClosed
            },
            ..
        }
    ));
}

#[test]
fn a_direct_mark_must_follow_acceptance_and_the_cutoff_on_the_grid() {
    for mark in [
        None,
        Some("2100-01-01T00:10:00Z"),
        Some("2100-01-01T00:21:00Z"),
    ] {
        let mut row = stored();
        row.cancel_due_at = mark.map(str::to_owned);
        assert!(row.decode_direct_message().is_err());
    }
}
