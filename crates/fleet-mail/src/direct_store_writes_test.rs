use super::*;
use workspace_ids::SessionRef;

#[test]
fn the_executed_direct_write_files_fit_the_owning_central_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let id: MessageId = "019f0000-0000-7000-8000-000000000011".parse()?;
    let target: MachineId = "019f0000-0000-7000-8000-000000000012".parse()?;
    let request: workspace_model::RequestId = "019f0000-0000-7000-8000-000000000013".parse()?;
    let now: Timestamp = "2030-01-01T00:00:00Z".parse()?;
    let message = MessageRecord {
        message_id: id.clone(),
        sender: MessageSender::Central,
        target: SessionRef {
            machine_id: target.clone(),
            session_id: "fixture".to_owned().try_into()?,
        },
        body: MessageBody::Direct {
            body: "hello".to_owned().try_into()?,
            delivery: Default::default(),
            reply_to: None,
        },
        state: MessageState::Queued,
        source: None,
        created_at: now,
        updated_at: now,
    };
    let prior = DirectReducerState {
        subject: DirectMessageSubject {
            id: id.clone(),
            sender_session: None,
            sender_request_id: request.clone(),
            recipient: target.clone(),
            cutoff: now,
            mark: now,
        },
        lifecycle: DirectLifecycle::Queued,
        deadline_settled_at: None,
    };
    let encoding = DirectStateUpdate::new(&prior)?;
    macro_rules! write {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file!($file, $($arg),+) };
    }
    macro_rules! route {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file_as!(StoredReceiptRoute, $file, $($arg),+) };
    }
    let _ = insert_send!(write, message, now, request, now);
    let _ = insert_outcome!(write, message, now);
    macro_rules! kind {
        ($file:literal, $($arg:expr),+) => { sqlx::query_file_as!(StoredReceiptKind, $file, $($arg),+) };
    }
    let _ = route_query!(route, id);
    let _ = owned_kind_query!(kind, id, target);
    let _ = clear_attempt!(write, id);
    let _ = lifecycle_update!(write, encoding, prior, now);
    Ok(())
}
