INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,
    body,state,created_at,updated_at,delivery_window_ends_at)
VALUES (?, ?, ?, ?, 'deliveryOutcome', ?, 'queued', ?, ?, ?)
