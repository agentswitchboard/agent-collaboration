SELECT message_id AS "message_id!: String", sender, message_kind, state, failure_reason, cancel_due_at,
       sender_request_id, created_at, updated_at, target_machine_id, delivery_window_ends_at,
       delivered_arrived_at, seen_completed_at, deadline_settled_at
FROM mail_messages WHERE message_id=? AND sender=? AND message_kind='direct'
