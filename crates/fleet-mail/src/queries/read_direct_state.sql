SELECT message_id AS "message_id!: String", sender, message_kind, state, failure_reason, cancel_due_at,
       sender_request_id, created_at, updated_at, target_machine_id, delivery_window_ends_at,
       delivered_arrived_at, delivered_waiting_reason, seen_completed_at, not_seen_waiting_reason
FROM mail_messages WHERE message_id=?
