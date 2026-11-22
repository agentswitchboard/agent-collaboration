UPDATE mail_messages
SET state=?, failure_reason=?, updated_at=?, delivered_arrived_at=?,
    delivered_waiting_reason=?, seen_completed_at=?, not_seen_waiting_reason=?
WHERE message_id=? AND message_kind='direct' AND target_machine_id=? AND state=?
