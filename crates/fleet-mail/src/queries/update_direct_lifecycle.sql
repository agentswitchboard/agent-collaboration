UPDATE mail_messages
SET state=?, failure_reason=?, updated_at=?, delivered_arrived_at=?,
    seen_completed_at=?, deadline_settled_at=?
WHERE message_id=? AND message_kind='direct' AND target_machine_id=? AND state=?
  AND deadline_settled_at IS ?
