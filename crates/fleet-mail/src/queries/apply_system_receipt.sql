UPDATE mail_messages SET state=?, failure_reason=?, updated_at=?
WHERE message_id=? AND target_machine_id=? AND message_kind=? AND state='queued'
  AND failure_reason IS NULL
