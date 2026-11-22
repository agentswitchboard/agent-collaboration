SELECT state, failure_reason FROM mail_messages
WHERE message_id=? AND target_machine_id=? AND message_kind=?
