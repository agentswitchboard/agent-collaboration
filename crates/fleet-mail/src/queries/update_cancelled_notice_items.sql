UPDATE mail_messages
SET body = ?1, state = ?2, source_change_id = ?3,
    source_workspace_id = ?4, source_thread_id = ?5, updated_at = ?6
WHERE message_id = ?7 AND message_kind = 'notice' AND state = 'queued' AND body = ?8
