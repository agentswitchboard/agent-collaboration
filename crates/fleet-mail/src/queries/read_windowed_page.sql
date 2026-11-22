SELECT message_id AS "message_id!: String",sender,target_machine_id,target_session,message_kind,body,state,
       failure_reason,source_change_id,recipient_actor,source_workspace_id,source_thread_id,
       created_at,updated_at,delivery_window_ends_at
FROM mail_messages
WHERE target_machine_id=?1 AND state='queued' AND message_kind IN ('direct','notice','deliveryOutcome')
  AND delivery_window_ends_at>?2 AND (message_kind!='direct' OR delivery_window_ends_at>created_at)
  AND (?3 IS NULL OR (created_at,message_id)>(?3,?4))
ORDER BY created_at,message_id LIMIT ?5
