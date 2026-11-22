WITH RECURSIVE fixture_marks(mark_number) AS (
  SELECT 2 WHERE ?1 >= 2 UNION ALL SELECT mark_number+1 FROM fixture_marks WHERE mark_number<?1
)
INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,
                          created_at,updated_at,cancel_due_at,delivery_window_ends_at)
SELECT printf('019e0000-0000-7000-8000-%012d',mark_number), message.sender,
       message.target_machine_id, message.target_session, 'direct', message.body, 'queued',
       message.created_at, message.updated_at, '2090-01-01T00:00:00.000Z', message.delivery_window_ends_at
FROM mail_messages message CROSS JOIN fixture_marks WHERE message.message_id=?2
