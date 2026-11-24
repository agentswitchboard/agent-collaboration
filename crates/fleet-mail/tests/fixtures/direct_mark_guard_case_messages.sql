WITH guard_cases(case_id, message_kind, state, mark, cutoff) AS (
  VALUES
    (1, 'direct', 'queued', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z'),
    (2, 'direct', 'seen', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z'),
    (3, 'direct', 'delivered', '1970-01-01T00:10:00.000Z', '1970-01-01T00:05:00.001Z'),
    (4, 'direct', 'queued', 'malformed','1970-01-01T00:04:30.000Z'),
    (5, 'direct', 'queued', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z'),
    (6, 'direct', 'queued', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z'),
    (7, 'direct', 'delivered', NULL,'1970-01-01T00:04:30.000Z'),
    (8, 'notice', 'queued', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z'),
    (9, 'direct', 'queued', '1970-01-01T00:05:00.000Z', '1970-01-01T00:04:30.000Z')
)
INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,
                          created_at,updated_at,cancel_due_at,delivery_window_ends_at)
SELECT printf('019d0000-0000-7000-8000-%012d', guard_cases.case_id), message.sender,
       message.target_machine_id, message.target_session, guard_cases.message_kind, message.body,
       guard_cases.state, '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z', guard_cases.mark, guard_cases.cutoff
FROM mail_messages message CROSS JOIN guard_cases WHERE message.message_id=?
