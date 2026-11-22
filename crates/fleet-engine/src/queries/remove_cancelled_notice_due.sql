DELETE FROM engine_due_entries
WHERE work_kind = 'system_message_expiry' AND entry_role = 'item' AND subject_key = ?1
    AND EXISTS (
        SELECT 1 FROM mail_messages
        WHERE message_id = ?1 AND message_kind = 'notice' AND state = 'cancelled'
    )
