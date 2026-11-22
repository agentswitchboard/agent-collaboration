SELECT
    attempt.machine_kind AS "kind!: String",
    attempt.subject_key AS "key!: String",
    1 AS "is_reference!: i64"
FROM engine_due_attempt_controls attempt
WHERE attempt.retired = 1 AND CASE attempt.machine_kind
    WHEN 'wake_timer' THEN EXISTS (
        SELECT 1 FROM wake_due_candidates WHERE wake_id = attempt.subject_key
    )
    WHEN 'automation_timer' THEN EXISTS (
        SELECT 1 FROM schedule_due_candidates WHERE automation_id = attempt.subject_key
    )
    WHEN 'direct_message' THEN EXISTS (
        SELECT 1 FROM mail_messages
        WHERE message_id = attempt.subject_key AND message_kind = 'direct'
            AND state IN ('queued', 'delivered') AND cancel_due_at IS NOT NULL
    )
    WHEN 'system_message_expiry' THEN EXISTS (
        SELECT 1 FROM mail_messages
        WHERE message_id = attempt.subject_key AND message_kind IN ('notice', 'deliveryOutcome')
            AND state = 'queued' AND delivery_window_ends_at IS NOT NULL
    )
    ELSE 0
END
UNION ALL
SELECT work_kind, subject_key, 0
FROM engine_due_entries
WHERE entry_role = 'item' AND effective_due_ms IS NULL
