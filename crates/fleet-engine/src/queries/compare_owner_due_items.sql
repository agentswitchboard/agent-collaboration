WITH owner_rows(kind, subject_key, owner_time, uses_occurrence) AS (
    SELECT 'wake_timer', wake_id, due_at, state = 'active'
    FROM wake_due_candidates
    UNION ALL
    SELECT 'automation_timer', automation_id, next_due_at, state = 'active'
    FROM schedule_due_candidates
    UNION ALL
    SELECT 'direct_message', message_id, cancel_due_at, 1
    FROM mail_messages
    WHERE message_kind = 'direct' AND state IN ('queued', 'delivered') AND cancel_due_at IS NOT NULL
    UNION ALL
    SELECT 'system_message_expiry', message_id, delivery_window_ends_at, 1
    FROM mail_messages
    WHERE message_kind IN ('notice', 'deliveryOutcome') AND state = 'queued'
        AND delivery_window_ends_at IS NOT NULL
),
owner_millis AS (
    SELECT kind, subject_key, uses_occurrence,
        unixepoch(owner_time)*1000+CAST(substr(strftime('%f',owner_time),4,3) AS INTEGER) AS raw_ms
    FROM owner_rows
),
occurrences AS (
    SELECT kind, subject_key, uses_occurrence,
        CASE WHEN raw_ms BETWEEN -9223372036854 AND 9223372036854 THEN raw_ms ELSE 0 END AS occurrence_ms
    FROM owner_millis
),
owner_timing AS (
    SELECT kind, subject_key, occurrence_ms,
        CASE WHEN uses_occurrence THEN occurrence_ms ELSE 0 END AS base_due_ms
    FROM occurrences
)
SELECT
    owner.kind AS "kind!: String",
    owner.subject_key AS "key?: String",
    owner.occurrence_ms AS "occurrence_ms!: i64",
    owner.base_due_ms AS "base_due_ms!: i64",
    CASE WHEN attempt.retired THEN NULL
         WHEN attempt.uses_subject_attempt THEN attempt.due_at_ms
         ELSE owner.base_due_ms END AS "effective_due_ms?: i64"
FROM owner_timing owner
LEFT JOIN engine_due_attempt_controls attempt
    ON attempt.machine_kind = owner.kind AND attempt.subject_key = owner.subject_key
