-- The provisioned organization ID is the primary key of its applying-time floor.
CREATE TABLE approval_applying_time (
    organization_id TEXT NOT NULL PRIMARY KEY REFERENCES signin_organization_identity(id),
    observed_ms INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Preserve published occurrence identity and move only untouched owner alarm deadlines.
UPDATE engine_due_entries
SET effective_due_ms = CASE
        WHEN effective_due_ms = base_due_ms AND NOT EXISTS (
            SELECT 1 FROM engine_due_attempt_controls AS attempt
            WHERE attempt.machine_kind = 'approval_request'
              AND attempt.subject_key = engine_due_entries.subject_key
              AND (attempt.uses_subject_attempt = 1 OR attempt.retired = 1)
        ) THEN base_due_ms + ((300000 - base_due_ms % 300000) % 300000)
        ELSE effective_due_ms
    END,
    base_due_ms = base_due_ms + ((300000 - base_due_ms % 300000) % 300000)
WHERE work_kind = 'approval_request' AND entry_role = 'item' AND base_due_ms IS NOT NULL;

DROP VIEW approval_request_expiry_due_rows;
CREATE VIEW approval_request_expiry_due_rows(
    due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm
) AS
WITH source_attempt AS (
    SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
    WHERE machine_kind = 'engine_source' AND subject_key = 'approval_request'
)
SELECT 'on_grid', 1, requests.approval_row_id, requests.deadline_ms,
       CASE WHEN attempt.uses_subject_attempt = 1 THEN attempt.due_at_ms
            ELSE requests.deadline_ms + ((300000 - requests.deadline_ms % 300000) % 300000) END,
       source_attempt.control_present IS NULL
FROM approval_requests AS requests
LEFT JOIN engine_due_attempt_controls AS attempt
    ON attempt.machine_kind = 'approval_request' AND attempt.subject_key = requests.approval_row_id
LEFT JOIN source_attempt ON 1
WHERE requests.record_state = 'waiting' AND COALESCE(attempt.retired, 0) = 0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;
