-- The original approval row's UUIDv7 identifies its compact successor.
CREATE TABLE approval_erased_requests (
    approval_row_id TEXT NOT NULL PRIMARY KEY,
    organization_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    router_run_id TEXT NOT NULL REFERENCES approval_router_runs (router_run_id),
    ending_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX approval_erased_requests_identity
ON approval_erased_requests (organization_id, request_id, router_run_id);

CREATE INDEX approval_erased_requests_run
ON approval_erased_requests (router_run_id, approval_row_id);

ALTER TABLE approval_requests ADD COLUMN erase_after_ms INTEGER;
ALTER TABLE approval_requests ADD COLUMN compact_ending_json TEXT;

-- Approval-owned replay results carry their exact request row alongside the
-- operation id. Generic organization request results leave this column empty.
ALTER TABLE runtime_request_log ADD COLUMN approval_row_id TEXT REFERENCES approval_requests (approval_row_id);

CREATE INDEX approval_request_operation_results
ON runtime_request_log (approval_row_id)
WHERE approval_row_id IS NOT NULL;

ALTER TABLE approval_router_runs ADD COLUMN enrollment_cleanup_ms INTEGER;
ALTER TABLE approval_router_runs ADD COLUMN enrollment_cleanup_due_ms INTEGER;
ALTER TABLE approval_router_runs ADD COLUMN marker_cleanup_due_ms INTEGER;

CREATE INDEX approval_router_runs_enrollment_cleanup
ON approval_router_runs (organization_id, enrollment_id, registration_order);

DROP VIEW approval_request_expiry_due_rows;
CREATE VIEW approval_request_expiry_due_rows(
    due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm
) AS
WITH owner_timing AS (
    SELECT approval_row_id AS subject_key,
           CASE record_state WHEN 'waiting' THEN deadline_ms ELSE erase_after_ms END AS occurrence_ms, NULL AS cleanup_due_ms
    FROM approval_requests WHERE record_state IN ('waiting', 'ended')
    UNION ALL
    SELECT 'retired-run:' || runs.router_run_id,
           CAST(round(unixepoch(runs.retired_at, 'subsec') * 1000) AS INTEGER), marker_cleanup_due_ms
    FROM approval_router_runs AS runs
    WHERE runs.retired_at IS NOT NULL AND runs.marker_cleanup_due_ms IS NOT NULL AND EXISTS (
        SELECT 1 FROM approval_erased_requests AS marker
        WHERE marker.router_run_id = runs.router_run_id
    )
    UNION ALL
    SELECT 'ended-enrollment:' || router_run_id, enrollment_cleanup_ms, enrollment_cleanup_due_ms
    FROM approval_router_runs WHERE enrollment_cleanup_due_ms IS NOT NULL
), source_attempt AS (
    SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
    WHERE machine_kind = 'engine_source' AND subject_key = 'approval_request'
)
SELECT 'on_grid', 1, owner_timing.subject_key, owner_timing.occurrence_ms,
       CASE WHEN attempt.uses_subject_attempt = 1 THEN attempt.due_at_ms
            ELSE COALESCE(owner_timing.cleanup_due_ms, owner_timing.occurrence_ms + ((300000 - owner_timing.occurrence_ms % 300000) % 300000))
       END,
       source_attempt.control_present IS NULL
FROM owner_timing
LEFT JOIN engine_due_attempt_controls AS attempt
    ON attempt.machine_kind = 'approval_request' AND attempt.subject_key = owner_timing.subject_key
LEFT JOIN source_attempt ON 1
WHERE COALESCE(attempt.retired, 0) = 0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;
