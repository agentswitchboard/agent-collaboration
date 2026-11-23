CREATE TABLE approval_router_runs (
    router_run_id TEXT NOT NULL PRIMARY KEY,
    organization_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    registration_order INTEGER NOT NULL,
    registered_at TEXT NOT NULL,
    retired_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX approval_router_runs_registration_order
ON approval_router_runs (organization_id, machine_id, enrollment_id, registration_order);

CREATE UNIQUE INDEX approval_router_runs_active_scope
ON approval_router_runs (organization_id, machine_id, enrollment_id)
WHERE retired_at IS NULL;

CREATE TABLE approval_requests (
    approval_row_id TEXT NOT NULL PRIMARY KEY,
    organization_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    router_run_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    contents_digest TEXT NOT NULL,
    record_state TEXT NOT NULL,
    request_kind TEXT,
    holder_session_id TEXT,
    offer_json TEXT,
    invocation_digest TEXT,
    waiting_status TEXT,
    first_saved_at TEXT,
    deadline_ms INTEGER,
    stop_cause TEXT,
    ended_at TEXT,
    ending_json TEXT,
    decided_operation_id TEXT,
    delivery_state TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX approval_requests_organization_request
ON approval_requests (organization_id, request_id);

-- Both Waiting and unpaid endings occupy the run's capacity.
CREATE INDEX approval_requests_outstanding
ON approval_requests (organization_id, machine_id, enrollment_id, router_run_id, approval_row_id)
WHERE record_state = 'waiting' OR (record_state = 'ended' AND delivery_state = 'owed');

CREATE INDEX approval_requests_owed
ON approval_requests (organization_id, machine_id, enrollment_id, router_run_id, approval_row_id)
WHERE record_state = 'ended' AND delivery_state = 'owed';

CREATE INDEX approval_requests_waiting_session
ON approval_requests (organization_id, machine_id, enrollment_id, holder_session_id, approval_row_id)
WHERE record_state = 'waiting' AND holder_session_id IS NOT NULL;

-- Independent V3 reference. Applying due selection uses the engine's due table.
CREATE INDEX approval_requests_waiting_deadline
ON approval_requests (deadline_ms, approval_row_id)
WHERE record_state = 'waiting';

-- Reference for engine diagnostics. Dispatch and arming use the engine index.
CREATE VIEW approval_request_expiry_due_rows(
    due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm
) AS
WITH source_attempt AS (
    SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
    WHERE machine_kind = 'engine_source' AND subject_key = 'approval_request'
)
SELECT 'on_grid', 1, requests.approval_row_id, requests.deadline_ms,
       CASE WHEN attempt.uses_subject_attempt = 1 THEN attempt.due_at_ms
            ELSE requests.deadline_ms END,
       source_attempt.control_present IS NULL
FROM approval_requests AS requests
LEFT JOIN engine_due_attempt_controls AS attempt
    ON attempt.machine_kind = 'approval_request' AND attempt.subject_key = requests.approval_row_id
LEFT JOIN source_attempt ON 1
WHERE requests.record_state = 'waiting' AND COALESCE(attempt.retired, 0) = 0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE TABLE approval_request_destinations (
    approval_destination_id TEXT NOT NULL PRIMARY KEY,
    approval_row_id TEXT NOT NULL REFERENCES approval_requests (approval_row_id),
    organization_id TEXT NOT NULL,
    destination_kind TEXT NOT NULL,
    session_machine_id TEXT,
    session_enrollment_id TEXT,
    session_id TEXT,
    raised_notice_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX approval_destinations_request
ON approval_request_destinations (approval_row_id, approval_destination_id);

CREATE UNIQUE INDEX approval_destinations_one_people
ON approval_request_destinations (approval_row_id)
WHERE destination_kind = 'people';

CREATE UNIQUE INDEX approval_destinations_one_parent
ON approval_request_destinations (approval_row_id)
WHERE destination_kind = 'parent_session';

CREATE INDEX approval_destinations_people_reads
ON approval_request_destinations (organization_id, approval_row_id)
WHERE destination_kind = 'people';
