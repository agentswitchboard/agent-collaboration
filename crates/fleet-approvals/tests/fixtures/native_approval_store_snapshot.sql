SELECT json_array('erased', approval_row_id, request_id, organization_id, router_run_id,
                  ending_json, created_at, updated_at)
FROM approval_erased_requests
UNION ALL
SELECT json_array('requestRetention', approval_row_id, erase_after_ms, compact_ending_json)
FROM approval_requests
UNION ALL
SELECT json_array('destinationRead', approval_destination_id, request_id, position, first_read_at)
FROM approval_request_destinations
UNION ALL
SELECT json_array('runCleanup', router_run_id, enrollment_cleanup_ms, enrollment_cleanup_due_ms,
                  marker_cleanup_due_ms)
FROM approval_router_runs
UNION ALL
SELECT json_array('requestLogApproval', request_id, approval_row_id)
FROM runtime_request_log
UNION ALL
SELECT json_array('applyingTime', organization_id, observed_ms, created_at, updated_at)
FROM approval_applying_time
UNION ALL
SELECT json_array('attempt', attempt_id, machine_kind, subject_key, state, failure_count,
                  next_try_at, last_failure, last_failure_detail, first_failed_at,
                  last_failed_at, retired_at, created_at, updated_at)
FROM engine_work_attempts
UNION ALL
SELECT json_array('actor', actor_id, actor_kind, notice_cadence, created_at, updated_at)
FROM actor_identities
UNION ALL
SELECT json_array('personActor', actor_id, person_id, created_at, updated_at)
FROM actor_persons
ORDER BY 1
