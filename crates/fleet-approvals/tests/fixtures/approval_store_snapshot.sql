SELECT json_array('request', approval_row_id, request_id, record_state, waiting_status,
                  ended_at, ending_json, decided_operation_id, delivery_state, updated_at,
                  organization_id, machine_id, enrollment_id, router_run_id, contents_digest,
                  request_kind, holder_session_id, offer_json, invocation_digest, first_saved_at,
                  deadline_ms, stop_family, stop_cause, created_at)
FROM approval_requests
UNION ALL
SELECT json_array('destination', approval_destination_id, approval_row_id,
                  destination_kind, session_id, raised_notice_at, updated_at,
                  organization_id, session_machine_id, session_enrollment_id, created_at)
FROM approval_request_destinations
UNION ALL
SELECT json_array('run', router_run_id, registration_order, registered_at, retired_at, updated_at,
                  organization_id, machine_id, enrollment_id, created_at)
FROM approval_router_runs
UNION ALL
SELECT json_array('erased', approval_row_id, request_id, organization_id, router_run_id,
                  ending_json, created_at, updated_at)
FROM approval_erased_requests
UNION ALL
SELECT json_array('due', due_entry_id, work_kind, subject_key, occurrence_ms, effective_due_ms,
                  entry_role, base_due_ms, created_at, updated_at)
FROM engine_due_entries
UNION ALL
SELECT json_array('requestLog', request_id, caller, content_digest, answer, created_at, updated_at)
FROM runtime_request_log
ORDER BY 1
