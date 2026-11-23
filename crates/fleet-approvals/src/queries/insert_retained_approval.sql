INSERT INTO approval_requests (
    approval_row_id, organization_id, machine_id, enrollment_id, router_run_id,
    request_id, contents_digest, record_state, request_kind, holder_session_id,
    offer_json, invocation_digest, waiting_status, first_saved_at, deadline_ms, created_at, updated_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'waiting', ?8, ?9, ?10, ?11, 'pending', ?12, ?13, ?12, ?12)
