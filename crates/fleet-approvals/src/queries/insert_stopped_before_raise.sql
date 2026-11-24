INSERT INTO approval_requests (
    approval_row_id, organization_id, machine_id, enrollment_id, router_run_id,
    request_id, contents_digest, record_state, stop_family, stop_cause, created_at, updated_at
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'stopped_before_raise', ?8, ?9, ?10, ?10)
