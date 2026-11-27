INSERT INTO login_machines (
    machine_id, label, state, enrollment_generation, created_at, updated_at
) VALUES (
    '019ac19f-aa00-7000-8000-00000000a101', 'Fixture machine', 'enrolled', 1,
    '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'
);

INSERT INTO login_machine_approvals (
    approval_id, request_id, machine_id, label, allowed_workspaces, state,
    approved_by_person_id, permission_checked, base_generation, code_expires_at,
    machine_client_id, last_grant_at, created_at, updated_at
) VALUES (
    '019ac19f-aa00-7000-8000-00000000a102', '019ac19f-aa00-7000-8000-00000000a160',
    '019ac19f-aa00-7000-8000-00000000a101', 'Fixture machine', '[]', 'enrolled',
    '019ac19f-aa00-7000-8000-00000000000b', 'machine:enroll', 0, '1970-01-01T00:00:00.000Z',
    '019ac19f-aa00-7000-8000-00000000a102', '1970-01-01T00:00:00.000Z',
    '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'
);
