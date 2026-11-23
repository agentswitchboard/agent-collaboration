INSERT INTO actor_sessions (
    actor_id, machine_id, enrollment_id, create_operation_id, create_contents_digest,
    create_attempt, agent_program, session_kind, creator_kind, creator_actor_id, secret_hash,
    session_state, reserved_until, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?, ?)
