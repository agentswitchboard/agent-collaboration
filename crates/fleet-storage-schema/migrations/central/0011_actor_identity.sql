CREATE TABLE actor_identities (
    actor_id TEXT PRIMARY KEY NOT NULL,
    actor_kind TEXT NOT NULL,
    notice_cadence TEXT NOT NULL DEFAULT 'm15',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE actor_sessions (
    actor_id TEXT PRIMARY KEY NOT NULL REFERENCES actor_identities(actor_id),
    machine_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    harness_session_id TEXT,
    create_operation_id TEXT NOT NULL,
    create_contents_digest TEXT NOT NULL,
    create_attempt INTEGER NOT NULL,
    agent_program TEXT NOT NULL,
    session_kind TEXT NOT NULL,
    creator_kind TEXT NOT NULL,
    creator_actor_id TEXT REFERENCES actor_sessions(actor_id),
    secret_hash TEXT NOT NULL,
    session_state TEXT NOT NULL,
    reserved_until TEXT NOT NULL,
    bound_at TEXT,
    settled_cause TEXT,
    settled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (machine_id, create_operation_id),
    UNIQUE (machine_id, harness_session_id)
);

CREATE TABLE actor_persons (
    actor_id TEXT PRIMARY KEY NOT NULL REFERENCES actor_identities(actor_id),
    person_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE actor_delegates (
    actor_id TEXT PRIMARY KEY NOT NULL REFERENCES actor_sessions(actor_id),
    parent_actor_id TEXT NOT NULL REFERENCES actor_sessions(actor_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX actor_sessions_by_machine_bound
    ON actor_sessions(machine_id, bound_at DESC, actor_id DESC) WHERE bound_at IS NOT NULL;
CREATE INDEX actor_sessions_by_bound
    ON actor_sessions(bound_at DESC, actor_id DESC) WHERE bound_at IS NOT NULL;
CREATE INDEX actor_sessions_unsettled_by_enrollment
    ON actor_sessions(enrollment_id, actor_id) WHERE session_state IN ('reserved', 'active');
CREATE INDEX actor_sessions_reserved_by_deadline
    ON actor_sessions(reserved_until, actor_id) WHERE session_state = 'reserved';
