-- Test-owned enrollment and session facts, lent in the applying transaction.
CREATE TABLE IF NOT EXISTS fixture_enrollments (
    organization_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    standing TEXT NOT NULL,
    PRIMARY KEY (organization_id,machine_id,enrollment_id)
);

CREATE TABLE IF NOT EXISTS fixture_sessions (
    organization_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    enrollment_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    live INTEGER NOT NULL,
    parent_machine TEXT,
    parent_enrollment TEXT,
    parent_session TEXT,
    PRIMARY KEY (organization_id,machine_id,enrollment_id,session_id)
);
