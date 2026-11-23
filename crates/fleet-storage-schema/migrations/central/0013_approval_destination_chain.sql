-- The position records the destination order even when successive forwards
-- share a wall-clock sample. A read settles a session destination's notice.
ALTER TABLE approval_request_destinations ADD COLUMN position INTEGER NOT NULL DEFAULT 0;
ALTER TABLE approval_request_destinations ADD COLUMN first_read_at TEXT;

DROP INDEX approval_destinations_one_parent;
CREATE UNIQUE INDEX approval_destinations_position
ON approval_request_destinations (approval_row_id, position);
CREATE UNIQUE INDEX approval_destinations_session
ON approval_request_destinations (approval_row_id, session_id)
WHERE destination_kind = 'parent_session';
CREATE INDEX approval_destinations_session_reads
ON approval_request_destinations (
    organization_id, session_machine_id, session_enrollment_id, session_id, approval_row_id
)
WHERE destination_kind = 'parent_session';
