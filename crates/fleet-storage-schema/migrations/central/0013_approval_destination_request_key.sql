-- The cutover starts empty. Carry the public request key on each destination
-- so a person lookup can prove visibility before touching the request row.
ALTER TABLE approval_request_destinations ADD COLUMN request_id TEXT NOT NULL;

CREATE UNIQUE INDEX approval_destinations_people_lookup
ON approval_request_destinations (organization_id, request_id)
WHERE destination_kind = 'people';
