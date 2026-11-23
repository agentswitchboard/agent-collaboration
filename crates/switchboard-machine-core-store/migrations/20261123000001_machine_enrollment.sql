-- Enrollment is absent on a newly created identity and stored on that same row.
ALTER TABLE machine_identity ADD COLUMN enrollment_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE machine_identity ADD COLUMN enrollment_json TEXT;
ALTER TABLE machine_identity ADD COLUMN enrollment_sealed BLOB;
