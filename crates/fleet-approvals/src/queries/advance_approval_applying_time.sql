INSERT INTO approval_applying_time(organization_id,observed_ms,created_at,updated_at)
VALUES (?1,?2,?3,?3)
ON CONFLICT(organization_id) DO UPDATE
SET observed_ms=excluded.observed_ms, updated_at=excluded.updated_at
WHERE approval_applying_time.observed_ms < excluded.observed_ms
