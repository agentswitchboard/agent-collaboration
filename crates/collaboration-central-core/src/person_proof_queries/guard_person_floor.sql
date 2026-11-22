UPDATE signin_person_authorization_floors
SET updated_at = updated_at
WHERE id = ?
  AND organization_id = ?
  AND person_id = ?
  AND minimum_revision = ?
  AND state = ?
