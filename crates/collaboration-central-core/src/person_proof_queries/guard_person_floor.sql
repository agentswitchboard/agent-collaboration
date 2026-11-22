UPDATE signin_person_authorization_floors
SET id = id
WHERE id = ?
  AND organization_id = ?
  AND person_id = ?
  AND minimum_revision = ?
  AND state = ?
