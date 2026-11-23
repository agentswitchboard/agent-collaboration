SELECT id, organization_id, person_id, minimum_revision, state
FROM signin_person_authorization_floors
WHERE organization_id = ? AND person_id = ?
