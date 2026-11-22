INSERT INTO signin_organization_identity(id,organization_statement,created_at,updated_at) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM signin_organization_identity) AND ?>=? AND ?<?
