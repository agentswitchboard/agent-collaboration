-- The approval fixtures admit authority separately. This row supplies only
-- the indexed alarm query's provisioned-object precondition.
INSERT INTO signin_organization_identity(id,organization_statement,created_at,updated_at)
VALUES ('019ac19f-aa00-7000-8000-00000000a100','approval alarm fixture',0,0)
ON CONFLICT(id) DO NOTHING;
