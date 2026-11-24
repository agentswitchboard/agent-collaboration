-- Sign-in exposes whether the owning Central has exactly one retained stamp.
-- The Engine reads this view on its applying transaction.
CREATE VIEW signin_provisioned_for_engine AS
SELECT 1 AS provisioned
WHERE (SELECT COUNT(*) FROM signin_organization_identity) = 1;
