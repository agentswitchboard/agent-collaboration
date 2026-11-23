# Fleet approvals tests

The integration aggregate runs fixed histories and a separately written model for requests, authority, and holder delivery. The sibling unit test checks row successors and typed refusals against the declared edge table.

The store cases call the public applying service with the shipped statements on SQLite. They reopen the database, check route derivation and replay, inject transaction failures, and measure indexed deadline lookup and retirement at capacity. The sibling SQLx witness checks the owner deadline reference query against the migrated schema.

The same service and migrations run on Workerd in `verification/targets/workerd-approval-store.test.ts`. The test-only authority facts stand in for the separate machine-login and session owners.

The store review cases exercise commit and recipient binding, hidden corruption, deadline observations through replay or reoffer, and Owed recovery. Workerd reopens its persistent directory after each injected rollback before comparing the unchanged snapshot.
