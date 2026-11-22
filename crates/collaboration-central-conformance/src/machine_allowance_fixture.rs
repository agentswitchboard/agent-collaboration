//! Seed a machine's exact workspace set for host fixtures.

/// Arguments are the fixture's workspace ID array as JSON and its machine UUID.
pub const REPLACE_FIXTURE_ALLOWANCES_SQL: &str = "
WITH requested(workspace_id) AS (SELECT DISTINCT value FROM json_each(?1)),
     changes(workspace_id, state) AS (
       SELECT workspace_id, 'active' FROM requested
       UNION ALL
       SELECT workspace_id, 'withdrawn' FROM login_machine_allowances
       WHERE machine_id=?2 AND state='active'
         AND workspace_id NOT IN (SELECT workspace_id FROM requested)
     )
INSERT INTO login_machine_allowances (
    allowance_id, machine_id, workspace_id, state, change_request_id, change_origin,
    created_at, updated_at
)
SELECT substr(?2,25,8)||'-'||substr(?2,33,4)||'-7000-8000-'||substr(workspace_id,25,12),
       ?2, workspace_id, state, 'verification-seed', 'automatic',
       '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z'
FROM changes WHERE true
ON CONFLICT (machine_id, workspace_id) DO UPDATE
SET state=excluded.state
";
