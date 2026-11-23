SELECT
    'legacy_notice_windows' AS "kind!: String",
    window_id AS "key!: String",
    due_at AS "stored_time!: String"
FROM mail_notification_windows
WHERE due_at IS NOT NULL
UNION ALL
SELECT 'legacy_workspace_drains', workspace_id, drain_due_at
FROM mail_workspace_drains
WHERE drain_due_at IS NOT NULL
