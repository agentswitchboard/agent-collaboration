-- Compare decoded deadlines so malformed values cannot hide behind future text keys.
-- The sentinel preserves the retry and retirement policy of the legacy owners.
CREATE INDEX notice_window_decoded_due ON mail_notification_windows(
    CASE WHEN substr(due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(due_at,5,1) <> '-' THEN 0 WHEN unixepoch(due_at) IS NULL
         OR abs(unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)) > 9223372036854
         THEN 0 ELSE unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER) END
) WHERE due_at IS NOT NULL;

CREATE INDEX workspace_drain_decoded_due ON mail_workspace_drains(
    CASE WHEN substr(drain_due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(drain_due_at,5,1) <> '-' THEN 0 WHEN unixepoch(drain_due_at) IS NULL
         OR abs(unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER)) > 9223372036854
         THEN 0 ELSE unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER) END
) WHERE drain_due_at IS NOT NULL;
