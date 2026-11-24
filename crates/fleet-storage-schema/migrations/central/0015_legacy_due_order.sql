-- Compare decoded deadlines so malformed values cannot hide behind future text keys.
-- SQLite accepts missing zones and rolls invalid dates forward. Check those
-- before conversion so the sentinel preserves the legacy owners' failure policy.
CREATE INDEX notice_window_decoded_due ON mail_notification_windows(
    CASE
        WHEN substr(due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(due_at,5,1) <> '-' THEN 0
        WHEN date(substr(due_at,1,10),'+0 days') IS NOT substr(due_at,1,10) THEN 0
        WHEN substr(due_at,11,1) NOT IN ('T','t',' ') THEN 0
        WHEN substr(ltrim(substr(due_at,12),char(9,10,13,32)),1,8) NOT GLOB '[0-2][0-9]:[0-5][0-9]:[0-5][0-9]' OR substr(ltrim(substr(due_at,12),char(9,10,13,32)),1,2) >= '24' THEN 0
        WHEN lower(substr(due_at,-1,1)) <> 'z' AND substr(due_at,-6) NOT GLOB '[+-][0-9][0-9]:[0-9][0-9]' THEN 0
        WHEN unixepoch(due_at) IS NULL OR abs(unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)) > 9223372036854 THEN 0
        ELSE unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)
    END
) WHERE due_at IS NOT NULL;

CREATE INDEX workspace_drain_decoded_due ON mail_workspace_drains(
    CASE
        WHEN substr(drain_due_at,1,4) NOT GLOB '[0-9][0-9][0-9][0-9]' OR substr(drain_due_at,5,1) <> '-' THEN 0
        WHEN date(substr(drain_due_at,1,10),'+0 days') IS NOT substr(drain_due_at,1,10) THEN 0
        WHEN substr(drain_due_at,11,1) NOT IN ('T','t',' ') THEN 0
        WHEN substr(ltrim(substr(drain_due_at,12),char(9,10,13,32)),1,8) NOT GLOB '[0-2][0-9]:[0-5][0-9]:[0-5][0-9]' OR substr(ltrim(substr(drain_due_at,12),char(9,10,13,32)),1,2) >= '24' THEN 0
        WHEN lower(substr(drain_due_at,-1,1)) <> 'z' AND substr(drain_due_at,-6) NOT GLOB '[+-][0-9][0-9]:[0-9][0-9]' THEN 0
        WHEN unixepoch(drain_due_at) IS NULL OR abs(unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER)) > 9223372036854 THEN 0
        ELSE unixepoch(drain_due_at)*1000+CAST(substr(strftime('%f',drain_due_at),4,3) AS INTEGER)
    END
) WHERE drain_due_at IS NOT NULL;
