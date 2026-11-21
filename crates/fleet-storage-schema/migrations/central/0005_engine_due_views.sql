-- Job-owned due inputs. The lane unions keep each compound SELECT within the Workerd limit.
-- Ordinary rows and source controls have explicit, independent presence markers.
CREATE INDEX wakes_unknown_state_due ON automation_wakes(due_at) WHERE state NOT IN ('active','done','disabled');
CREATE VIEW wake_due_candidates AS
SELECT wake_id,due_at,state FROM automation_wakes INDEXED BY wakes_due WHERE state='active'
UNION ALL
SELECT wake_id,due_at,state FROM automation_wakes INDEXED BY wakes_unknown_state_due WHERE state NOT IN ('active','done','disabled');
CREATE INDEX automations_unknown_state_due ON automation_schedules(next_due_at) WHERE state NOT IN ('active','done','disabled') OR (state='active' AND next_due_at IS NULL);
CREATE VIEW schedule_due_candidates AS
SELECT automation_id,next_due_at,state FROM automation_schedules INDEXED BY automations_due WHERE state='active' AND next_due_at IS NOT NULL
UNION ALL
SELECT automation_id,next_due_at,state FROM automation_schedules INDEXED BY automations_unknown_state_due WHERE state NOT IN ('active','done','disabled') OR (state='active' AND next_due_at IS NULL);

CREATE VIEW engine_due_attempt_controls(machine_kind,subject_key,uses_subject_attempt,retired,due_at_ms) AS
SELECT machine_kind,subject_key,attempt_id IS NOT NULL,
       state='retired' AND last_failure IN ('corrupt_row','constraint_violated','statement_rejected','refused','other_store_unavailable'),
       CASE
         WHEN state='retired' AND last_failure IN ('corrupt_row','constraint_violated','statement_rejected','refused','other_store_unavailable') THEN NULL
         WHEN state='failing' AND last_failure IN ('corrupt_row','constraint_violated','statement_rejected','refused','other_store_unavailable') THEN CASE WHEN unixepoch(next_try_at) IS NULL OR abs(unixepoch(next_try_at)*1000+CAST(substr(strftime('%f',next_try_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(next_try_at)*1000+CAST(substr(strftime('%f',next_try_at),4,3) AS INTEGER) END
         ELSE 0
       END
FROM engine_work_attempts;

CREATE VIEW wake_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='wake_timer'
)
SELECT 'on_grid', 1, s.wake_id, CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='active' THEN 0 ELSE CASE WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.due_at)*1000+CAST(substr(strftime('%f',s.due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM wake_due_candidates s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='wake_timer' AND a.subject_key=s.wake_id
LEFT JOIN source_attempt ON 1
WHERE COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE VIEW schedule_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='automation_timer'
)
SELECT 'on_grid', 1, s.automation_id, CASE WHEN unixepoch(s.next_due_at) IS NULL OR abs(unixepoch(s.next_due_at)*1000+CAST(substr(strftime('%f',s.next_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.next_due_at)*1000+CAST(substr(strftime('%f',s.next_due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='active' THEN 0 ELSE CASE WHEN unixepoch(s.next_due_at) IS NULL OR abs(unixepoch(s.next_due_at)*1000+CAST(substr(strftime('%f',s.next_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.next_due_at)*1000+CAST(substr(strftime('%f',s.next_due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM schedule_due_candidates s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='automation_timer' AND a.subject_key=s.automation_id
LEFT JOIN source_attempt ON 1
WHERE COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE VIEW direct_message_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='direct_message'
)
SELECT 'on_grid', 1, s.message_id, CASE WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='queued' THEN 0 ELSE CASE WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM mail_messages s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='direct_message' AND a.subject_key=s.message_id
LEFT JOIN source_attempt ON 1
WHERE s.state='queued' AND s.cancel_due_at IS NOT NULL AND COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE VIEW system_message_expiry_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='system_message_expiry'
)
SELECT 'on_grid', 1, s.message_id, CASE WHEN unixepoch(s.delivery_window_ends_at) IS NULL OR abs(unixepoch(s.delivery_window_ends_at)*1000+CAST(substr(strftime('%f',s.delivery_window_ends_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.delivery_window_ends_at)*1000+CAST(substr(strftime('%f',s.delivery_window_ends_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms WHEN s.state!='queued' THEN 0 ELSE CASE WHEN unixepoch(s.delivery_window_ends_at) IS NULL OR abs(unixepoch(s.delivery_window_ends_at)*1000+CAST(substr(strftime('%f',s.delivery_window_ends_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.delivery_window_ends_at)*1000+CAST(substr(strftime('%f',s.delivery_window_ends_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM mail_messages s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='system_message_expiry' AND a.subject_key=s.message_id
LEFT JOIN source_attempt ON 1
WHERE s.state='queued' AND s.message_kind IN ('notice','deliveryOutcome') AND s.delivery_window_ends_at IS NOT NULL AND COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;

CREATE VIEW notice_window_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
-- A control governs the whole unit. The held arm must skip the window scan.
SELECT 'legacy', 0, NULL, NULL,
       CASE
         WHEN EXISTS (
           SELECT 1 FROM engine_due_attempt_controls
           WHERE machine_kind='legacy_notice_windows' AND subject_key='all'
         ) THEN (
           SELECT due_at_ms FROM engine_due_attempt_controls
           WHERE machine_kind='legacy_notice_windows' AND subject_key='all'
         )
         ELSE (
           SELECT MIN(CASE
             WHEN unixepoch(due_at) IS NULL
                  OR abs(unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER))>9223372036854
             THEN 0
             ELSE unixepoch(due_at)*1000+CAST(substr(strftime('%f',due_at),4,3) AS INTEGER)
           END)
           FROM mail_notification_windows
           WHERE due_at IS NOT NULL
         )
       END, 1;

CREATE VIEW workspace_drain_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='legacy_workspace_drains' AND subject_key='all'
)
SELECT 'legacy', 1, s.workspace_id, CASE WHEN unixepoch(s.drain_due_at) IS NULL OR abs(unixepoch(s.drain_due_at)*1000+CAST(substr(strftime('%f',s.drain_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.drain_due_at)*1000+CAST(substr(strftime('%f',s.drain_due_at),4,3) AS INTEGER) END,
       CASE WHEN unixepoch(s.drain_due_at) IS NULL OR abs(unixepoch(s.drain_due_at)*1000+CAST(substr(strftime('%f',s.drain_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.drain_due_at)*1000+CAST(substr(strftime('%f',s.drain_due_at),4,3) AS INTEGER) END, source_attempt.control_present IS NULL
FROM mail_workspace_drains s
LEFT JOIN source_attempt ON 1
WHERE s.drain_due_at IS NOT NULL
UNION ALL
SELECT 'legacy', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;



CREATE INDEX messages_system_expiry_due ON mail_messages(delivery_window_ends_at) WHERE state='queued' AND message_kind IN ('notice','deliveryOutcome') AND delivery_window_ends_at IS NOT NULL;

CREATE VIEW engine_grid_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM wake_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM schedule_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM direct_message_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM system_message_expiry_due_rows;

CREATE VIEW engine_legacy_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM notice_window_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM workspace_drain_due_rows;

CREATE VIEW engine_production_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_grid_due_rows
UNION ALL SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_legacy_due_rows;

CREATE VIEW engine_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
SELECT due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm FROM engine_production_due_rows;
