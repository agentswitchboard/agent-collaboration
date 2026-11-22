-- The contract switches with an empty mailbox. Reopen retains the applied
-- migration ledger, and changed receipt fields are validated by the mail owner.
ALTER TABLE mail_messages ADD COLUMN delivered_arrived_at TEXT;
ALTER TABLE mail_messages ADD COLUMN delivered_waiting_reason TEXT;
ALTER TABLE mail_messages ADD COLUMN seen_completed_at TEXT;
ALTER TABLE mail_messages ADD COLUMN not_seen_waiting_reason TEXT;

DROP INDEX messages_cancel_due;
CREATE INDEX messages_cancel_due ON mail_messages(cancel_due_at)
WHERE message_kind='direct' AND state IN ('queued','delivered') AND cancel_due_at IS NOT NULL;

DROP VIEW direct_message_due_rows;
CREATE VIEW direct_message_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='direct_message'
)
SELECT 'on_grid', 1, s.message_id, CASE WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER) END,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms ELSE CASE WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER))>9223372036854 THEN 0 ELSE unixepoch(s.cancel_due_at)*1000+CAST(substr(strftime('%f',s.cancel_due_at),4,3) AS INTEGER) END END, source_attempt.control_present IS NULL
FROM mail_messages s INDEXED BY messages_cancel_due
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='direct_message' AND a.subject_key=s.message_id
LEFT JOIN source_attempt ON 1
WHERE s.message_kind='direct' AND s.state IN ('queued','delivered')
  AND s.cancel_due_at IS NOT NULL AND COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;
