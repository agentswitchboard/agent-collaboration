-- The contract switches with an empty mailbox. Reopen retains the applied
-- migration ledger, and changed receipt fields are validated by the mail owner.
ALTER TABLE mail_messages ADD COLUMN delivered_arrived_at TEXT;
ALTER TABLE mail_messages ADD COLUMN seen_completed_at TEXT;
ALTER TABLE mail_messages ADD COLUMN deadline_settled_at TEXT;

-- The indexed predicate excludes valid settlements and leaves malformed ones
-- visible to the same typed row decoder used by status, offers and applying.
DROP INDEX messages_cancel_due;
CREATE INDEX messages_cancel_due ON mail_messages(cancel_due_at)
WHERE message_kind = 'direct' AND state IN ('queued','delivered')
  AND (deadline_settled_at IS NULL OR NOT COALESCE(
      strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN delivery_window_ends_at GLOB '????-??-??T??:??:??.???Z' THEN delivery_window_ends_at END) = delivery_window_ends_at
      AND strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) = cancel_due_at
      AND delivery_window_ends_at >= created_at
      AND cancel_due_at >= delivery_window_ends_at
      AND (unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER)) - (unixepoch(CASE WHEN delivery_window_ends_at GLOB '????-??-??T??:??:??.???Z' THEN delivery_window_ends_at END) * 1000 + CAST(substr(delivery_window_ends_at, 21, 3) AS INTEGER)) < 300000
      AND (unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER)) % 300000 = 0
      AND abs((unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER))) <= 9223372036854
      AND strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN deadline_settled_at GLOB '????-??-??T??:??:??.???Z' THEN deadline_settled_at END) = deadline_settled_at
      AND deadline_settled_at >= cancel_due_at
      AND abs((unixepoch(CASE WHEN deadline_settled_at GLOB '????-??-??T??:??:??.???Z' THEN deadline_settled_at END) * 1000 + CAST(substr(deadline_settled_at, 21, 3) AS INTEGER))) <= 9223372036854
    , 0));

CREATE VIEW direct_message_due_candidates AS
SELECT message_id,
       CASE
         WHEN cancel_due_at GLOB '????-??-??T??:??:??.000Z'
          AND strftime('%Y-%m-%dT%H:%M:%fZ', cancel_due_at) = cancel_due_at
          AND strftime('%Y-%m-%dT%H:%M:%fZ', delivery_window_ends_at) = delivery_window_ends_at
          AND cancel_due_at >= delivery_window_ends_at
          AND (unixepoch(cancel_due_at) - unixepoch(delivery_window_ends_at)) * 1000
                - CAST(substr(delivery_window_ends_at, 21, 3) AS INTEGER) < 300000
          AND unixepoch(cancel_due_at) % 300 = 0
          AND abs(unixepoch(cancel_due_at) * 1000) <= 9223372036854
         THEN unixepoch(cancel_due_at) * 1000 ELSE 0
       END AS occurrence_ms
FROM mail_messages
WHERE message_kind = 'direct' AND state IN ('queued','delivered')
  AND (deadline_settled_at IS NULL OR NOT COALESCE(
      strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN delivery_window_ends_at GLOB '????-??-??T??:??:??.???Z' THEN delivery_window_ends_at END) = delivery_window_ends_at
      AND strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) = cancel_due_at
      AND delivery_window_ends_at >= created_at
      AND cancel_due_at >= delivery_window_ends_at
      AND (unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER)) - (unixepoch(CASE WHEN delivery_window_ends_at GLOB '????-??-??T??:??:??.???Z' THEN delivery_window_ends_at END) * 1000 + CAST(substr(delivery_window_ends_at, 21, 3) AS INTEGER)) < 300000
      AND (unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER)) % 300000 = 0
      AND abs((unixepoch(CASE WHEN cancel_due_at GLOB '????-??-??T??:??:??.???Z' THEN cancel_due_at END) * 1000 + CAST(substr(cancel_due_at, 21, 3) AS INTEGER))) <= 9223372036854
      AND strftime('%Y-%m-%dT%H:%M:%fZ', CASE WHEN deadline_settled_at GLOB '????-??-??T??:??:??.???Z' THEN deadline_settled_at END) = deadline_settled_at
      AND deadline_settled_at >= cancel_due_at
      AND abs((unixepoch(CASE WHEN deadline_settled_at GLOB '????-??-??T??:??:??.???Z' THEN deadline_settled_at END) * 1000 + CAST(substr(deadline_settled_at, 21, 3) AS INTEGER))) <= 9223372036854
    , 0));

DROP VIEW direct_message_due_rows;
CREATE VIEW direct_message_due_rows(due_lane, is_subject, subject_key, occurrence_ms, due_at_ms, contributes_to_alarm) AS
WITH source_attempt AS (
  SELECT 1 AS control_present, due_at_ms FROM engine_due_attempt_controls
  WHERE machine_kind='engine_source' AND subject_key='direct_message'
)
SELECT 'on_grid', 1, s.message_id, s.occurrence_ms,
       CASE WHEN a.uses_subject_attempt = 1 THEN a.due_at_ms ELSE s.occurrence_ms END,
       source_attempt.control_present IS NULL
FROM direct_message_due_candidates s
LEFT JOIN engine_due_attempt_controls a
  ON a.machine_kind='direct_message' AND a.subject_key=s.message_id
LEFT JOIN source_attempt ON 1
WHERE COALESCE(a.retired,0)=0
UNION ALL
SELECT 'on_grid', 0, NULL, NULL, due_at_ms, 1 FROM source_attempt;
