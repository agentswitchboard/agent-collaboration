SELECT
CASE
    WHEN (SELECT COUNT(*) FROM signin_organization_identity) = 1 THEN NULLIF(min(
        COALESCE(
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM engine_work_attempts
                    WHERE machine_kind = 'engine_source' AND subject_key = 'engine_probe'
                ) THEN (
                    SELECT
                    CASE
                        WHEN state = 'retired' AND last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable')
                        THEN NULL
                        WHEN state = 'failing' AND last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable') THEN
                        CASE
                            WHEN unixepoch(next_try_at) IS NULL OR abs(unixepoch(next_try_at) * 1000 + CAST(substr(strftime('%f', next_try_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(next_try_at) * 1000 + CAST(substr(strftime('%f', next_try_at), 4, 3) AS INTEGER)
                        END
                        ELSE 0
                    END
                    FROM engine_work_attempts
                    WHERE machine_kind = 'engine_source' AND subject_key = 'engine_probe'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.item_id AS subject_key,
                        CASE
                            WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)
                        END AS occurrence_ms,
                        CASE
                            WHEN a.attempt_id IS NOT NULL THEN
                            CASE
                                WHEN a.state = 'failing' AND a.last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable') THEN
                                CASE
                                    WHEN unixepoch(a.next_try_at) IS NULL OR abs(unixepoch(a.next_try_at) * 1000 + CAST(substr(strftime('%f', a.next_try_at), 4, 3) AS INTEGER)) > 9223372036854
                                    THEN 0
                                    ELSE unixepoch(a.next_try_at) * 1000 + CAST(substr(strftime('%f', a.next_try_at), 4, 3) AS INTEGER)
                                END
                                ELSE 0
                            END
                            WHEN s.state != 'waiting' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM engine_probe_items s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'engine_probe' AND a.subject_key = s.item_id
                        WHERE s.state NOT IN ('fired', 'stopped') AND (s.machine_kind = 'engine_probe' AND s.due_at IS NOT NULL)
                        AND NOT (
                            COALESCE(a.state, '') = 'retired' AND a.last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable')
                        )
                    )
                )
            END, 9223372036855
        ),
        COALESCE(
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM engine_work_attempts
                    WHERE machine_kind = 'engine_source' AND subject_key = 'engine_alarm_only'
                ) THEN (
                    SELECT
                    CASE
                        WHEN state = 'retired' AND last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable')
                        THEN NULL
                        WHEN state = 'failing' AND last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable') THEN
                        CASE
                            WHEN unixepoch(next_try_at) IS NULL OR abs(unixepoch(next_try_at) * 1000 + CAST(substr(strftime('%f', next_try_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(next_try_at) * 1000 + CAST(substr(strftime('%f', next_try_at), 4, 3) AS INTEGER)
                        END
                        ELSE 0
                    END
                    FROM engine_work_attempts
                    WHERE machine_kind = 'engine_source' AND subject_key = 'engine_alarm_only'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.item_id AS subject_key,
                        CASE
                            WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)
                        END AS occurrence_ms,
                        CASE
                            WHEN a.attempt_id IS NOT NULL THEN
                            CASE
                                WHEN a.state = 'failing' AND a.last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable') THEN
                                CASE
                                    WHEN unixepoch(a.next_try_at) IS NULL OR abs(unixepoch(a.next_try_at) * 1000 + CAST(substr(strftime('%f', a.next_try_at), 4, 3) AS INTEGER)) > 9223372036854
                                    THEN 0
                                    ELSE unixepoch(a.next_try_at) * 1000 + CAST(substr(strftime('%f', a.next_try_at), 4, 3) AS INTEGER)
                                END
                                ELSE 0
                            END
                            WHEN s.state != 'waiting' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM engine_alarm_only_items s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'engine_alarm_only' AND a.subject_key = s.item_id
                        WHERE s.state NOT IN ('fired', 'stopped') AND (s.due_at IS NOT NULL) AND NOT (
                            COALESCE(a.state, '') = 'retired' AND a.last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable')
                        )
                    )
                )
            END, 9223372036855
        )
    ), 9223372036855)
    ELSE NULL
END AS "on_grid_due?: i64", NULL AS "legacy_due?: i64"
