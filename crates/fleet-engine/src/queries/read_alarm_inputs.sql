SELECT
CASE
    WHEN (SELECT COUNT(*) FROM signin_organization_identity) = 1 THEN NULLIF(min(
        COALESCE(
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM engine_work_attempts
                    WHERE machine_kind = 'engine_source' AND subject_key = 'wake_timer'
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'wake_timer'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.wake_id AS subject_key,
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
                            WHEN s.state != 'active' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.due_at) IS NULL OR abs(unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.due_at) * 1000 + CAST(substr(strftime('%f', s.due_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM automation_wakes s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'wake_timer' AND a.subject_key = s.wake_id
                        WHERE s.state NOT IN ('done', 'disabled') AND (1) AND NOT (
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'automation_timer'
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'automation_timer'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.automation_id AS subject_key,
                        CASE
                            WHEN unixepoch(s.next_due_at) IS NULL OR abs(unixepoch(s.next_due_at) * 1000 + CAST(substr(strftime('%f', s.next_due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(s.next_due_at) * 1000 + CAST(substr(strftime('%f', s.next_due_at), 4, 3) AS INTEGER)
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
                            WHEN s.state != 'active' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.next_due_at) IS NULL OR abs(unixepoch(s.next_due_at) * 1000 + CAST(substr(strftime('%f', s.next_due_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.next_due_at) * 1000 + CAST(substr(strftime('%f', s.next_due_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM automation_schedules s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'automation_timer' AND a.subject_key = s.automation_id
                        WHERE s.state NOT IN ('done', 'disabled') AND (1) AND NOT (
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'direct_message'
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'direct_message'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.message_id AS subject_key,
                        CASE
                            WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at) * 1000 + CAST(substr(strftime('%f', s.cancel_due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(s.cancel_due_at) * 1000 + CAST(substr(strftime('%f', s.cancel_due_at), 4, 3) AS INTEGER)
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
                            WHEN s.state != 'queued' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.cancel_due_at) IS NULL OR abs(unixepoch(s.cancel_due_at) * 1000 + CAST(substr(strftime('%f', s.cancel_due_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.cancel_due_at) * 1000 + CAST(substr(strftime('%f', s.cancel_due_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM mail_messages s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'direct_message' AND a.subject_key = s.message_id
                        WHERE s.state NOT IN ('delivered', 'cancelled', 'failed') AND (s.state = 'queued'
                        AND s.cancel_due_at IS NOT NULL) AND NOT (
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'system_message_expiry'
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
                    WHERE machine_kind = 'engine_source' AND subject_key = 'system_message_expiry'
                )
                ELSE (
                    SELECT MIN(effective_due_ms)
                    FROM (
                        SELECT s.message_id AS subject_key,
                        CASE
                            WHEN unixepoch(s.delivery_window_ends_at) IS NULL OR abs(unixepoch(s.delivery_window_ends_at) * 1000 + CAST(substr(strftime('%f', s.delivery_window_ends_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(s.delivery_window_ends_at) * 1000 + CAST(substr(strftime('%f', s.delivery_window_ends_at), 4, 3) AS INTEGER)
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
                            WHEN s.state != 'queued' THEN 0
                            ELSE
                            CASE
                                WHEN unixepoch(s.delivery_window_ends_at) IS NULL OR abs(unixepoch(s.delivery_window_ends_at) * 1000 + CAST(substr(strftime('%f', s.delivery_window_ends_at), 4, 3) AS INTEGER)) > 9223372036854
                                THEN 0
                                ELSE unixepoch(s.delivery_window_ends_at) * 1000 + CAST(substr(strftime('%f', s.delivery_window_ends_at), 4, 3) AS INTEGER)
                            END
                        END AS effective_due_ms
                        FROM mail_messages s
                        LEFT JOIN engine_work_attempts a ON a.machine_kind = 'system_message_expiry' AND a.subject_key = s.message_id
                        WHERE s.state NOT IN ('delivered', 'cancelled', 'failed') AND (s.state = 'queued'
                        AND s.message_kind IN ('notice', 'deliveryOutcome') AND s.delivery_window_ends_at IS NOT NULL)
                        AND NOT (
                            COALESCE(a.state, '') = 'retired' AND a.last_failure IN ('corrupt_row', 'constraint_violated', 'statement_rejected', 'refused', 'other_store_unavailable')
                        )
                    )
                )
            END, 9223372036855
        )
    ), 9223372036855)
    ELSE NULL
END AS "on_grid_due?: i64",
CASE
    WHEN (SELECT COUNT(*) FROM signin_organization_identity) = 1 THEN NULLIF(min(
        COALESCE(
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM engine_work_attempts
                    WHERE machine_kind = 'legacy_notice_windows' AND subject_key = 'all'
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
                    WHERE machine_kind = 'legacy_notice_windows' AND subject_key = 'all'
                )
                ELSE (
                    SELECT MIN(
                        CASE
                            WHEN unixepoch(due_at) IS NULL OR abs(unixepoch(due_at) * 1000 + CAST(substr(strftime('%f', due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(due_at) * 1000 + CAST(substr(strftime('%f', due_at), 4, 3) AS INTEGER)
                        END
                    )
                    FROM mail_notification_windows
                    WHERE due_at IS NOT NULL
                )
            END, 9223372036855
        ),
        COALESCE(
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM engine_work_attempts
                    WHERE machine_kind = 'legacy_workspace_drains' AND subject_key = 'all'
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
                    WHERE machine_kind = 'legacy_workspace_drains' AND subject_key = 'all'
                )
                ELSE (
                    SELECT MIN(
                        CASE
                            WHEN unixepoch(drain_due_at) IS NULL OR abs(unixepoch(drain_due_at) * 1000 + CAST(substr(strftime('%f', drain_due_at), 4, 3) AS INTEGER)) > 9223372036854
                            THEN 0
                            ELSE unixepoch(drain_due_at) * 1000 + CAST(substr(strftime('%f', drain_due_at), 4, 3) AS INTEGER)
                        END
                    )
                    FROM mail_workspace_drains
                    WHERE drain_due_at IS NOT NULL
                )
            END, 9223372036855
        )
    ), 9223372036855)
    ELSE NULL
END AS "legacy_due?: i64"
