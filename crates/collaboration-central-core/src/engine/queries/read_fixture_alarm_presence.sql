SELECT EXISTS(
    SELECT 1 FROM sqlite_schema WHERE type = 'view' AND name = 'engine_probe_due_rows'
) AS "installed!: i64"
