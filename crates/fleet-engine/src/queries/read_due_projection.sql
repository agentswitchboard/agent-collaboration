SELECT
    CASE WHEN typeof(due_entry_id) = 'text' THEN due_entry_id ELSE hex(due_entry_id) END AS "id!: String",
    CASE WHEN typeof(work_kind) = 'text' THEN work_kind ELSE hex(work_kind) END AS "kind!: String",
    CASE WHEN typeof(entry_role) = 'text' THEN entry_role ELSE hex(entry_role) END AS "role!: String",
    CASE WHEN typeof(subject_key) = 'text' THEN subject_key ELSE hex(subject_key) END AS "key!: String",
    CASE WHEN typeof(occurrence_ms) = 'integer' THEN occurrence_ms END AS "occurrence_ms?: i64",
    CASE WHEN typeof(base_due_ms) = 'integer' THEN base_due_ms END AS "base_due_ms?: i64",
    CASE WHEN typeof(effective_due_ms) = 'integer' THEN effective_due_ms END AS "effective_due_ms?: i64",
    CASE WHEN typeof(due_entry_id) = 'text' AND typeof(work_kind) = 'text'
        AND typeof(entry_role) = 'text' AND typeof(subject_key) = 'text'
        AND typeof(occurrence_ms) IN ('integer', 'null')
        AND typeof(base_due_ms) IN ('integer', 'null')
        AND typeof(effective_due_ms) IN ('integer', 'null')
        THEN 1 ELSE 0 END AS "valid_storage!: i64"
FROM engine_due_entries
ORDER BY work_kind, entry_role, subject_key
