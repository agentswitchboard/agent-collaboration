-- An Engine projection of current work. The owner row remains the authority.
CREATE TABLE engine_due_entries (
    due_entry_id TEXT NOT NULL PRIMARY KEY,
    work_kind TEXT NOT NULL,
    entry_role TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    occurrence_ms INTEGER,
    base_due_ms INTEGER,
    effective_due_ms INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (work_kind, entry_role, subject_key)
);
CREATE INDEX engine_due_entries_seek
ON engine_due_entries(work_kind, entry_role, effective_due_ms, subject_key)
WHERE effective_due_ms IS NOT NULL;
