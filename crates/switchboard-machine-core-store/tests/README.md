# Machine core store tests

Unit tests live beside the store modules. They exercise new database creation
and reopen and machine identity persistence, concurrent initialization and
rejection of damaged identity rows through real SQLite connections in temporary
directories.

Inbox tests exercise durable receipt decisions and handoff state through
SQLite, including an insert failure, generated duplicate and reconnect
sequences, retention, and rejection of corrupt rows.

The outbox tests exercise queued and claimed writes, caller-scoped views,
replay, deadlines, and terminal rows through SQLite. The storage contract
cases run against the production adapter. The Hegel model checks status,
waiting order, pending and outgoing views, and terminal payload removal.
