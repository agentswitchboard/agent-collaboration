# Machine core store tests

Unit tests live beside the store modules. They exercise new database creation
and reopen and machine identity persistence, concurrent initialization and
rejection of damaged identity rows through real SQLite connections in temporary
directories.

Inbox tests exercise durable receipt decisions and handoff state through
SQLite, including an insert failure, generated duplicate and reconnect
sequences, retention, and rejection of corrupt rows.
