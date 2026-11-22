# Session store tests

Unit tests sit beside the store implementation. They cover SQLite initialization
and reopening, and run with the crate's default features.

The integration target opens a copy of the historical provider operations
database and compares the whole SQLx history and every typed record. The
fixture was created by the store at commit
`c172f363b4de0497966bb05ec034db7b861f42d2` using its normal `open`,
`record_session`, `admit_with_target`, `record_terminal`, and `close`
methods. All five migrations ran when the old store first opened the file.

The fixture contains one completed provider prompt and two saved sessions.
The session records cover all reopen settings present and all settings
absent. The integration test writes out its expected values independently
of the database. The fixture's SHA-256 is
`e1c1dc54cb0aad155845f3e234c16239414023d8a74f3d112d76091207534b42`.
