# Session start tests

The integration target drives the reducer and its two ports through the public
API. It includes an independent transition model, a separate take and completion
executor, deterministic transition witnesses, wrong-build assertions, and
fixtures derived from central's session reducer and reply context. The executor
can retire a connection or change custody between take, completion, and
delivery. Unit tests with the snapshot implementation exercise each corrupt
record refusal through decoding and restart.
