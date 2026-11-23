# Integration tests

The signed integration target drives the enrollment client over loopback against
the scripted device-login contract. Poll timing uses the injected manual clock.
The server's recorded request envelopes and a child test's output establish what
can leave the client through those surfaces.

The [pinned Sign-in C5 fixtures](fixtures/sign-in-c5/README.md) supply every
request form and response envelope. The replay checks the original response
bodies and compares all three request forms received on the loopback server.
