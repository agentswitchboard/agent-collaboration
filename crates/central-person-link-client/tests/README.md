# Person link client tests

- The colocated connection machine tests check both directions of the transition table and generate histories with reconnects, stale replies, and typed refusals.
- `integration` runs the person client against a TLS Sign-in fixture forwarding to real Tokio Central. It exercises DPoP, credential rotation, pagination, and reconnect after the authority deadline on one manual clock.
