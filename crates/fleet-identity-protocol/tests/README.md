# fleet-identity-protocol tests

Run the package with its workspace CI features:

```sh
cargo nextest run --locked --profile ci -p fleet-identity-protocol
```

`integration/main.rs` covers identity values, connection wire, remote starts,
activity, and the create, bind, and end session contracts. The session tests
check every reply case, malformed records at every nesting level, and fixed
create digest vectors. Library unit tests sit beside their implementations.

With `wire-export` enabled, the integration target checks the generated
TypeScript against the six request and reply roots.

Each aggregate declares shared helper implementations once. Existing fixture
directories remain in place. Only nonempty test categories are declared in
Cargo.toml; CI selects no separate e2e category for this package.

A former binary `<root>` and test `<case>` becomes its category binary and
test `<root>::<case>`. Library test identities do not change. Nextest preserves
one process per case and the existing scheduling and deadline rules.
