# Session payload tests

The integration target exercises the load request and query, the typed session item, and the published session-read contract. The exact-byte fixtures compare literal JSON with Rust values. The schema-meaning tests exercise freshly derived schemas separately from the on-disk drift test.

## Covered contract files

- `contracts/<RustType>.schema.json` contains the Draft 2020-12 schema for each root.
- `contracts/<RustType>.fixtures.json` contains named canonical values serialized from Rust.
- `contracts/Item.reads.json` pairs accepted input with its expected canonical value.
- `contracts/Item.negatives.json` gives each refused input's root, reader path and reason. A schema exception is named when the instance schema accepts a token the reader refuses.
- `contracts/session-read-contract.json` names the version, exported roots, deferred roots and validation limits.

The validator registers `x-maxUtf8Bytes` for byte limits on text. The generated decimal pattern has an explicit end-of-input assertion. The semantic test verifies that a final line feed is refused.

Regenerate the tracked files after a deliberate contract change from the workspace root.

```sh
UPDATE_SESSION_READ_CONTRACT=1 cargo nextest run --locked -p switchboard-session-payloads --no-tests fail -E 'test(=contract_artifacts::session_read_contract_artifacts_match_generation)'
```

With update mode absent the same test compares the fixtures and manifest and checks both the JSON structure and exact bytes of every file.

```sh
cargo nextest run --locked -p switchboard-session-payloads --no-tests fail
```
