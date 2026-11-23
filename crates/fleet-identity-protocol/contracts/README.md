# Machine connection contracts

Each `<RustType>.schema.json` is derived from its Rust owner.
`<RustType>.fixtures.json` contains canonical serializer output checked against
literal examples from the item-two Program Design.
`<RustType>.negatives.json` contains rejected reads.
`<RustType>.reads.json`, where present, contains accepted `input` and expected
`canonical` output, including null-to-omitted normalization.

These fixtures describe serialization. The Verifier runs the same committed
corpora natively and under WebAssembly. Types are exported from
`fleet_identity_protocol`. The older connection fixtures use
`message_board::MachineLabel` and `message_board::SessionRef`. Session lifecycle
replies use the machine and session pair in `workspace_ids::SessionRef`.

```sh
UPDATE_FLEET_IDENTITY_CONTRACTS=1 cargo test -p fleet-identity-protocol
cargo test -p fleet-identity-protocol
```

The permanent activity, remote-start and identity-value tests
own the files and their freshness. They reject wrong tags, field casing and
unknown body fields in Serde and JSON Schema. Optional fields accept omission
or null on read; writers omit absent fields.

Every schema validator must enforce `x-maxUtf8Bytes`. Rust tests register it
with `jsonschema`; Zod consumers add a `TextEncoder` byte refinement.
Schemas validate canonical values; Rust additionally accepts and normalizes
surrounding whitespace on read. A padded label may exceed the raw schema bound
while its trimmed value is within Rust's bound; full read equivalence is not claimed.
Consumers copy files from a pinned producer commit and retain the type names.
The later producer/consumer ticket owns the check against real output.

Regenerate the session lifecycle TypeScript from the six Rust wire roots:

```sh
UPDATE_SESSION_LIFECYCLE_TYPES=1 cargo nextest run -p fleet-identity-protocol --features wire-export -E 'test(session_lifecycle_types_match_generated_typescript)'
```
