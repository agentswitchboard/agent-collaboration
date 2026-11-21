# Central identity contracts

These schemas derive from `collaboration_central_core::identity_contract`.
Each file is keyed by type name, including shapes with identical fields such
as `DecideMachineToken` and `DisableMachineClient`.

- `<RustType>.fixtures.json`: canonical serializer output.
- `<RustType>.negatives.json`: rejected reads.
- `<RustType>.reads.json`, where present: accepted `input` and expected
  `canonical` serialization.

The Verifier runs the same committed corpora natively and under WebAssembly.

```sh
UPDATE_FLEET_IDENTITY_CONTRACTS=1 cargo test -p collaboration-central-core
cargo test -p collaboration-central-core
```

Permanent tests cover issuer decisions, observable enrollments and
organization admin contracts.

Bodies refuse unknown fields. Machine token extension claims accept the
issuer's registered JWT claims alongside them. Writers omit absent optional
fields, while readers accept omission or null. Every schema validator must
enforce `x-maxUtf8Bytes`; Rust tests register the keyword, and Zod consumers
add a `TextEncoder` byte refinement.
Schemas validate canonical values; Rust additionally accepts and normalizes
surrounding whitespace on read. A padded label may exceed the raw schema bound
while its trimmed value is within Rust's bound; full read equivalence is not claimed.

Reserved and approved enrollments remain internal to the authority. F1
defines no Rust `Principal` union. The Switchboard actor-union parsing and
cross-repository real-output checks remain with their consuming tickets.

## Sign-in TypeScript

`central-sign-in.ts` is the generated TypeScript contract for Sign-in.
Copy it from an exact Central commit and pin that commit in the consumer.
The Rust exporter uses the Serde serialization and deserialization phases.
Use `_Serialize` or `_Deserialize` where the phases have different shapes.
Validated IDs are JSON string aliases. Explicit integer overrides preserve the
JSON number representation. TypeScript cannot guarantee precision beyond
`Number.MAX_SAFE_INTEGER` or enforce the Rust constructors' runtime rules.

The native export feature is opt-in and is excluded from production builds.
With the verification package's dependencies installed, run the byte comparison
and strict fixture assignment checks from the repository root.

```sh
pnpm --dir verification install --frozen-lockfile
cargo test --locked -p collaboration-central-core --features wire-export --lib signin_wire_export_test
```

To update the generated file after a deliberate contract change, run the export
before rerunning both checks.

```sh
UPDATE_CENTRAL_SIGNIN_TYPES=1 cargo test --locked -p collaboration-central-core --features wire-export --lib central_signin_types_match_generated_serde_wire
```
