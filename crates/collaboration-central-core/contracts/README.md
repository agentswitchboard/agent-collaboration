# Central identity contracts

These schemas derive from `collaboration_central_core::identity_contract`.
Each file is keyed by type name.

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
Use `_Deserialize` for inputs to Central, including the signed headers and claims
Central only reads. Use `_Serialize` for Central's replies and the canonical admin
intent Central recomputes. Use the unsuffixed alias where the exporter emits a
single phase. `organization-admin-intent-vector.json`
contains a request, its canonical intent bytes, and the base64url SHA-256 digest.

Validated IDs are JSON string aliases. Timestamp decoding accepts exact JSON
integers from zero through `Number.MAX_SAFE_INTEGER`. Fence revisions start at
one. TypeScript represents these values as `number`. Rust applies the wire bounds
and the separate time and admission policy.

The native export feature is opt-in and is excluded from production builds.
Run the byte comparison, schema parity, and strict positive and negative
fixture checks from the repository root.

```sh
pnpm --dir verification install --frozen-lockfile
cargo nextest run --locked -p collaboration-central-core --features wire-export --lib --no-tests fail -E 'test(signin_wire_export_test)'
```

To update the generated declarations and canonical intent vector after a
deliberate contract change, run the two generators before checking the fixtures.

```sh
UPDATE_CENTRAL_SIGNIN_TYPES=1 cargo test --locked -p collaboration-central-core --features wire-export --lib signin_wire_export_test::central_signin_types_match_generated_serde_wire -- --exact
UPDATE_CENTRAL_SIGNIN_TYPES=1 cargo test --locked -p collaboration-central-core --features wire-export --lib signin_wire_export_test::canonical_signin_intent_hash_matches_shared_vector -- --exact
```

The [person approval result matrix](approval-person-results.md) describes the
WebSocket replies and their owning-store effects. The reply vectors pin their
JSON shapes. The Decide digest vector lives with its Rust command in Fleet Sign-in.
