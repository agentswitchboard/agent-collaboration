# Approval request protocol tests

The `integration` target exercises the public Serde contracts, wording-independent option duration, answer validation, and the answer projection visible to a destination. Hegel generates choices and answers against separately specified legal sets.

With `wire-export` on a native host, the same target also checks that every expected wire root is exported and that the generated TypeScript is byte-identical to the tracked file. To update the file after a deliberate contract change, run the export test from the workspace root.

```sh
UPDATE_APPROVAL_TYPES=1 cargo nextest run --locked -p approval-request-protocol --features wire-export --test integration -E 'test(=typescript_export::approval_typescript_contains_expected_roots_and_matches_generation)'
```

The production crate has no requirement for a Router process, Central host, provider or storage.

The TypeScript fixture checks valid narrowing and the rejected cross-kind shapes.

```sh
tsc --noEmit --strict --skipLibCheck crates/approval-request-protocol/tests/typescript/approval-wire-shapes.ts
```

The fuller Typed calls table is checked by exhaustive Rust matches over each reply. The named-map walk checks the same committed wire roots at every nesting level. Digest vectors fix the canonical bytes of each Raise and private ending, and central-core compares those vectors with its signed-intent canonicalizer.
