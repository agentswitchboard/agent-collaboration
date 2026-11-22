# Approval request protocol tests

The `integration` target exercises the public Serde contracts, wording-independent option duration, answer validation, and the answer projection visible to a destination. Hegel generates choices and answers against separately specified legal sets.

With `wire-export` on a native host, the same target also checks that every expected wire root is exported and that the generated TypeScript is byte-identical to the tracked file. To update the file after a deliberate contract change, run the export test from the workspace root.

```sh
UPDATE_APPROVAL_TYPES=1 cargo test --locked -p approval-request-protocol --features wire-export --test integration approval_typescript_contains_expected_roots_and_matches_generation
```

The production crate has no requirement for a Router process, Central host, provider or storage.
