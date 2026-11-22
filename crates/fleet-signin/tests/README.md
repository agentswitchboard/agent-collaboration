# Fleet sign-in tests

Colocated unit tests cover issuer verification, stamp and cache decisions, and
Central SQLite query descriptions. The integration target proves that the
migrated schema rejects wrong SQL columns, bind counts and Rust row contracts.
The owning Tokio and Cloudflare suites
exercise the composed private HTTP behavior and the live machine verifier.
