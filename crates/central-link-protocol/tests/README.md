# Central link protocol proof

Unit cases are colocated in `src/link_codec_test.rs` and `src/link_request_test.rs`.
They exercise the actual text codec, strict direction and semantic decoding,
UUIDv7 correlation and distinct write-id policy. No integration or e2e target is
needed for this driver-free vocabulary. Socket admission and hibernation belong
to the later host slice.

`src/mac_call_capabilities_test.rs` covers the Mac-to-Central announcement,
including a required, unique list of known call kinds. The focused Specta test
checks the generated `contracts/mac-call-capabilities.ts` against the codec.

```sh
cargo nextest run --locked -p central-link-protocol --features wire-export --lib --no-tests fail -E 'test(mac_call_capabilities)'
```
