//! Issuer cache and parser observations use explicit injected receipt times.
use super::{IssuerKeyCache, IssuerKeySet};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fleet_contracts::{IssuerKeyId, IssuerKeysUnavailable};
use fleet_runtime_ports::RawJwks;
use serde_json::{Value, json};

const SOURCE: &str = "https://issuer.example.invalid/jwks";
const NOW_MILLIS: i64 = 1_793_462_400_000;

fn at(offset_millis: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(NOW_MILLIS + offset_millis).unwrap()
}

fn key_id(value: &str) -> IssuerKeyId {
    IssuerKeyId::try_from(value.to_owned()).unwrap()
}

fn published_key(identity: &str, bytes: [u8; 32]) -> Value {
    json!({
        "kid":identity,
        "kty":"OKP",
        "crv":"Ed25519",
        "x":URL_SAFE_NO_PAD.encode(bytes),
        "alg":"EdDSA",
        "use":"sig"
    })
}

fn response(keys: Vec<Value>, cache_control: Option<&str>) -> RawJwks {
    RawJwks {
        body: serde_json::to_vec(&json!({"keys":keys})).unwrap(),
        cache_control: cache_control.map(str::to_owned),
    }
}

// The five-minute cap is local receipt time, not an issuer timestamp.
#[test]
fn issuer_keys_expire_at_the_five_minute_boundary() {
    for header in [None, Some("public, max-age=3600")] {
        let cache = IssuerKeyCache::default();
        cache
            .remember(
                SOURCE.to_owned(),
                response(vec![published_key("first", [7; 32])], header),
                at(0),
            )
            .unwrap();
        let retained = cache.fresh(SOURCE, at(299_999)).unwrap().unwrap();
        assert_eq!(
            retained.key(&key_id("first")).unwrap().public_key(),
            &[7; 32]
        );
        assert!(cache.fresh(SOURCE, at(300_000)).unwrap().is_none());
    }
}

#[test]
fn issuer_keys_preserve_shorter_and_zero_cache_lifetimes() {
    let cache = IssuerKeyCache::default();
    cache
        .remember(
            SOURCE.to_owned(),
            response(vec![published_key("short", [1; 32])], Some("max-age=1")),
            at(0),
        )
        .unwrap();
    assert!(cache.fresh(SOURCE, at(999)).unwrap().is_some());
    assert!(cache.fresh(SOURCE, at(1000)).unwrap().is_none());
    cache
        .remember(
            SOURCE.to_owned(),
            response(vec![published_key("zero", [2; 32])], Some("max-age=0")),
            at(1000),
        )
        .unwrap();
    assert!(cache.fresh(SOURCE, at(1000)).unwrap().is_none());
}

#[test]
fn issuer_keys_from_another_configured_source_are_never_reused() {
    let cache = IssuerKeyCache::default();
    cache
        .remember(
            SOURCE.to_owned(),
            response(vec![published_key("first", [3; 32])], None),
            at(0),
        )
        .unwrap();
    assert!(
        cache
            .fresh("https://another.example.invalid/jwks", at(0))
            .unwrap()
            .is_none()
    );
}

#[test]
fn unknown_keys_and_a_withdrawn_empty_set_are_explicit() {
    let keys =
        IssuerKeySet::decode(&response(vec![published_key("first", [4; 32])], None).body).unwrap();
    assert!(keys.key(&key_id("unknown")).is_none());
    let withdrawn = IssuerKeySet::decode(br#"{"keys":[]}"#).unwrap();
    assert!(withdrawn.key(&key_id("first")).is_none());
}

#[test]
fn malformed_key_sets_cannot_partially_replace_a_retained_set() {
    let cache = IssuerKeyCache::default();
    cache
        .remember(
            SOURCE.to_owned(),
            response(vec![published_key("original", [5; 32])], None),
            at(0),
        )
        .unwrap();
    let mut malformed = published_key("invalid", [6; 32]);
    malformed["x"] = json!(URL_SAFE_NO_PAD.encode([0; 31]));
    assert!(matches!(
        cache.remember(
            SOURCE.to_owned(),
            response(vec![published_key("new", [8; 32]), malformed], None),
            at(1),
        ),
        Err(IssuerKeysUnavailable::MalformedKeySet)
    ));
    let retained = cache.fresh(SOURCE, at(1)).unwrap().unwrap();
    assert_eq!(
        retained.key(&key_id("original")).unwrap().public_key(),
        &[5; 32]
    );
    assert!(retained.key(&key_id("new")).is_none());
}

#[test]
fn ambiguous_or_wrong_kind_keys_are_refused() {
    let first = published_key("same", [9; 32]);
    assert!(matches!(
        IssuerKeySet::decode(&response(vec![first.clone(), first], None).body),
        Err(IssuerKeysUnavailable::MalformedKeySet)
    ));
    for (field, value) in [
        ("kid", json!("")),
        ("kty", json!("RSA")),
        ("crv", json!("X25519")),
        ("alg", json!("RS256")),
        ("use", json!("enc")),
        ("x", json!("not base64")),
    ] {
        let mut invalid = published_key("first", [9; 32]);
        invalid[field] = value;
        assert!(matches!(
            IssuerKeySet::decode(&response(vec![invalid], None).body),
            Err(IssuerKeysUnavailable::MalformedKeySet)
        ));
    }
}

#[hegel::test(test_cases = 256)]
fn the_last_valid_receipt_controls_key_bytes_and_expiry(case: hegel::TestCase) {
    use hegel::generators as gs;
    let cache = IssuerKeyCache::default();
    let mut retained = None;
    let mut receipt = 0;
    for key_byte in 1..=case.draw(gs::integers::<u8>().min_value(1).max_value(32)) {
        receipt += case.draw(gs::integers::<i64>().max_value(301_000).min_value(0));
        let (header, lifetime) = match case.draw(gs::integers::<u8>().max_value(2)) {
            0 => (Some("max-age=0"), 0),
            1 => (Some("max-age=2"), 2_000),
            _ => (Some("max-age=303"), 300_000),
        };
        if case.draw(gs::booleans()) {
            cache
                .remember(
                    SOURCE.to_owned(),
                    response(vec![published_key("stable", [key_byte; 32])], header),
                    at(receipt),
                )
                .unwrap();
            retained = Some((key_byte, receipt + lifetime));
        } else {
            assert!(matches!(
                cache.remember(
                    SOURCE.to_owned(),
                    RawJwks {
                        body: b"{}".to_vec(),
                        cache_control: None
                    },
                    at(receipt)
                ),
                Err(IssuerKeysUnavailable::MalformedKeySet)
            ));
        }
        let expiry = retained.map_or(receipt, |(_, expiry)| expiry);
        for sample in [receipt, expiry - 1, expiry] {
            let actual = cache
                .fresh(SOURCE, at(sample))
                .unwrap()
                .map(|keys| *keys.key(&key_id("stable")).unwrap().public_key());
            let expected = retained
                .filter(|(_, expiry)| sample < *expiry)
                .map(|(value, _)| [value; 32]);
            assert_eq!(actual, expected, "receipt={receipt}, sample={sample}");
        }
    }
}
