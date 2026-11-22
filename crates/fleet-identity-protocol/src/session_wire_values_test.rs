use super::*;
use serde_json::json;

#[test]
fn attempt_is_positive_and_exact_with_a_checked_successor() {
    for invalid in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!("1"),
        json!(9_007_199_254_740_992_u64),
    ] {
        assert!(serde_json::from_value::<CreateSessionAttempt>(invalid).is_err());
    }
    let first: CreateSessionAttempt = serde_json::from_value(json!(1)).unwrap();
    let last = CreateSessionAttempt::try_from(MAXIMUM_EXACT_JSON_INTEGER).unwrap();

    assert_eq!(first.checked_next().unwrap().get(), 2);
    assert_eq!(last.checked_next(), None);
    assert_eq!(
        serde_json::to_value(last).unwrap(),
        json!(9_007_199_254_740_991_u64)
    );
}

#[test]
fn secrets_and_digests_have_one_spelling_and_debug_redacts_secrets() {
    let secret = SessionSecret::from_bytes(&[0xff; 32]);
    let encoded = secret.expose_secret();
    assert_eq!(encoded, "__________________________________________8");
    assert!(!format!("{secret:?}").contains(encoded));
    assert_eq!(serde_json::to_value(&secret).unwrap(), json!(encoded));
    assert_eq!(
        serde_json::from_value::<SessionSecret>(json!(encoded)).unwrap(),
        secret
    );
    assert!(serde_json::from_value::<CreateSessionDigest>(json!(encoded)).is_ok());

    let prefix = encoded.strip_suffix('8').unwrap();
    for invalid in [
        format!("{encoded}="),
        format!("{prefix}9"),
        encoded.replace('_', "/"),
        prefix.to_owned(),
        format!("{encoded}\n"),
    ] {
        assert!(serde_json::from_value::<SessionSecret>(json!(invalid)).is_err());
        assert!(serde_json::from_value::<CreateSessionDigest>(json!(invalid)).is_err());
    }
}

#[test]
fn central_times_require_fixed_width_utc_milliseconds() {
    let canonical = json!("2026-11-22T20:01:02.003Z");
    let time: SessionTime = serde_json::from_value(canonical.clone()).unwrap();
    assert_eq!(serde_json::to_value(time).unwrap(), canonical);

    for invalid in [
        "2026-11-22T20:01:02.003+00:00",
        "2026-11-22T20:01:02Z",
        "2026-11-22T20:01:02.0030Z",
        "2026-02-30T20:01:02.003Z",
        "+10000-11-22T20:01:02.003Z",
    ] {
        assert!(
            serde_json::from_value::<SessionTime>(json!(invalid)).is_err(),
            "{invalid}"
        );
    }
}
