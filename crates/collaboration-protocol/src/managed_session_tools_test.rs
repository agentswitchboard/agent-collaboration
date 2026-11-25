use super::ManagedToolBearer;

#[test]
fn managed_bearer_debug_omits_the_encoded_header() {
    let bearer = ManagedToolBearer::from_random_bytes(&[0; 32]);

    let debug = format!("{bearer:?}");

    assert_eq!(
        bearer.expose_header_value(),
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    );
    assert_eq!(debug, "ManagedToolBearer([REDACTED])");
}
