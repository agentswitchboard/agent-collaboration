use url::Url;

use super::{BindingGeneration, EnrollmentEnd, SignInDeployment, StoredEnrollment};

#[test]
fn a_durable_ending_keeps_its_generation_and_never_wraps() {
    let last = BindingGeneration::from_persisted(u64::MAX);
    let ended = StoredEnrollment::Ended {
        generation: last,
        why: EnrollmentEnd::Revoked,
    };
    assert_eq!(ended.generation(), last);
    assert_eq!(last.successor(), None);
    assert_eq!(
        StoredEnrollment::Empty
            .generation()
            .successor()
            .map(BindingGeneration::get),
        Some(1)
    );
}

#[test]
fn deployment_is_a_validated_origin() {
    for unauthorized in [
        "https://setup:secret@signin.example/",
        "http://signin.example/",
        "https://signin.example/private",
        "https://signin.example/?secret=private",
        "https://signin.example/#fragment",
    ] {
        let input = Url::parse(unauthorized).expect("URL fixture");
        assert!(
            SignInDeployment::new(input).is_err(),
            "untrusted origin was accepted"
        );
    }
    let input = Url::parse("https://signin.example/").expect("URL fixture");
    let trusted = SignInDeployment::new(input).expect("trusted HTTPS");
    assert_eq!(trusted.origin().as_str(), "https://signin.example/");
}
