use url::Url;

use super::{BindingGeneration, EnrollmentEnd, EnrollmentKey, SignInDeployment, StoredEnrollment};

#[test]
fn a_durable_ending_keeps_its_generation_and_never_wraps() {
    let last = BindingGeneration::from_persisted(u64::try_from(i64::MAX).expect("SQLite maximum"))
        .expect("fixture generation");
    let ended = StoredEnrollment::Ended {
        generation: last,
        why: EnrollmentEnd::Revoked,
        key: EnrollmentKey {
            organization_id: "019a0000-0000-7000-8000-000000000001"
                .parse()
                .expect("organization"),
            machine_id: "019a0000-0000-7000-8000-000000000002"
                .parse()
                .expect("machine"),
            enrollment_id: "019a0000-0000-7000-8000-000000000003"
                .parse()
                .expect("enrollment"),
        },
    };
    assert_eq!(ended.generation(), last);
    assert_eq!(last.successor(), None);
    assert_eq!(BindingGeneration::from_persisted(last.get() + 1), None);
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
