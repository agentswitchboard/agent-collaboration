use std::ffi::OsString;

use switchboard_machine_enrollment::{
    CredentialStoreError, DeviceInstructions, EnrollmentEnd, EnrollmentFailure,
};

use super::{EnrollArguments, EnrollCommandError, show_device_instructions};

#[test]
fn a_scope_refusal_tells_the_person_to_remove_the_machine() {
    let error = EnrollCommandError::Enrollment(EnrollmentFailure::Store(
        CredentialStoreError::EnrollmentScopeMismatch,
    ));

    assert_eq!(
        error.to_string(),
        "Remove this machine first, then enroll it in the new organization or sign-in deployment."
    );
}

#[test]
fn enrollment_arguments_take_the_organization_and_sign_in_origin() {
    let arguments = EnrollArguments::parse(
        [
            "--organization",
            "019a0000-0000-7000-8000-000000000001",
            "--sign-in",
            "https://signin.example/",
        ]
        .map(OsString::from),
    )
    .expect("explicit enrollment arguments");

    assert_eq!(
        arguments.organization_id.to_string(),
        "019a0000-0000-7000-8000-000000000001"
    );
    assert_eq!(
        arguments.sign_in.origin().as_str(),
        "https://signin.example/"
    );
    let refused = EnrollArguments::parse(
        [
            "--organization",
            "019a0000-0000-7000-8000-000000000001",
            "--sign-in",
            "https://person:private@signin.example/",
        ]
        .map(OsString::from),
    );
    assert!(matches!(refused, Err(EnrollCommandError::SignInOrigin)));
}

#[test]
fn enrollment_command_shows_only_the_approval_link_and_personal_code() {
    let instructions = DeviceInstructions {
        user_code: "TEST-USER".to_owned(),
        verification_link: "https://signin.example/device?user_code=TEST-USER"
            .parse()
            .expect("fixture link"),
    };
    let mut output = Vec::new();

    show_device_instructions(&mut output, &instructions).expect("write to the captured terminal");

    assert_eq!(
        output,
        b"Open https://signin.example/device?user_code=TEST-USER\nCode TEST-USER\n"
    );
}

#[test]
fn terminal_enrollment_messages_say_what_to_do_next() {
    for (failure, wording) in [
        (
            EnrollmentFailure::Ended(EnrollmentEnd::Denied),
            "Ask an approver",
        ),
        (
            EnrollmentFailure::Ended(EnrollmentEnd::Expired),
            "fresh code",
        ),
        (
            EnrollmentFailure::Ended(EnrollmentEnd::LocalCredentialLost),
            "Run enroll again",
        ),
        (
            EnrollmentFailure::NoDurableStore,
            "no durable credential store",
        ),
        (
            EnrollmentFailure::ReplacementConflict,
            "Check the current enrollment",
        ),
    ] {
        assert!(
            EnrollCommandError::Enrollment(failure)
                .to_string()
                .contains(wording)
        );
    }
}
