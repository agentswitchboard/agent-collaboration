//! Human-readable copies of the state stored in the Mac's enrollment record.

use std::io::{self, Write};

use switchboard_machine_enrollment::{EnrollmentEnd, EnrollmentMetadata, InvalidReason};

pub(crate) const ENROLLED_TEXT: &str = "This Mac is enrolled.";

pub(crate) fn write_enrollment_status(
    output: &mut impl Write,
    metadata: &EnrollmentMetadata,
) -> io::Result<()> {
    match metadata {
        EnrollmentMetadata::Empty => writeln!(output, "This Mac is not enrolled."),
        EnrollmentMetadata::Active {
            key, deployment, ..
        } => {
            writeln!(output, "{ENROLLED_TEXT}")?;
            writeln!(output, "Organization: {}", key.organization_id)?;
            writeln!(output, "Sign-in: {}", deployment.origin())?;
            writeln!(
                output,
                "This is what the Mac has stored. It does not show whether the link to central is up."
            )
        }
        EnrollmentMetadata::Ended { key, why, .. } => {
            writeln!(
                output,
                "This Mac's enrollment ended: {}.",
                ending_reason(*why)
            )?;
            writeln!(output, "Organization: {}", key.organization_id)
        }
    }
}

fn ending_reason(why: EnrollmentEnd) -> &'static str {
    match why {
        EnrollmentEnd::Denied => "sign-in denied the enrollment",
        EnrollmentEnd::Expired => "the device code expired",
        EnrollmentEnd::Invalid(reason) => match reason {
            InvalidReason::GrantRefused => "sign-in refused the device code",
            InvalidReason::InvalidClient => "sign-in rejected the Router client",
            InvalidReason::InvalidRequest => "sign-in rejected the enrollment request",
            InvalidReason::UnknownError => "sign-in returned an unrecognized error",
            InvalidReason::MalformedAnswer => "sign-in returned a malformed answer",
            InvalidReason::UnexpectedStatus => "sign-in returned an unexpected response status",
            InvalidReason::WrongEnrollment => "sign-in answered for another enrollment",
            InvalidReason::OversizedAnswer => "sign-in returned an answer that was too large",
        },
        EnrollmentEnd::RefreshRefused => "sign-in refused to renew the enrollment",
        EnrollmentEnd::RefreshClientDisabled => "sign-in disabled the enrollment",
        EnrollmentEnd::EndedByCentral => "central ended the enrollment",
        EnrollmentEnd::LocalCredentialLost => "the Mac lost its enrollment credential",
        EnrollmentEnd::Revoked => "the enrollment was revoked",
        EnrollmentEnd::Inactive => "the enrollment became inactive",
    }
}
