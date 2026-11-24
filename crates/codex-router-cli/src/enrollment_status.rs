//! Human-readable live link status and stored Mac enrollment.

use std::io::{self, Write};

use codex_router_host::MessageSystemStatus;
use switchboard_machine_enrollment::{EnrollmentEnd, EnrollmentMetadata, InvalidReason};
use switchboard_machine_enrollment_store::MachineEnrollmentStatus;

pub(crate) const ENROLLED_TEXT: &str = "This Mac is enrolled.";

pub(crate) fn write_router_link_status(
    output: &mut impl Write,
    status: Option<MessageSystemStatus>,
) -> io::Result<()> {
    use MessageSystemStatus as Status;

    let line = match status {
        None => "Router has no message link",
        Some(Status::Starting) => "starting",
        Some(Status::NotEnrolled) => "not enrolled",
        Some(Status::EnrollmentEnded) => "enrollment ended",
        Some(Status::EnrollmentUnreadable) => "could not read enrollment",
        Some(Status::Connecting) => "enrolled, connecting",
        Some(Status::Connected) => "enrolled, connected",
        Some(Status::Stopping) => "stopping",
        Some(Status::Restarting) => "enrolled, restarting",
        Some(Status::EnrollmentMismatch) => "enrollment does not match this Router",
        Some(Status::Stopped) => "link stopped",
    };
    writeln!(output, "{line}")
}

pub(crate) fn write_enrollment_status(
    output: &mut impl Write,
    status: &MachineEnrollmentStatus,
) -> io::Result<()> {
    let MachineEnrollmentStatus::Present {
        machine_id,
        metadata,
    } = status
    else {
        return writeln!(output, "This Mac is not enrolled.");
    };
    match metadata {
        EnrollmentMetadata::Empty => {
            writeln!(output, "This Mac is not enrolled.")?;
            writeln!(output, "Machine: {}", machine_id.as_str())
        }
        EnrollmentMetadata::Active {
            key, deployment, ..
        } => {
            writeln!(output, "{ENROLLED_TEXT}")?;
            writeln!(output, "Machine: {}", machine_id.as_str())?;
            writeln!(output, "Organization: {}", key.organization_id)?;
            writeln!(output, "Sign-in: {}", deployment.origin())
        }
        EnrollmentMetadata::Ended { key, why, .. } => {
            writeln!(
                output,
                "This Mac's enrollment ended: {}.",
                ending_reason(*why)
            )?;
            writeln!(output, "Machine: {}", machine_id.as_str())?;
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
