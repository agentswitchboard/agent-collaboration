//! Human-readable live link status and stored Mac enrollment.

use std::io::{self, Write};

use codex_router_host::{MessageSystemReason, MessageSystemStatus};
use switchboard_machine_enrollment::{EnrollmentEnd, EnrollmentMetadata, InvalidReason};
use switchboard_machine_enrollment_store::MachineEnrollmentStatus;

pub(crate) const ENROLLED_TEXT: &str = "This Mac is enrolled.";

pub(crate) fn write_router_link_status(
    output: &mut impl Write,
    status: Option<(MessageSystemStatus, Option<MessageSystemReason>)>,
) -> io::Result<()> {
    use MessageSystemStatus as Status;

    let Some((status, reason)) = status else {
        return writeln!(output, "Router has no message link");
    };
    let line = match status {
        Status::Starting => "starting",
        Status::NotEnrolled => "not enrolled",
        Status::EnrollmentEnded => "enrollment ended",
        Status::EnrollmentUnreadable => "could not read enrollment",
        Status::Connecting => "enrolled, connecting",
        Status::Registering => "welcomed, registering",
        Status::RegistrationFailed => match reason {
            Some(MessageSystemReason::RegistrationTimedOut) => "registration timed out",
            Some(
                MessageSystemReason::CentralReportedEnrollmentEnded
                | MessageSystemReason::AuthorityRevoked
                | MessageSystemReason::AuthorityExpired
                | MessageSystemReason::AuthorityInactive,
            ) => "registration refused",
            _ => "registration failed",
        },
        Status::RunRetired => "run retired",
        Status::Connected => "enrolled, connected",
        Status::Fault => "link fault",
        Status::Stopping => "stopping",
        Status::Restarting => "enrolled, restarting",
        Status::EnrollmentMismatch => "enrollment does not match this Router",
        Status::Stopped => "link stopped",
    };
    match reason {
        None => writeln!(output, "{line} (reason not reported)"),
        Some(MessageSystemReason::NoReason) => writeln!(output, "{line}"),
        Some(reason) => writeln!(output, "{line}: {}", link_status_reason(reason)),
    }
}

fn link_status_reason(reason: MessageSystemReason) -> &'static str {
    use MessageSystemReason as Reason;
    match reason {
        Reason::NoReason => "",
        Reason::RegistrationTimedOut => "no fitting acknowledgement arrived in time",
        Reason::RegistrationWriterUnavailable => {
            "the writer was unavailable before the frame was sent"
        }
        Reason::RegistrationWriterBusy => "the writer was busy before the frame was sent",
        Reason::RegistrationSendWindowElapsed => {
            "the write deadline elapsed before the frame was sent"
        }
        Reason::RegistrationFrameTooLarge => "the frame exceeded the link's size limit",
        Reason::RegistrationFrameInvalid => "the frame could not be encoded",
        Reason::RegistrationSinkRejected => "the socket rejected the frame",
        Reason::CentralReportedEnrollmentEnded => "central reports the enrollment ended",
        Reason::CentralFault => "central could not serve registration",
        Reason::CentralDoesNotKnowCall => "central does not recognize the registration call",
        Reason::CentralRejectedTarget => "central rejected the registration target",
        Reason::CentralDidNotPermitCall => "central did not permit registration",
        Reason::CentralRejectedSessionProof => "central did not accept the session proof",
        Reason::AuthorityRevoked => "central reports the link authority was revoked",
        Reason::AuthorityExpired => "central reports the link authority expired",
        Reason::AuthorityInactive => "central reports the link authority became inactive",
        Reason::AnotherRunTookOver => "another run of this Router took over",
        Reason::MalformedAddress => "the central address is invalid",
        Reason::FreshCredentialRefused => "central refused the refreshed credential",
        Reason::LeaseSourceFault => "the enrollment lease could not be obtained",
        Reason::EnrollmentEndingPending => "waiting for the stored ending",
        Reason::EnrollmentDenied => ending_reason(EnrollmentEnd::Denied),
        Reason::DeviceCodeExpired => ending_reason(EnrollmentEnd::Expired),
        Reason::DeviceCodeRefused => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::GrantRefused))
        }
        Reason::InvalidClient => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::InvalidClient))
        }
        Reason::InvalidRequest => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::InvalidRequest))
        }
        Reason::UnknownSignInError => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::UnknownError))
        }
        Reason::MalformedSignInAnswer => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::MalformedAnswer))
        }
        Reason::UnexpectedSignInStatus => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::UnexpectedStatus))
        }
        Reason::WrongEnrollment => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::WrongEnrollment))
        }
        Reason::OversizedSignInAnswer => {
            ending_reason(EnrollmentEnd::Invalid(InvalidReason::OversizedAnswer))
        }
        Reason::RefreshRefused => ending_reason(EnrollmentEnd::RefreshRefused),
        Reason::RefreshClientDisabled => ending_reason(EnrollmentEnd::RefreshClientDisabled),
        Reason::EnrollmentEndedByCentral => ending_reason(EnrollmentEnd::EndedByCentral),
        Reason::LocalCredentialLost => ending_reason(EnrollmentEnd::LocalCredentialLost),
        Reason::EnrollmentRevoked => ending_reason(EnrollmentEnd::Revoked),
        Reason::EnrollmentInactive => ending_reason(EnrollmentEnd::Inactive),
    }
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
