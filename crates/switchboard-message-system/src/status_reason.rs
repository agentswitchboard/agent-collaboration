//! The closed reason vocabulary carried by the local operator snapshot.

use central_link_protocol::{LinkCallRefusal, MachineLinkCloseCode};
use serde::{Deserialize, Serialize};
use switchboard_central_link::link_policy::{LinkFault, RegistrationFailure, UntouchedCommand};
use switchboard_machine_enrollment::{EnrollmentEnd, InvalidReason};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageSystemReason {
    NoReason,
    RegistrationTimedOut,
    RegistrationWriterUnavailable,
    RegistrationWriterBusy,
    RegistrationSendWindowElapsed,
    RegistrationFrameTooLarge,
    RegistrationFrameInvalid,
    RegistrationSinkRejected,
    CentralReportedEnrollmentEnded,
    CentralFault,
    CentralDoesNotKnowCall,
    CentralRejectedTarget,
    CentralDidNotPermitCall,
    CentralRejectedSessionProof,
    AuthorityRevoked,
    AuthorityExpired,
    AuthorityInactive,
    AnotherRunTookOver,
    MalformedAddress,
    FreshCredentialRefused,
    LeaseSourceFault,
    EnrollmentEndingPending,
    EnrollmentDenied,
    DeviceCodeExpired,
    DeviceCodeRefused,
    InvalidClient,
    InvalidRequest,
    UnknownSignInError,
    MalformedSignInAnswer,
    UnexpectedSignInStatus,
    WrongEnrollment,
    OversizedSignInAnswer,
    RefreshRefused,
    RefreshClientDisabled,
    EnrollmentEndedByCentral,
    LocalCredentialLost,
    EnrollmentRevoked,
    EnrollmentInactive,
}

impl MessageSystemReason {
    pub(super) const fn registration(failure: RegistrationFailure) -> Self {
        match failure {
            RegistrationFailure::TimedOut => Self::RegistrationTimedOut,
            RegistrationFailure::CentralEnrollmentEnded => Self::CentralReportedEnrollmentEnded,
            RegistrationFailure::NotWritten(cause) => match cause {
                UntouchedCommand::ConnectionUnavailable => Self::RegistrationWriterUnavailable,
                UntouchedCommand::WriterBusy => Self::RegistrationWriterBusy,
                UntouchedCommand::SendWindowElapsed => Self::RegistrationSendWindowElapsed,
                UntouchedCommand::FrameTooLarge => Self::RegistrationFrameTooLarge,
                UntouchedCommand::InvalidFrame => Self::RegistrationFrameInvalid,
                UntouchedCommand::SinkRejected => Self::RegistrationSinkRejected,
            },
            RegistrationFailure::Refused(reason) => match reason {
                LinkCallRefusal::CentralFault => Self::CentralFault,
                LinkCallRefusal::UnknownCall => Self::CentralDoesNotKnowCall,
                LinkCallRefusal::WrongTarget => Self::CentralRejectedTarget,
                LinkCallRefusal::NotPermitted => Self::CentralDidNotPermitCall,
                LinkCallRefusal::SessionProofNotAccepted => Self::CentralRejectedSessionProof,
                LinkCallRefusal::AuthorityLapsed { cause } => match cause {
                    MachineLinkCloseCode::Revoked => Self::AuthorityRevoked,
                    MachineLinkCloseCode::Expired => Self::AuthorityExpired,
                    MachineLinkCloseCode::Inactive => Self::AuthorityInactive,
                },
            },
        }
    }

    pub(super) const fn link_fault(fault: LinkFault) -> Self {
        match fault {
            LinkFault::MalformedAddress => Self::MalformedAddress,
            LinkFault::FreshCredentialRefused => Self::FreshCredentialRefused,
            LinkFault::LeaseSource => Self::LeaseSourceFault,
        }
    }

    pub(super) const fn stored_ending(why: EnrollmentEnd) -> Self {
        match why {
            EnrollmentEnd::Denied => Self::EnrollmentDenied,
            EnrollmentEnd::Expired => Self::DeviceCodeExpired,
            EnrollmentEnd::Invalid(reason) => match reason {
                InvalidReason::GrantRefused => Self::DeviceCodeRefused,
                InvalidReason::InvalidClient => Self::InvalidClient,
                InvalidReason::InvalidRequest => Self::InvalidRequest,
                InvalidReason::UnknownError => Self::UnknownSignInError,
                InvalidReason::MalformedAnswer => Self::MalformedSignInAnswer,
                InvalidReason::UnexpectedStatus => Self::UnexpectedSignInStatus,
                InvalidReason::WrongEnrollment => Self::WrongEnrollment,
                InvalidReason::OversizedAnswer => Self::OversizedSignInAnswer,
            },
            EnrollmentEnd::RefreshRefused => Self::RefreshRefused,
            EnrollmentEnd::RefreshClientDisabled => Self::RefreshClientDisabled,
            EnrollmentEnd::EndedByCentral => Self::EnrollmentEndedByCentral,
            EnrollmentEnd::LocalCredentialLost => Self::LocalCredentialLost,
            EnrollmentEnd::Revoked => Self::EnrollmentRevoked,
            EnrollmentEnd::Inactive => Self::EnrollmentInactive,
        }
    }
}
