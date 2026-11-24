//! The interactive enrollment command. Its process dispatch needs a machine identity and store.
use std::{ffi::OsString, io::Write, sync::Arc};

use clap::Parser;
use router_clock::RuntimeClock;
use switchboard_machine_enrollment::{
    CredentialStore, CredentialStoreError, DeviceInstructions, EnrollmentEnd, EnrollmentFailure,
    EnrollmentRequest, InvalidReason, MachineId, OrganizationId, SignInClient, SignInClientError,
    SignInDeployment, SignInFailure,
};

/// The enroll command parses these values from the person's arguments.
pub struct EnrollArguments {
    pub organization_id: OrganizationId,
    pub sign_in: SignInDeployment,
}

#[derive(Parser)]
#[command(
    name = "enroll",
    about = "Enroll this Mac in a Switchboard organization"
)]
struct EnrollOptions {
    #[arg(long)]
    organization: String,
    #[arg(long)]
    sign_in: String,
}

/// The composition root supplies the Mac identity, store and clock to the enroll command.
pub struct EnrollBindings<'input, Store> {
    pub machine_id: &'input MachineId,
    pub machine_label: &'input str,
    pub store: &'input Store,
    pub clock: Arc<dyn RuntimeClock + Send + Sync>,
}

/// The command emits these action-oriented messages without carrying raw arguments or replies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollCommandError {
    #[error("Use enroll --organization <organization-uuid> --sign-in <https-origin>.")]
    Arguments,
    #[error("Copy the organization id from sign-in and pass it to --organization.")]
    Organization,
    #[error("Pass the HTTPS origin of sign-in to --sign-in.")]
    SignInOrigin,
    #[error("The sign-in client could not start. Check the sign-in origin and try again.")]
    Client,
    #[error("The enrollment output could not be written.")]
    Output,
    #[error("{}", describe_ending(*.0))]
    Enrollment(EnrollmentFailure),
}

impl EnrollArguments {
    pub fn parse(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, EnrollCommandError> {
        let options = EnrollOptions::try_parse_from(
            std::iter::once(OsString::from("enroll")).chain(arguments),
        )
        .map_err(|_| EnrollCommandError::Arguments)?;
        let organization_id = options
            .organization
            .parse()
            .map_err(|_| EnrollCommandError::Organization)?;
        let origin =
            reqwest::Url::parse(&options.sign_in).map_err(|_| EnrollCommandError::SignInOrigin)?;
        let sign_in =
            SignInDeployment::new(origin).map_err(|_| EnrollCommandError::SignInOrigin)?;
        Ok(Self {
            organization_id,
            sign_in,
        })
    }
}

pub async fn run_enroll(
    arguments: EnrollArguments,
    bindings: EnrollBindings<'_, impl CredentialStore>,
    output: &mut impl Write,
) -> Result<(), EnrollCommandError> {
    let client =
        SignInClient::new(arguments.sign_in.origin().clone(), bindings.clock).map_err(|error| {
            match error {
                SignInClientError::InvalidIssuer => EnrollCommandError::SignInOrigin,
                SignInClientError::ClientSetup => EnrollCommandError::Client,
            }
        })?;
    let request = EnrollmentRequest {
        machine_id: bindings.machine_id,
        machine_label: bindings.machine_label,
        organization_id: arguments.organization_id,
    };
    client
        .enroll(request, bindings.store, |instructions| {
            show_device_instructions(output, instructions)
                .map_err(|_| EnrollmentFailure::Instructions)
        })
        .await
        .map_err(EnrollCommandError::Enrollment)?;
    writeln!(output, "This Mac is enrolled.").map_err(|_| EnrollCommandError::Output)
}

fn show_device_instructions(
    output: &mut impl Write,
    instructions: &DeviceInstructions,
) -> std::io::Result<()> {
    writeln!(output, "Open {}", instructions.verification_link)?;
    writeln!(output, "Code {}", instructions.user_code)?;
    output.flush()
}

fn describe_ending(failure: EnrollmentFailure) -> &'static str {
    match failure {
        EnrollmentFailure::NoDurableStore => {
            "This Mac has no durable credential store. Restore Keychain access before enrolling."
        }
        EnrollmentFailure::ReplacementConflict => {
            "Another enrollment changed this Mac's binding. Check the current enrollment before trying again."
        }
        EnrollmentFailure::GenerationExhausted => {
            "The enrollment store cannot advance. Contact the Router administrator."
        }
        EnrollmentFailure::Instructions => {
            "The sign-in instructions could not be shown. Restore terminal output and try again."
        }
        EnrollmentFailure::Store(CredentialStoreError::EnrollmentScopeMismatch) => {
            "Remove this machine first, then enroll it in the new organization or sign-in deployment."
        }
        EnrollmentFailure::Store(
            CredentialStoreError::Read
            | CredentialStoreError::Replace
            | CredentialStoreError::Remove
            | CredentialStoreError::CleanupTimedOut
            | CredentialStoreError::OutcomeNotKnown,
        ) => "The enrollment store is unavailable. Check Keychain access and try again.",
        EnrollmentFailure::SignIn(SignInFailure::Unavailable { .. }) => {
            "Sign-in is temporarily unavailable. Try enrolling again later."
        }
        EnrollmentFailure::SignIn(SignInFailure::Invalid { reason })
        | EnrollmentFailure::Ended(EnrollmentEnd::Invalid(reason)) => invalid_answer(reason),
        EnrollmentFailure::Ended(EnrollmentEnd::Denied) => {
            "The enrollment was denied in sign-in. Ask an approver before trying again."
        }
        EnrollmentFailure::Ended(EnrollmentEnd::Expired) => {
            "The device code expired. Run enroll again for a fresh code."
        }
        EnrollmentFailure::Ended(EnrollmentEnd::LocalCredentialLost) => {
            "The local credential was lost. Run enroll again."
        }
        EnrollmentFailure::Ended(EnrollmentEnd::RefreshClientDisabled) => {
            "Sign-in disabled this enrollment. Run enroll again to enroll this Mac."
        }
        EnrollmentFailure::Ended(
            EnrollmentEnd::RefreshRefused
            | EnrollmentEnd::Revoked
            | EnrollmentEnd::Inactive
            | EnrollmentEnd::EndedByCentral,
        ) => "Enrollment ended. Run enroll again to enroll this Mac.",
    }
}

fn invalid_answer(reason: InvalidReason) -> &'static str {
    match reason {
        InvalidReason::GrantRefused => {
            "Sign-in refused this device code. Run enroll again for a fresh code."
        }
        InvalidReason::InvalidClient | InvalidReason::InvalidRequest => {
            "Sign-in rejected the Router's request. Check the sign-in origin and report the error if it recurs."
        }
        InvalidReason::UnknownError
        | InvalidReason::MalformedAnswer
        | InvalidReason::UnexpectedStatus
        | InvalidReason::WrongEnrollment
        | InvalidReason::OversizedAnswer => {
            "Sign-in sent an unrecognized answer. Check the sign-in deployment before trying again."
        }
    }
}

#[cfg(test)]
#[path = "enrollment_command_test.rs"]
mod tests;
