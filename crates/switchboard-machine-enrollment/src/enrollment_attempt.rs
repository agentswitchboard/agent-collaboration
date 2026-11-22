use std::time::Duration;

use crate::{
    ConditionalWrite, CredentialStore, CredentialStoreError, DeviceEnrollment,
    DeviceEnrollmentInput, DeviceInstructions, DevicePollSchedule, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, EnrollmentReceipt, EnrollmentRequest, InvalidReason,
    PollAnswer, RefreshCredential, SignInClient, SignInFailure, StoreDurability, StoredEnrollment,
};

const FINAL_POLL_GRACE: Duration = Duration::from_secs(2);

/// The client reports why one human-initiated device login did not install a durable binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentFailure {
    #[error("this Mac has no durable enrollment store")]
    NoDurableStore,
    #[error("another enrollment replaced the preceding binding")]
    ReplacementConflict,
    #[error("the enrollment generation cannot be advanced")]
    GenerationExhausted,
    #[error(transparent)]
    SignIn(#[from] SignInFailure),
    #[error("the enrollment instructions could not be shown")]
    Instructions,
    #[error("the enrollment attempt ended")]
    Ended(EnrollmentEnd),
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
}

impl SignInClient {
    pub async fn enroll(
        &self,
        request: EnrollmentRequest<'_>,
        store: &impl CredentialStore,
        show_instructions: impl FnOnce(&DeviceInstructions) -> Result<(), EnrollmentFailure>,
    ) -> Result<EnrollmentReceipt, EnrollmentFailure> {
        if store.durability() == StoreDurability::MemoryOnly {
            return Err(EnrollmentFailure::NoDurableStore);
        }
        let before = store.read().await?;
        let context = EnrollmentContext {
            organization_id: request.organization_id,
            deployment: self.deployment().clone(),
            machine_id: request.machine_id.clone(),
        };
        if matches!(&before, StoredEnrollment::Active(binding) if binding.context != context) {
            return Err(EnrollmentFailure::ReplacementConflict);
        }

        let client_id = self.discover().await?.client_id;
        let authorization = self.start(client_id, request).await?;
        let started = self.clock.monotonic_now();
        show_instructions(&authorization.instructions)?;
        let mut state = DeviceEnrollment::NotEnrolled
            .step(started, DeviceEnrollmentInput::Started(authorization));

        loop {
            let now = self.clock.monotonic_now();
            match state.poll_schedule(now) {
                DevicePollSchedule::Inactive => {
                    return match state {
                        DeviceEnrollment::Enrolled(receipt) => Ok(receipt),
                        DeviceEnrollment::Ended { why } => Err(EnrollmentFailure::Ended(why)),
                        _ => Err(invalid_attempt()),
                    };
                }
                DevicePollSchedule::DeadlineReached => {
                    state = state.step(now, DeviceEnrollmentInput::ClockReached);
                }
                DevicePollSchedule::WaitUntil(at) => self.clock.sleep_until(at).await,
                DevicePollSchedule::Due => {
                    let DeviceEnrollment::Asking {
                        authorization,
                        deadline,
                        ..
                    } = &state
                    else {
                        return Err(invalid_attempt());
                    };
                    let deadline = *deadline;
                    let response = router_clock::timeout_at(
                        self.clock.as_ref(),
                        deadline.after(FINAL_POLL_GRACE),
                        async {
                            if self.clock.monotonic_now() >= deadline {
                                return None;
                            }
                            Some(self.poll(client_id, &authorization.device_code).await)
                        },
                    )
                    .await
                    .ok()
                    .flatten();
                    let now = self.clock.monotonic_now();
                    let input = match response {
                        Some(PollAnswer::Enrolled(grant))
                            if now <= deadline.after(FINAL_POLL_GRACE) =>
                        {
                            let receipt = install_approved_binding(
                                store,
                                &before,
                                &context,
                                grant.credential,
                            )
                            .await?;
                            DeviceEnrollmentInput::Committed(receipt)
                        }
                        _ if now >= deadline => DeviceEnrollmentInput::ClockReached,
                        Some(PollAnswer::Pending) => DeviceEnrollmentInput::Pending,
                        Some(PollAnswer::SlowDown) => DeviceEnrollmentInput::SlowDown,
                        Some(PollAnswer::Denied) => DeviceEnrollmentInput::Denied,
                        Some(PollAnswer::Expired) | None => DeviceEnrollmentInput::Expired,
                        Some(PollAnswer::Invalid { reason }) => {
                            DeviceEnrollmentInput::Invalid { reason }
                        }
                        Some(PollAnswer::Unavailable { retry_after }) => {
                            DeviceEnrollmentInput::Unavailable { retry_after }
                        }
                        Some(PollAnswer::Enrolled(_)) => return Err(invalid_attempt()),
                    };
                    state = state.step(now, input);
                }
            }
        }
    }
}

fn invalid_attempt() -> EnrollmentFailure {
    EnrollmentFailure::Ended(EnrollmentEnd::Invalid(InvalidReason::UnexpectedStatus))
}

async fn install_approved_binding(
    store: &impl CredentialStore,
    before: &StoredEnrollment,
    context: &EnrollmentContext,
    credential: RefreshCredential,
) -> Result<EnrollmentReceipt, EnrollmentFailure> {
    let old_identity = before.key();
    let mut expected = before.generation();
    let mut binding = EnrollmentBinding {
        generation: expected
            .successor()
            .ok_or(EnrollmentFailure::GenerationExhausted)?,
        context: context.clone(),
        credential,
    };

    loop {
        match store.replace(expected, &binding).await? {
            ConditionalWrite::Applied => {
                return Ok(EnrollmentReceipt {
                    generation: binding.generation,
                    enrollment_id: binding.credential.enrollment_id,
                    central: binding.credential.central,
                });
            }
            ConditionalWrite::Superseded => {
                let current = store.read().await?;
                if current.key() != old_identity || current.generation() <= expected {
                    return Err(EnrollmentFailure::ReplacementConflict);
                }
                expected = current.generation();
                binding.generation = expected
                    .successor()
                    .ok_or(EnrollmentFailure::GenerationExhausted)?;
            }
        }
    }
}
