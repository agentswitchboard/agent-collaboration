use state_machine_core::StateMachine;

use crate::{
    ApprovedEnrollmentSaveFailure, CredentialStore, CredentialStoreError, DeviceInstructions,
    EnrollmentContext, EnrollmentEnd, EnrollmentReceipt, EnrollmentRequest, InvalidReason,
    SignInClient, SignInFailure, StoreDurability, StoredEnrollment,
    approved_enrollment_save::install_approved_binding,
    device_state::{DeviceSignInEnding, DeviceSignInEvent, DeviceSignInMachine, DeviceSignInState},
};

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
    #[error(transparent)]
    AfterApproval(#[from] ApprovedEnrollmentSaveFailure),
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
        let stored_scope = match &before {
            StoredEnrollment::Empty => None,
            StoredEnrollment::Active(binding) => {
                Some((binding.context.organization_id, &binding.context.deployment))
            }
            StoredEnrollment::Ended {
                key, deployment, ..
            } => Some((key.organization_id, deployment)),
        };
        if stored_scope.is_some_and(|(organization, deployment)| {
            !context.matches_scope(organization, deployment)
        }) {
            return Err(CredentialStoreError::EnrollmentScopeMismatch.into());
        }
        if matches!(&before, StoredEnrollment::Active(binding) if binding.context.machine_id != context.machine_id)
        {
            return Err(EnrollmentFailure::ReplacementConflict);
        }
        if before.generation().binding_successor().is_none() {
            return Err(EnrollmentFailure::GenerationExhausted);
        }

        let client_id = self.discover().await?.client_id;
        let dispatched = self.clock.monotonic_now();
        let authorization = self.start(client_id, request).await?;
        let arrived = self.clock.monotonic_now();
        show_instructions(&authorization.instructions)?;
        let (mut state, _) = DeviceSignInMachine::transition(
            &DeviceSignInState::Starting,
            DeviceSignInEvent::Started {
                dispatched,
                arrived,
                interval: authorization.interval,
                expires_in: authorization.expires_in,
            },
        )
        .map_err(|_| transition_fault())?;

        loop {
            state = match state {
                DeviceSignInState::Waiting(window) => {
                    let now = self.clock.monotonic_now();
                    let (next, _) =
                        DeviceSignInMachine::transition(&state, DeviceSignInEvent::Woke(now))
                            .map_err(|_| transition_fault())?;
                    if matches!(next, DeviceSignInState::Waiting(_)) {
                        self.clock.sleep_until(window.wake_at()).await;
                    }
                    next
                }
                DeviceSignInState::Polling(window) => {
                    let reply = router_clock::timeout_at(
                        self.clock.as_ref(),
                        window.reply_deadline(),
                        async {
                            if !window.dispatch_allowed(self.clock.monotonic_now()) {
                                return None;
                            }
                            Some(self.poll(client_id, &authorization.device_code).await)
                        },
                    )
                    .await
                    .ok()
                    .flatten();
                    let (mut next, effects) = DeviceSignInMachine::transition(
                        &state,
                        DeviceSignInEvent::PollSettled {
                            now: self.clock.monotonic_now(),
                            reply: reply.as_ref(),
                        },
                    )
                    .map_err(|_| transition_fault())?;
                    for grant in effects {
                        let result = install_approved_binding(
                            store,
                            &before,
                            &context,
                            grant.credential.clone(),
                        )
                        .await;
                        next = DeviceSignInMachine::transition(
                            &next,
                            DeviceSignInEvent::SaveSettled(result),
                        )
                        .map_err(|_| transition_fault())?
                        .0;
                    }
                    next
                }
                DeviceSignInState::Enrolled(receipt) => return Ok(receipt),
                DeviceSignInState::Ended(ending) => {
                    return Err(match ending {
                        DeviceSignInEnding::SignIn(why) => EnrollmentFailure::Ended(why),
                        DeviceSignInEnding::AfterApproval(error) => {
                            EnrollmentFailure::AfterApproval(error)
                        }
                    });
                }
                // Start settles above and an approved grant's save effect settles
                // before the drive loop observes its next state.
                DeviceSignInState::Starting | DeviceSignInState::Saving => {
                    return Err(transition_fault());
                }
            };
        }
    }
}

fn transition_fault() -> EnrollmentFailure {
    EnrollmentFailure::Ended(EnrollmentEnd::Invalid(InvalidReason::UnexpectedStatus))
}
