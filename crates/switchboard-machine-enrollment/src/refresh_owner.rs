use std::{future::Future, sync::Arc, time::Duration};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use router_clock::{ClockInstant, RuntimeClock};
use tokio::sync::Mutex;

use crate::{
    CentralAddresses, CredentialStore, CredentialStoreError, EnrollmentEnd, EnrollmentId,
    RefreshAnswer, RefreshCredential, SecretValue,
};

/// The sign-in adapter supplies a refresh result for the current credential.
pub trait RefreshSource: Send + Sync {
    fn refresh(&self, credential: &RefreshCredential)
    -> impl Future<Output = RefreshAnswer> + Send;
}

/// The refresh owner issues this memory-only bearer after the durable write commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessBearer {
    pub enrollment_id: EnrollmentId,
    pub central: CentralAddresses,
    pub access_token: SecretValue,
    pub expires_at: ClockInstant,
}

/// The refresh owner reports why a caller did not receive a bearer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BearerUnavailable {
    #[error("this Mac is not enrolled")]
    NotEnrolled,
    #[error("enrollment ended, enroll this Mac again")]
    Ended {
        why: EnrollmentEnd,
        cleanup: Result<(), CredentialStoreError>,
    },
    #[error("sign-in is temporarily unreachable")]
    SignInUnreachable { retry_after: Option<Duration> },
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
}

type BearerResult = Result<AccessBearer, BearerUnavailable>;
type RefreshFlight = Shared<BoxFuture<'static, BearerResult>>;
type RemovalFlight = Shared<BoxFuture<'static, Result<(), CredentialStoreError>>>;

enum RefreshState {
    Idle,
    Usable(AccessBearer),
    WaitingUntil(ClockInstant),
    Refreshing(RefreshFlight),
    Ending {
        why: EnrollmentEnd,
        removal: RemovalFlight,
    },
}

struct RefreshInputs<Store, Source> {
    store: Store,
    source: Source,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
}

/// Composition creates one owner for all consumers of the Mac's current credential.
pub struct RefreshOwner<Store, Source> {
    inputs: Arc<RefreshInputs<Store, Source>>,
    state: Mutex<RefreshState>,
}

impl<Store, Source> RefreshOwner<Store, Source>
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    pub fn new(store: Store, source: Source, clock: Arc<dyn RuntimeClock + Send + Sync>) -> Self {
        Self {
            inputs: Arc::new(RefreshInputs {
                store,
                source,
                clock,
            }),
            state: Mutex::new(RefreshState::Idle),
        }
    }

    pub async fn bearer(&self) -> Result<AccessBearer, BearerUnavailable> {
        let flight = {
            let mut state = self.state.lock().await;
            let now = self.inputs.clock.monotonic_now();
            match &*state {
                RefreshState::Usable(bearer) if now < bearer.expires_at => {
                    return Ok(bearer.clone());
                }
                RefreshState::WaitingUntil(until) if now < *until => {
                    return Err(BearerUnavailable::SignInUnreachable {
                        retry_after: Some(until.saturating_duration_since(now)),
                    });
                }
                RefreshState::Ending { why, removal } => {
                    let why = *why;
                    let removal = removal.clone();
                    drop(state);
                    return Err(BearerUnavailable::Ended {
                        why,
                        cleanup: removal.await,
                    });
                }
                RefreshState::Refreshing(flight) => flight.clone(),
                _ => {
                    let flight = self.new_refresh();
                    *state = RefreshState::Refreshing(flight.clone());
                    flight
                }
            }
        };
        let result = flight.clone().await;
        let mut state = self.state.lock().await;
        if let RefreshState::Ending { why, removal } = &*state {
            let why = *why;
            let removal = removal.clone();
            drop(state);
            return Err(BearerUnavailable::Ended {
                why,
                cleanup: removal.await,
            });
        }
        if matches!(&*state, RefreshState::Refreshing(active) if active.ptr_eq(&flight)) {
            *state = match &result {
                Ok(bearer) => RefreshState::Usable(bearer.clone()),
                Err(BearerUnavailable::Ended { why, cleanup }) => RefreshState::Ending {
                    why: *why,
                    removal: std::future::ready(*cleanup).boxed().shared(),
                },
                Err(BearerUnavailable::SignInUnreachable {
                    retry_after: Some(delay),
                }) => RefreshState::WaitingUntil(self.inputs.clock.monotonic_now().after(*delay)),
                Err(_) => RefreshState::Idle,
            };
        }
        result
    }

    pub async fn end_enrollment(&self, why: EnrollmentEnd) -> Result<(), CredentialStoreError> {
        let removal = {
            let mut state = self.state.lock().await;
            if let RefreshState::Ending { removal, .. } = &*state {
                removal.clone()
            } else {
                let in_flight = match &*state {
                    RefreshState::Refreshing(flight) => Some(flight.clone()),
                    _ => None,
                };
                let inputs = Arc::clone(&self.inputs);
                let removal = async move {
                    // A rotation already sent can still commit. Remove only after it settles.
                    if let Some(flight) = in_flight
                        && matches!(
                            flight.await,
                            Err(BearerUnavailable::Ended {
                                cleanup: Ok(()),
                                ..
                            })
                        )
                    {
                        return Ok(());
                    }
                    inputs.store.remove().await
                }
                .boxed()
                .shared();
                *state = RefreshState::Ending {
                    why,
                    removal: removal.clone(),
                };
                removal
            }
        };
        removal.await
    }

    fn new_refresh(&self) -> RefreshFlight {
        let inputs = Arc::clone(&self.inputs);
        async move {
            let credential = inputs
                .store
                .read()
                .await?
                .ok_or(BearerUnavailable::NotEnrolled)?;
            let started = inputs.clock.monotonic_now();
            match inputs.source.refresh(&credential).await {
                RefreshAnswer::Rotated(grant) => {
                    let bearer = AccessBearer {
                        enrollment_id: grant.credential.enrollment_id,
                        central: grant.credential.central,
                        access_token: grant.access_token,
                        expires_at: started.after(grant.expires_in),
                    };
                    inputs.store.replace(grant.credential).await?;
                    Ok(bearer)
                }
                RefreshAnswer::Unavailable { retry_after } => {
                    Err(BearerUnavailable::SignInUnreachable { retry_after })
                }
                RefreshAnswer::Refused { .. } => Err(BearerUnavailable::Ended {
                    why: EnrollmentEnd::RefreshRefused,
                    cleanup: inputs.store.remove().await,
                }),
            }
        }
        .boxed()
        .shared()
    }
}

#[cfg(test)]
#[path = "refresh_owner_test.rs"]
mod tests;
