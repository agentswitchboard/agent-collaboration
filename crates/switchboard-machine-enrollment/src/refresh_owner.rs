use std::{future::Future, sync::Arc, time::Duration};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use tokio::sync::Mutex;

use crate::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, EnrollmentKey, InvalidReason, RefreshAnswer,
    RefreshCredential, SecretValue, StoredEnrollment, TokenGrant,
};

const WRITE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const WRITE_RETRY_LIMIT: Duration = Duration::from_secs(5);

/// The sign-in adapter supplies one refresh result for the current credential.
pub trait RefreshSource: Send + Sync {
    fn refresh(&self, credential: &RefreshCredential)
    -> impl Future<Output = RefreshAnswer> + Send;
}

/// The refresh owner issues this memory-only lease after the new binding commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessLease {
    pub token: SecretValue,
    pub usable_until: ClockInstant,
    pub binding_generation: BindingGeneration,
}

/// A link caller requests any usable lease or one newer than a rejected lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessRequest {
    Usable,
    NewerThan(BindingGeneration),
}

impl AccessRequest {
    fn accepts(self, generation: BindingGeneration) -> bool {
        match self {
            Self::Usable => true,
            Self::NewerThan(rejected) => generation > rejected,
        }
    }
}

/// The refresh owner reports why a caller did not receive a lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseUnavailable {
    #[error("this Mac is not enrolled")]
    NotEnrolled,
    #[error("enrollment ended, enroll this Mac again")]
    Ended { why: EnrollmentEnd },
    #[error("sign-in is temporarily unreachable")]
    SignInUnreachable { retry_after: Option<Duration> },
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
    #[error(transparent)]
    Configuration(#[from] LeaseConfigurationFault),
}

/// The owner rejects incompatible binding and sign-in configuration as a distinct fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseConfigurationFault {
    #[error(
        "the saved enrollment belongs to a different machine, organization or sign-in deployment"
    )]
    BindingContext,
    #[error("the saved enrollment generation cannot be advanced")]
    GenerationExhausted,
    #[error("the requested generation is ahead of the saved enrollment")]
    FutureGeneration,
    #[error("sign-in returned an invalid refresh answer")]
    InvalidAnswer(InvalidReason),
}

type LeaseResult = Result<AccessLease, LeaseUnavailable>;
type RefreshFlight = Shared<BoxFuture<'static, RefreshCompletion>>;

#[derive(Clone)]
enum RefreshCompletion {
    Resolved(LeaseResult),
    Superseded,
    Backoff {
        generation: BindingGeneration,
        until: ClockInstant,
    },
    FailClosed(EnrollmentEnd),
    RefusedConfiguration {
        generation: BindingGeneration,
        fault: LeaseConfigurationFault,
    },
}

#[derive(Default)]
struct RefreshState {
    flight: Option<RefreshFlight>,
    lease: Option<AccessLease>,
    backoff: Option<(BindingGeneration, ClockInstant)>,
    fail_closed: Option<EnrollmentEnd>,
    fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
}

struct RefreshInputs<Store, Source> {
    store: Store,
    source: Source,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    context: EnrollmentContext,
}

/// Composition makes one refresh owner per configured enrollment scope.
pub struct RefreshOwner<Store, Source> {
    inputs: Arc<RefreshInputs<Store, Source>>,
    state: Mutex<RefreshState>,
}

impl<Store, Source> RefreshOwner<Store, Source>
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    pub fn new(
        store: Store,
        source: Source,
        clock: Arc<dyn RuntimeClock + Send + Sync>,
        context: EnrollmentContext,
    ) -> Self {
        Self {
            inputs: Arc::new(RefreshInputs {
                store,
                source,
                clock,
                context,
            }),
            state: Mutex::new(RefreshState::default()),
        }
    }

    pub async fn lease(&self, request: AccessRequest) -> LeaseResult {
        loop {
            let flight = {
                let mut state = self.state.lock().await;
                if let Some(why) = state.fail_closed {
                    return Err(LeaseUnavailable::Ended { why });
                }
                if let Some(flight) = &state.flight {
                    flight.clone()
                } else {
                    let flight =
                        self.new_refresh(request, state.lease.clone(), state.backoff, state.fault);
                    state.flight = Some(flight.clone());
                    flight
                }
            };

            let result = flight.clone().await;
            let mut state = self.state.lock().await;
            if let Some(why) = state.fail_closed {
                return Err(LeaseUnavailable::Ended { why });
            }
            let owns_completion = state
                .flight
                .as_ref()
                .is_some_and(|active| active.ptr_eq(&flight));
            if owns_completion {
                state.flight = None;
                match &result {
                    RefreshCompletion::Resolved(Ok(lease)) => state.lease = Some(lease.clone()),
                    RefreshCompletion::Backoff { generation, until } => {
                        state.backoff = Some((*generation, *until))
                    }
                    RefreshCompletion::FailClosed(why) => {
                        state.lease = None;
                        state.fail_closed = Some(*why);
                    }
                    RefreshCompletion::RefusedConfiguration { generation, fault } => {
                        state.fault = Some((*generation, *fault));
                    }
                    _ => {}
                }
            }
            match result {
                RefreshCompletion::Resolved(Ok(lease))
                    if !request.accepts(lease.binding_generation) => {}
                RefreshCompletion::Superseded => {}
                RefreshCompletion::Resolved(result) => return result,
                RefreshCompletion::Backoff { until, .. } => {
                    return Err(LeaseUnavailable::SignInUnreachable {
                        retry_after: Some(
                            until.saturating_duration_since(self.inputs.clock.monotonic_now()),
                        ),
                    });
                }
                RefreshCompletion::FailClosed(why) => return Err(LeaseUnavailable::Ended { why }),
                RefreshCompletion::RefusedConfiguration { fault, .. } => return Err(fault.into()),
            }
        }
    }

    pub async fn end_enrollment(
        &self,
        key: &EnrollmentKey,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        loop {
            let StoredEnrollment::Active(binding) = self.inputs.store.read().await? else {
                return Ok(ConditionalWrite::Superseded);
            };
            if &binding.key() != key {
                return Ok(ConditionalWrite::Superseded);
            }
            match self.inputs.end_binding(binding.generation, why).await {
                Ok(ConditionalWrite::Applied) => return Ok(ConditionalWrite::Applied),
                Ok(ConditionalWrite::Superseded) => {}
                Err(error) => {
                    let mut state = self.state.lock().await;
                    state.lease = None;
                    state.fail_closed = Some(why);
                    return Err(error);
                }
            }
        }
    }

    fn new_refresh(
        &self,
        request: AccessRequest,
        cached: Option<AccessLease>,
        backoff: Option<(BindingGeneration, ClockInstant)>,
        prior_fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
    ) -> RefreshFlight {
        let inputs = Arc::clone(&self.inputs);
        async move {
            match inputs
                .refresh_if_needed(request, cached, backoff, prior_fault)
                .await
            {
                Ok(completion) => completion,
                Err(error) => RefreshCompletion::Resolved(Err(error)),
            }
        }
        .boxed()
        .shared()
    }
}

impl<Store: CredentialStore, Source: RefreshSource> RefreshInputs<Store, Source> {
    async fn read_binding(&self) -> Result<EnrollmentBinding, LeaseUnavailable> {
        match self.store.read().await? {
            StoredEnrollment::Empty => Err(LeaseUnavailable::NotEnrolled),
            StoredEnrollment::Ended { why, .. } => Err(LeaseUnavailable::Ended { why }),
            StoredEnrollment::Active(binding) if binding.context == self.context => Ok(binding),
            StoredEnrollment::Active(_) => Err(LeaseConfigurationFault::BindingContext.into()),
        }
    }

    async fn refresh_if_needed(
        &self,
        request: AccessRequest,
        cached: Option<AccessLease>,
        backoff: Option<(BindingGeneration, ClockInstant)>,
        prior_fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
    ) -> Result<RefreshCompletion, LeaseUnavailable> {
        let binding = self.read_binding().await?;
        let generation = binding.generation;
        let now = self.clock.monotonic_now();
        if let Some(lease) = cached
            && lease.binding_generation == generation
            && request.accepts(generation)
            && now < lease.usable_until
        {
            return Ok(RefreshCompletion::Resolved(Ok(lease)));
        }
        if let AccessRequest::NewerThan(rejected) = request
            && rejected > generation
        {
            return Err(LeaseConfigurationFault::FutureGeneration.into());
        }
        if let Some((refused_generation, fault)) = prior_fault
            && refused_generation == generation
        {
            return Err(fault.into());
        }
        if let Some((waiting_generation, until)) = backoff
            && waiting_generation == generation
            && now < until
        {
            return Ok(RefreshCompletion::Backoff { generation, until });
        }
        let next = generation
            .successor()
            .ok_or(LeaseConfigurationFault::GenerationExhausted)?;
        match self.source.refresh(&binding.credential).await {
            RefreshAnswer::Rotated(grant) => {
                let usable_until = now.after(grant.expires_in);
                Ok(self
                    .commit_rotation(binding, next, grant, usable_until)
                    .await)
            }
            RefreshAnswer::Unavailable { retry_after } => {
                match self.read_binding().await {
                    Ok(current) if current.generation == generation => {}
                    Ok(_) => return Ok(RefreshCompletion::Superseded),
                    Err(error) => return Err(error),
                }
                Ok(match retry_after {
                    Some(wait) => RefreshCompletion::Backoff {
                        generation,
                        until: self.clock.monotonic_now().after(wait),
                    },
                    None => RefreshCompletion::Resolved(Err(LeaseUnavailable::SignInUnreachable {
                        retry_after: None,
                    })),
                })
            }
            RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused,
            } => Ok(self
                .finish_binding(generation, EnrollmentEnd::RefreshRefused)
                .await),
            RefreshAnswer::Refused { reason } => Ok(RefreshCompletion::RefusedConfiguration {
                generation,
                fault: LeaseConfigurationFault::InvalidAnswer(reason),
            }),
        }
    }

    async fn commit_rotation(
        &self,
        previous: EnrollmentBinding,
        generation: BindingGeneration,
        grant: TokenGrant,
        usable_until: ClockInstant,
    ) -> RefreshCompletion {
        let replacement = EnrollmentBinding {
            generation,
            context: previous.context,
            credential: grant.credential,
        };
        let deadline = self
            .clock
            .monotonic_now()
            .after(WRITE_RETRY_LIMIT)
            .min(usable_until);
        loop {
            let now = self.clock.monotonic_now();
            if now >= deadline {
                return self
                    .finish_binding(previous.generation, EnrollmentEnd::LocalCredentialLost)
                    .await;
            }
            // Retain the same pair across write failures. The old token is spent.
            let change = timeout_at(
                self.clock.as_ref(),
                deadline,
                self.store.replace(previous.generation, &replacement),
            )
            .await;
            match change {
                Ok(Ok(ConditionalWrite::Applied)) => {
                    if self.clock.monotonic_now() < usable_until {
                        return RefreshCompletion::Resolved(Ok(AccessLease {
                            token: grant.access_token,
                            usable_until,
                            binding_generation: generation,
                        }));
                    }
                    return RefreshCompletion::Superseded;
                }
                Ok(Ok(ConditionalWrite::Superseded)) => return RefreshCompletion::Superseded,
                _ => {
                    let next_attempt = self
                        .clock
                        .monotonic_now()
                        .after(WRITE_RETRY_INTERVAL)
                        .min(deadline);
                    self.clock.sleep_until(next_attempt).await;
                }
            }
        }
    }

    async fn finish_binding(
        &self,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    ) -> RefreshCompletion {
        match self.end_binding(generation, why).await {
            Ok(ConditionalWrite::Applied) => {
                RefreshCompletion::Resolved(Err(LeaseUnavailable::Ended { why }))
            }
            Ok(ConditionalWrite::Superseded) => RefreshCompletion::Superseded,
            Err(_) => RefreshCompletion::FailClosed(why),
        }
    }

    async fn end_binding(
        &self,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        match self.store.remove(generation, why).await {
            Ok(ending) => Ok(ending),
            Err(_) => self.store.mark_unusable(generation, why).await,
        }
    }
}

#[cfg(test)]
#[path = "refresh_owner_test.rs"]
mod tests;
