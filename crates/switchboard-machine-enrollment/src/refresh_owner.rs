use std::{
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

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
// Reserve time for the fallback marker even if physical removal stalls.
const ENDING_CLEANUP_LIMIT: Duration = Duration::from_secs(2);

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
    #[error("sign-in returned access grants that expired before delivery")]
    StaleAccessGrant,
    #[error("sign-in's refresh response exceeded the size limit")]
    OversizedRefreshReply,
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
type RefreshFlight = Shared<BoxFuture<'static, (BindingGeneration, RefreshCompletion)>>;

#[derive(Clone)]
enum RefreshCompletion {
    Committed(AccessLease),
    Unavailable(Option<ClockInstant>),
    Invalid(LeaseConfigurationFault),
    Ended {
        why: EnrollmentEnd,
        deadline: ClockInstant,
    },
    OversizedReply,
    Recheck,
}

#[derive(Default)]
struct RefreshState {
    flight: Option<RefreshFlight>,
    lease: Option<AccessLease>,
    backoff: Option<(BindingGeneration, ClockInstant)>,
    ending: Option<(EnrollmentKey, EnrollmentEnd)>,
    fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
}

impl RefreshState {
    fn admit(
        &mut self,
        binding: &EnrollmentBinding,
        request: AccessRequest,
    ) -> Result<(), LeaseUnavailable> {
        if let Some((key, why)) = &self.ending {
            if *key == binding.key() {
                return Err(LeaseUnavailable::Ended { why: *why });
            }
            self.ending = None;
        }
        if let AccessRequest::NewerThan(rejected) = request
            && rejected > binding.generation
        {
            return Err(LeaseConfigurationFault::FutureGeneration.into());
        }
        Ok(())
    }

    fn fence(&mut self, key: EnrollmentKey, why: EnrollmentEnd) {
        self.lease = None;
        self.ending = Some((key, why));
    }
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
    state: Arc<Mutex<RefreshState>>,
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
            state: Arc::new(Mutex::new(RefreshState::default())),
        }
    }

    pub async fn lease(&self, request: AccessRequest) -> LeaseResult {
        let mut followed_stale_access = false;
        loop {
            let flight = {
                let mut state = self.state.lock().await;
                let binding = self.inputs.read_binding().await?;
                state.admit(&binding, request)?;
                let now = self.inputs.clock.monotonic_now();
                let generation = binding.generation;
                if let Some((faulted, fault)) = state.fault
                    && faulted == generation
                {
                    return Err(fault.into());
                }
                if let Some(lease) = &state.lease
                    && lease.binding_generation == generation
                    && request.accepts(generation)
                    && now < lease.usable_until
                {
                    return Ok(lease.clone());
                }
                if let Some((waiting, until)) = state.backoff
                    && waiting == generation
                    && now < until
                {
                    return Err(unavailable_until(Some(until), now));
                }
                if let Some(flight) = &state.flight {
                    // The shared result records whether it is usable on its own generation.
                    flight.clone()
                } else {
                    let next = generation
                        .successor()
                        .ok_or(LeaseConfigurationFault::GenerationExhausted)?;
                    let flight = self.new_refresh(binding, next);
                    state.flight = Some(flight.clone());
                    flight
                }
            };

            let (origin, result) = flight.clone().await;
            let mut state = self.state.lock().await;
            if state
                .flight
                .as_ref()
                .is_some_and(|active| active.ptr_eq(&flight))
            {
                state.flight = None;
            }
            let binding = match &result {
                RefreshCompletion::Ended { why, deadline } => match timeout_at(
                    self.inputs.clock.as_ref(),
                    *deadline,
                    self.inputs.read_binding(),
                )
                .await
                {
                    Ok(Ok(binding)) => binding,
                    Ok(Err(error)) if !matches!(error, LeaseUnavailable::Store(_)) => {
                        return Err(error);
                    }
                    _ => return Err(LeaseUnavailable::Ended { why: *why }),
                },
                _ => self.inputs.read_binding().await?,
            };
            state.admit(&binding, request)?;
            let now = self.inputs.clock.monotonic_now();
            match result {
                RefreshCompletion::Committed(lease)
                    if binding.generation == lease.binding_generation =>
                {
                    if now < lease.usable_until {
                        state.lease = Some(lease.clone());
                        if request.accepts(lease.binding_generation) {
                            return Ok(lease);
                        }
                    } else if followed_stale_access {
                        return Err(LeaseUnavailable::StaleAccessGrant);
                    } else {
                        followed_stale_access = true;
                    }
                }
                RefreshCompletion::Invalid(fault) if binding.generation == origin => {
                    state.fault = Some((origin, fault));
                    return Err(fault.into());
                }
                RefreshCompletion::OversizedReply if binding.generation == origin => {
                    return Err(LeaseUnavailable::OversizedRefreshReply);
                }
                RefreshCompletion::Unavailable(until) if binding.generation == origin => {
                    state.backoff = until.map(|until| (origin, until));
                    return Err(unavailable_until(until, now));
                }
                _ => {}
            }
        }
    }

    pub async fn end_enrollment(
        &self,
        key: &EnrollmentKey,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let deadline = self
            .inputs
            .clock
            .monotonic_now()
            .after(ENDING_CLEANUP_LIMIT);
        loop {
            let generation = {
                let mut state = self.state.lock().await;
                let snapshot = self.inputs.read_for_cleanup(deadline).await?;
                match snapshot {
                    StoredEnrollment::Active(binding) if &binding.key() == key => {
                        state.fence(key.clone(), why);
                        binding.generation
                    }
                    StoredEnrollment::Ended {
                        key: ended,
                        why: recorded,
                        ..
                    } if &ended == key && recorded == why => return Ok(ConditionalWrite::Applied),
                    _ => return Ok(ConditionalWrite::Superseded),
                }
            };
            match self.inputs.end_binding(generation, why, deadline).await? {
                ConditionalWrite::Applied => return Ok(ConditionalWrite::Applied),
                // A same-identity rotation may have advanced the version. Re-check within
                // the original end budget. Never apply an old ending to a new identity.
                ConditionalWrite::Superseded => {}
            }
        }
    }

    fn new_refresh(&self, binding: EnrollmentBinding, next: BindingGeneration) -> RefreshFlight {
        let inputs = Arc::clone(&self.inputs);
        let state = Arc::downgrade(&self.state);
        async move {
            let origin = binding.generation;
            let completion = inputs.rotate(binding, next, state).await;
            (origin, completion)
        }
        .boxed()
        .shared()
    }
}

fn unavailable_until(until: Option<ClockInstant>, now: ClockInstant) -> LeaseUnavailable {
    LeaseUnavailable::SignInUnreachable {
        retry_after: until.map(|until| until.saturating_duration_since(now)),
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

    async fn rotate(
        &self,
        binding: EnrollmentBinding,
        next: BindingGeneration,
        state: Weak<Mutex<RefreshState>>,
    ) -> RefreshCompletion {
        let dispatched = self.clock.monotonic_now();
        match self.source.refresh(&binding.credential).await {
            RefreshAnswer::Rotated(grant) => {
                if grant.credential.enrollment_id != binding.credential.enrollment_id {
                    return RefreshCompletion::Invalid(LeaseConfigurationFault::InvalidAnswer(
                        InvalidReason::WrongEnrollment,
                    ));
                }
                let usable_until = dispatched.after(grant.expires_in);
                self.commit_rotation(binding, next, grant, usable_until, state)
                    .await
            }
            RefreshAnswer::OversizedReply => RefreshCompletion::OversizedReply,
            RefreshAnswer::Unavailable { retry_after } => RefreshCompletion::Unavailable(
                retry_after.map(|delay| self.clock.monotonic_now().after(delay)),
            ),
            RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused,
            } => {
                self.finish_binding(binding, EnrollmentEnd::RefreshRefused, state)
                    .await
            }
            RefreshAnswer::Refused { reason } => {
                RefreshCompletion::Invalid(LeaseConfigurationFault::InvalidAnswer(reason))
            }
        }
    }

    async fn commit_rotation(
        &self,
        previous: EnrollmentBinding,
        generation: BindingGeneration,
        grant: TokenGrant,
        usable_until: ClockInstant,
        state: Weak<Mutex<RefreshState>>,
    ) -> RefreshCompletion {
        let mut replacement = previous.clone();
        replacement.generation = generation;
        replacement.credential = grant.credential;
        let deadline = self.clock.monotonic_now().after(WRITE_RETRY_LIMIT);
        loop {
            let now = self.clock.monotonic_now();
            if now >= deadline {
                return self
                    .finish_binding(previous, EnrollmentEnd::LocalCredentialLost, state)
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
                    return RefreshCompletion::Committed(AccessLease {
                        token: grant.access_token,
                        usable_until,
                        binding_generation: generation,
                    });
                }
                Ok(Ok(ConditionalWrite::Superseded)) => return RefreshCompletion::Recheck,
                _ => {
                    let retry = self
                        .clock
                        .monotonic_now()
                        .after(WRITE_RETRY_INTERVAL)
                        .min(deadline);
                    self.clock.sleep_until(retry).await;
                }
            }
        }
    }

    async fn finish_binding(
        &self,
        binding: EnrollmentBinding,
        why: EnrollmentEnd,
        state: Weak<Mutex<RefreshState>>,
    ) -> RefreshCompletion {
        let deadline = self.clock.monotonic_now().after(ENDING_CLEANUP_LIMIT);
        if let Some(state) = state.upgrade() {
            let mut state = state.lock().await;
            let key = binding.key();
            if state
                .ending
                .as_ref()
                .is_some_and(|(ending, _)| *ending == key)
            {
                return RefreshCompletion::Recheck;
            }
            match self.read_for_cleanup(deadline).await {
                Ok(StoredEnrollment::Active(current))
                    if current.generation == binding.generation && current.key() == key =>
                {
                    state.fence(key, why);
                }
                Ok(_) => return RefreshCompletion::Recheck,
                Err(_) => {
                    // Losing store access cannot withdraw a fence for a successor.
                    if state.ending.is_none() {
                        state.fence(key, why);
                    }
                    return RefreshCompletion::Ended { why, deadline };
                }
            }
        }
        match self.end_binding(binding.generation, why, deadline).await {
            Ok(ConditionalWrite::Superseded) => RefreshCompletion::Recheck,
            Ok(ConditionalWrite::Applied) | Err(_) => RefreshCompletion::Ended { why, deadline },
        }
    }

    async fn read_for_cleanup(
        &self,
        deadline: ClockInstant,
    ) -> Result<StoredEnrollment, CredentialStoreError> {
        if self.clock.monotonic_now() >= deadline {
            return Err(CredentialStoreError::CleanupTimedOut);
        }
        timeout_at(self.clock.as_ref(), deadline, self.store.read())
            .await
            .map_err(|_| CredentialStoreError::CleanupTimedOut)?
    }

    async fn end_binding(
        &self,
        generation: BindingGeneration,
        why: EnrollmentEnd,
        deadline: ClockInstant,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let removal_deadline = self
            .clock
            .monotonic_now()
            .after(ENDING_CLEANUP_LIMIT / 2)
            .min(deadline);
        if let Ok(Ok(ending)) = timeout_at(
            self.clock.as_ref(),
            removal_deadline,
            self.store.remove(generation, why),
        )
        .await
        {
            return Ok(ending);
        }

        if let Ok(marked) = timeout_at(
            self.clock.as_ref(),
            deadline,
            self.store.mark_unusable(generation, why),
        )
        .await
        {
            return marked;
        }
        // Cancellation can follow a committed marker whose acknowledgement was delayed.
        // The elapsed deadline permits an immediately ready read, with no extra wait.
        match timeout_at(self.clock.as_ref(), deadline, self.store.read()).await {
            Ok(Ok(StoredEnrollment::Ended {
                generation: saved,
                why: recorded,
                ..
            })) if Some(saved) == generation.successor() && recorded == why => {
                Ok(ConditionalWrite::Applied)
            }
            _ => Err(CredentialStoreError::CleanupTimedOut),
        }
    }
}

#[cfg(test)]
#[path = "refresh_owner_test.rs"]
mod tests;
