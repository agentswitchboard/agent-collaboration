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
    EnrollmentContext, EnrollmentEnd, EnrollmentKey, EnrollmentMetadata, InvalidReason,
    RefreshAnswer, RefreshCredential, SecretValue, StoredEnrollment, TokenGrant,
};

const WRITE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const WRITE_RETRY_LIMIT: Duration = Duration::from_secs(5);
const RECONCILIATION_RESERVE: Duration = Duration::from_secs(1);
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
    pub key: EnrollmentKey,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum StaleGrantRetry {
    Initial,
    Available,
    Consumed,
}

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
    OutcomeNotKnown,
    Recheck,
}

struct LocalEnding {
    key: EnrollmentKey,
    through_generation: Option<BindingGeneration>,
    why: EnrollmentEnd,
}

#[derive(Default)]
struct RefreshState {
    flight: Option<RefreshFlight>,
    lease: Option<AccessLease>,
    backoff: Option<(BindingGeneration, ClockInstant)>,
    endings: Vec<LocalEnding>,
    newest_observed: BindingGeneration,
    fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
}

impl RefreshState {
    fn admit(
        &self,
        binding: &EnrollmentBinding,
        request: AccessRequest,
    ) -> Result<(), LeaseUnavailable> {
        if let Some(why) = self.ending(&binding.key(), binding.generation) {
            return Err(LeaseUnavailable::Ended { why });
        }
        if let AccessRequest::NewerThan(rejected) = request
            && rejected > binding.generation
        {
            return Err(LeaseConfigurationFault::FutureGeneration.into());
        }
        Ok(())
    }

    fn ending(&self, key: &EnrollmentKey, generation: BindingGeneration) -> Option<EnrollmentEnd> {
        self.endings
            .iter()
            .find(|ending| {
                &ending.key == key
                    && ending
                        .through_generation
                        .is_none_or(|last| generation <= last)
            })
            .map(|ending| ending.why)
    }

    fn fence(&mut self, key: EnrollmentKey, why: EnrollmentEnd) {
        if !self
            .endings
            .iter()
            .any(|ending| ending.key == key && ending.through_generation.is_none())
        {
            self.endings.push(LocalEnding {
                key,
                through_generation: None,
                why,
            });
        }
    }

    fn observe(&mut self, generation: BindingGeneration) -> bool {
        if generation < self.newest_observed {
            return false;
        }
        self.newest_observed = generation;
        true
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
        let mut retry = StaleGrantRetry::Initial;
        loop {
            let flight = {
                let binding = self.inputs.read_binding().await?;
                let mut state = self.state.lock().await;
                if !state.observe(binding.generation) {
                    continue;
                }
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
                if retry == StaleGrantRetry::Consumed {
                    return Err(LeaseUnavailable::StaleAccessGrant);
                }
                if retry == StaleGrantRetry::Available {
                    retry = StaleGrantRetry::Consumed;
                }
                if let Some(flight) = &state.flight {
                    // The shared result records whether it is usable on its own generation.
                    flight.clone()
                } else {
                    let next = generation
                        .binding_successor()
                        .ok_or(LeaseConfigurationFault::GenerationExhausted)?;
                    let flight = self.new_refresh(binding, next);
                    state.flight = Some(flight.clone());
                    flight
                }
            };

            let (origin, result) = flight.clone().await;
            let (snapshot, state) = if let RefreshCompletion::Ended { why, deadline } = result {
                let snapshot = timeout_at(
                    self.inputs.clock.as_ref(),
                    deadline,
                    self.inputs.read_binding(),
                )
                .await;
                let state = self.state.lock().await;
                let snapshot = match snapshot {
                    Ok(Ok(binding)) => Ok(binding),
                    Ok(Err(error)) if !matches!(error, LeaseUnavailable::Store(_)) => Err(error),
                    _ => Err(LeaseUnavailable::Ended { why }),
                };
                (snapshot, state)
            } else {
                let snapshot = if matches!(result, RefreshCompletion::OutcomeNotKnown) {
                    Err(CredentialStoreError::OutcomeNotKnown.into())
                } else {
                    self.inputs.read_binding().await
                };
                (snapshot, self.state.lock().await)
            };
            let mut state = state;
            if state
                .flight
                .as_ref()
                .is_some_and(|active| active.ptr_eq(&flight))
            {
                state.flight = None;
            }
            let binding = snapshot?;
            if !state.observe(binding.generation) {
                continue;
            }
            state.admit(&binding, request)?;
            let now = self.inputs.clock.monotonic_now();
            if let RefreshCompletion::Committed(lease) = &result
                && now >= lease.usable_until
                && retry == StaleGrantRetry::Initial
            {
                retry = StaleGrantRetry::Available;
            }
            match result {
                RefreshCompletion::Committed(lease)
                    if binding.generation == lease.binding_generation =>
                {
                    if now < lease.usable_until {
                        state.lease = Some(lease.clone());
                        if request.accepts(lease.binding_generation) {
                            return Ok(lease);
                        }
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
        let started = self.inputs.clock.monotonic_now();
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        {
            let mut state = timeout_at(self.inputs.clock.as_ref(), deadline, self.state.lock())
                .await
                .map_err(|_| CredentialStoreError::CleanupTimedOut)?;
            state.fence(key.clone(), why);
        }
        loop {
            let generation = match self.inputs.read_for_cleanup(deadline).await? {
                EnrollmentMetadata::Active {
                    key: active,
                    generation,
                    ..
                } if &active == key => generation,
                EnrollmentMetadata::Ended {
                    key: ended,
                    why: recorded,
                    ..
                } if &ended == key && recorded == why => return Ok(ConditionalWrite::Applied),
                _ => return Ok(ConditionalWrite::Superseded),
            };
            {
                let mut state = timeout_at(self.inputs.clock.as_ref(), deadline, self.state.lock())
                    .await
                    .map_err(|_| CredentialStoreError::CleanupTimedOut)?;
                if !state.observe(generation) {
                    continue;
                }
            }
            match self
                .inputs
                .end_binding(key, generation, why, started)
                .await?
            {
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
            RefreshAnswer::Refused {
                reason: InvalidReason::InvalidClient,
            } => {
                self.finish_binding(binding, EnrollmentEnd::RefreshClientDisabled, state)
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
        let key = previous.key();
        let mut replacement = previous.clone();
        replacement.generation = generation;
        replacement.credential = grant.credential;
        let started = self.clock.monotonic_now();
        let deadline = started.after(WRITE_RETRY_LIMIT);
        let write_deadline = started.after(WRITE_RETRY_LIMIT - RECONCILIATION_RESERVE);
        loop {
            let now = self.clock.monotonic_now();
            if now >= write_deadline {
                self.clock.sleep_until(deadline).await;
                return self
                    .finish_binding(previous, EnrollmentEnd::LocalCredentialLost, state)
                    .await;
            }
            // Admit a write under the same state lock that records an ending.
            // Once admitted, the conditional store version fences the in-flight operation.
            if let Some(owner) = state.upgrade() {
                let Ok(mut owner) =
                    timeout_at(self.clock.as_ref(), write_deadline, owner.lock()).await
                else {
                    continue;
                };
                if owner.ending(&key, previous.generation).is_some()
                    || !owner.observe(previous.generation)
                {
                    return RefreshCompletion::Recheck;
                }
            }
            if self.clock.monotonic_now() >= write_deadline {
                continue;
            }
            // Retain the same pair across write failures. The old token is spent.
            let change = timeout_at(
                self.clock.as_ref(),
                write_deadline,
                self.store.replace(previous.generation, &replacement),
            )
            .await;
            let committed = match change {
                Ok(Ok(ConditionalWrite::Applied)) => true,
                Ok(Ok(ConditionalWrite::Superseded)) => return RefreshCompletion::Recheck,
                Err(_) | Ok(Err(CredentialStoreError::OutcomeNotKnown)) => {
                    let read_deadline = self
                        .clock
                        .monotonic_now()
                        .after(RECONCILIATION_RESERVE)
                        .min(deadline);
                    match timeout_at(self.clock.as_ref(), read_deadline, self.store.read()).await {
                        Ok(Ok(StoredEnrollment::Active(stored)))
                            if replacement.matches_replacement(&stored) =>
                        {
                            true
                        }
                        Ok(Ok(stored)) if stored.generation() == previous.generation => false,
                        Ok(Ok(_)) => return RefreshCompletion::Recheck,
                        _ => return RefreshCompletion::OutcomeNotKnown,
                    }
                }
                Ok(Err(_)) => false,
            };
            if committed {
                if let Some(owner) = state.upgrade() {
                    owner.lock().await.observe(generation);
                }
                return RefreshCompletion::Committed(AccessLease {
                    token: grant.access_token,
                    usable_until,
                    binding_generation: generation,
                    key,
                });
            }
            let retry = self
                .clock
                .monotonic_now()
                .after(WRITE_RETRY_INTERVAL)
                .min(write_deadline);
            self.clock.sleep_until(retry).await;
        }
    }

    async fn finish_binding(
        &self,
        binding: EnrollmentBinding,
        why: EnrollmentEnd,
        state: Weak<Mutex<RefreshState>>,
    ) -> RefreshCompletion {
        let key = binding.key();
        {
            let Some(state) = state.upgrade() else {
                return RefreshCompletion::Recheck;
            };
            let mut state = state.lock().await;
            if !state.observe(binding.generation)
                || state.ending(&key, binding.generation).is_some()
            {
                return RefreshCompletion::Recheck;
            }
            // This refusal applies to the credential presented to sign-in.
            // Another process may already have rotated the same enrollment.
            state.endings.push(LocalEnding {
                key: key.clone(),
                through_generation: Some(binding.generation),
                why,
            });
        }
        let started = self.clock.monotonic_now();
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        match self.read_for_cleanup(deadline).await {
            Ok(EnrollmentMetadata::Active {
                key: active,
                generation,
                ..
            }) if generation == binding.generation && active == key => {}
            Ok(_) => return RefreshCompletion::Recheck,
            Err(_) => return RefreshCompletion::Ended { why, deadline },
        }
        match self
            .end_binding(&key, binding.generation, why, started)
            .await
        {
            Ok(ConditionalWrite::Superseded) => RefreshCompletion::Recheck,
            Ok(ConditionalWrite::Applied) | Err(_) => RefreshCompletion::Ended { why, deadline },
        }
    }

    async fn read_for_cleanup(
        &self,
        deadline: ClockInstant,
    ) -> Result<EnrollmentMetadata, CredentialStoreError> {
        if self.clock.monotonic_now() >= deadline {
            return Err(CredentialStoreError::CleanupTimedOut);
        }
        timeout_at(self.clock.as_ref(), deadline, self.store.read_metadata())
            .await
            .map_err(|_| CredentialStoreError::CleanupTimedOut)?
    }

    async fn end_binding(
        &self,
        key: &EnrollmentKey,
        generation: BindingGeneration,
        why: EnrollmentEnd,
        started: ClockInstant,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        let write_deadline = started.after(ENDING_CLEANUP_LIMIT - RECONCILIATION_RESERVE);
        if self.clock.monotonic_now() >= write_deadline {
            return Err(CredentialStoreError::CleanupTimedOut);
        }
        let mut attempt = timeout_at(
            self.clock.as_ref(),
            write_deadline,
            self.store.remove(generation, why),
        )
        .await;
        loop {
            match attempt {
                Ok(Ok(ending)) => return Ok(ending),
                Err(_) | Ok(Err(CredentialStoreError::OutcomeNotKnown)) => {
                    let observed =
                        timeout_at(self.clock.as_ref(), deadline, self.store.read_metadata())
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .ok_or(CredentialStoreError::CleanupTimedOut)?;
                    if matches!(observed, EnrollmentMetadata::Ended {
                        generation: saved,
                        why: recorded,
                        key: ended,
                    } if saved >= generation && recorded == why && &ended == key)
                    {
                        return Ok(ConditionalWrite::Applied);
                    }
                }
                Ok(Err(_)) => {}
            }
            if self.clock.monotonic_now() >= write_deadline {
                return Err(CredentialStoreError::CleanupTimedOut);
            }
            attempt = timeout_at(
                self.clock.as_ref(),
                write_deadline,
                self.store.mark_unusable(generation, why),
            )
            .await;
            if let Ok(Err(error)) = attempt
                && error != CredentialStoreError::OutcomeNotKnown
            {
                return Err(error);
            }
        }
    }
}

#[cfg(test)]
#[path = "refresh_owner_test.rs"]
mod tests;
