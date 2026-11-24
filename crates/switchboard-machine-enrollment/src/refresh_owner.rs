use std::{
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use state_machine_core::StateMachine;
use tokio::sync::Mutex;

use crate::{
    AccessLease, AccessRequest, BindingGeneration, ConditionalWrite, CredentialStore,
    CredentialStoreError, EnrollmentBinding, EnrollmentContext, EnrollmentEnd, EnrollmentKey,
    EnrollmentMetadata, KeyedEnrollment, LeaseConfigurationFault, LeaseUnavailable, RefreshAnswer,
    RefreshCredential, RetirementFence, StoredEnrollment, TokenGrant,
    renewal_admission::{
        Admission, CompletionFacts, FlightAvailability, LeaseAdmission, LeaseSettlement,
        LeaseWindow, RenewalFacts, Settlement, StaleGrantRetry,
    },
    renewal_machine::{
        FlightOutcome, FlightPhase, RenewalEffect, RenewalEvent, RenewalMachine, RenewalState,
        WriteAdmission, advance_flight,
    },
    retirement_fence::RetirementAttempt,
    retirement_machine::{
        CleanupDecision, EndingWriteDecision, RetirementEffect, RetirementEvent, RetirementOutcome,
        RetirementRequest, RetirementState, read_effect,
    },
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

type LeaseResult = Result<AccessLease, LeaseUnavailable>;
type RefreshFuture = Shared<BoxFuture<'static, (BindingGeneration, RefreshCompletion)>>;

struct RefreshFlight {
    key: EnrollmentKey,
    future: RefreshFuture,
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

#[derive(Default)]
struct RefreshState {
    flight: Option<RefreshFlight>,
    lease: Option<AccessLease>,
    retirements: RetirementFence,
    facts: RenewalFacts,
}

impl RefreshState {
    fn admit(
        &self,
        binding: &EnrollmentBinding,
        request: AccessRequest,
    ) -> Result<(), LeaseUnavailable> {
        let (_, effects) = RenewalMachine::transition(
            &RenewalState::Owner(self.facts),
            RenewalEvent::BindingChecked {
                ending: self.retirements.ending(&binding.key()),
                generation: binding.generation,
                request,
            },
        )
        .map_err(|_| CredentialStoreError::Read)?;
        let [RenewalEffect::BindingAdmission(answer)] = effects.as_slice() else {
            return Err(CredentialStoreError::Read.into());
        };
        *answer
    }

    fn observe(&mut self, generation: BindingGeneration) -> bool {
        self.facts.observe(generation)
    }
}

fn lease_window(lease: &AccessLease) -> LeaseWindow {
    LeaseWindow {
        generation: lease.binding_generation,
        usable_until: lease.usable_until,
    }
}

struct RefreshInputs<Store, Source> {
    store: Store,
    source: Source,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    context: EnrollmentContext,
    retirements: RetirementFence,
    supervised_retirement: AtomicBool,
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
        let retirements = RetirementFence::default();
        Self {
            inputs: Arc::new(RefreshInputs {
                store,
                source,
                clock,
                context,
                retirements: retirements.clone(),
                supervised_retirement: AtomicBool::new(false),
            }),
            state: Arc::new(Mutex::new(RefreshState {
                retirements,
                ..RefreshState::default()
            })),
        }
    }

    /// Gives composition synchronous retirement admission without access to refresh work.
    pub fn retirement_fence(&self) -> RetirementFence {
        self.inputs.retirements.clone()
    }

    /// Transfers responsibility for attempting pending entries to the adapter supervisor.
    pub fn supervise_retirement(&self) {
        self.inputs
            .supervised_retirement
            .store(true, Ordering::Release);
    }

    /// Requests a lease for one exact enrollment key.
    pub async fn lease_for(&self, key: &EnrollmentKey, request: AccessRequest) -> LeaseResult {
        self.lease_with_key(Some(key), request).await
    }

    pub async fn lease(&self, request: AccessRequest) -> LeaseResult {
        self.lease_with_key(None, request).await
    }

    async fn lease_with_key(
        &self,
        key: Option<&EnrollmentKey>,
        request: AccessRequest,
    ) -> LeaseResult {
        let mut retry = StaleGrantRetry::Initial;
        loop {
            let flight = {
                let binding = self.inputs.read_binding(key).await?;
                let mut state = self.state.lock().await;
                if !state.observe(binding.generation) {
                    continue;
                }
                state.admit(&binding, request)?;
                let now = self.inputs.clock.monotonic_now();
                let key = binding.key();
                let flight = match &state.flight {
                    Some(flight) if flight.key == key => FlightAvailability::SameKey,
                    Some(_) => FlightAvailability::OtherKey,
                    None => FlightAvailability::None,
                };
                let (_, effects) = RenewalMachine::transition(
                    &RenewalState::Owner(state.facts),
                    RenewalEvent::LeaseRequested(LeaseAdmission {
                        generation: binding.generation,
                        request,
                        now,
                        cached: state.lease.as_ref().map(lease_window),
                        flight,
                        retry,
                    }),
                )
                .map_err(|_| CredentialStoreError::Read)?;
                let [RenewalEffect::Admission(admission)] = effects.as_slice() else {
                    return Err(CredentialStoreError::Read.into());
                };
                match *admission {
                    Admission::CachedLease => {
                        let lease = state.lease.clone().ok_or(CredentialStoreError::Read)?;
                        return self.deliver(lease);
                    }
                    Admission::Join { retry: remaining } => {
                        retry = remaining;
                        state
                            .flight
                            .as_ref()
                            .ok_or(CredentialStoreError::Read)?
                            .future
                            .clone()
                    }
                    Admission::Start {
                        next,
                        retry: remaining,
                    } => {
                        retry = remaining;
                        let flight = self.new_refresh(binding, next);
                        state.flight = Some(RefreshFlight {
                            key,
                            future: flight.clone(),
                        });
                        flight
                    }
                    Admission::Refuse(error) => return Err(error),
                }
            };

            let (origin, result) = flight.clone().await;
            let (snapshot, state) = if let RefreshCompletion::Ended { why, deadline } = result {
                let snapshot = timeout_at(
                    self.inputs.clock.as_ref(),
                    deadline,
                    self.inputs.read_binding(key),
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
                    self.inputs.read_binding(key).await
                };
                (snapshot, self.state.lock().await)
            };
            let mut state = state;
            if state
                .flight
                .as_ref()
                .is_some_and(|active| active.future.ptr_eq(&flight))
            {
                state.flight = None;
            }
            let binding = snapshot?;
            if !state.observe(binding.generation) {
                continue;
            }
            state.admit(&binding, request)?;
            let now = self.inputs.clock.monotonic_now();
            let completion = match &result {
                RefreshCompletion::Committed(lease) => {
                    CompletionFacts::Committed(lease_window(lease))
                }
                RefreshCompletion::Invalid(fault) => CompletionFacts::Invalid(*fault),
                RefreshCompletion::Unavailable(until) => CompletionFacts::Unavailable(*until),
                RefreshCompletion::OversizedReply => CompletionFacts::Oversized,
                RefreshCompletion::Ended { .. }
                | RefreshCompletion::OutcomeNotKnown
                | RefreshCompletion::Recheck => CompletionFacts::Recheck,
            };
            let (next, effects) = RenewalMachine::transition(
                &RenewalState::Owner(state.facts),
                RenewalEvent::LeaseSettled(LeaseSettlement {
                    observed: binding.generation,
                    origin,
                    request,
                    now,
                    retry,
                    result: completion,
                }),
            )
            .map_err(|_| CredentialStoreError::Read)?;
            let (RenewalState::Owner(facts), [RenewalEffect::Settlement(settlement)]) =
                (next, effects.as_slice())
            else {
                return Err(CredentialStoreError::Read.into());
            };
            state.facts = facts;
            match (*settlement, result) {
                (Settlement::CacheAndDeliver, RefreshCompletion::Committed(lease)) => {
                    state.lease = Some(lease.clone());
                    return self.deliver(lease);
                }
                (Settlement::CacheAndRetry(remaining), RefreshCompletion::Committed(lease)) => {
                    state.lease = Some(lease);
                    retry = remaining;
                }
                (Settlement::Retry(remaining), _) => retry = remaining,
                (Settlement::Refuse(error), _) => return Err(error),
                // These two effects are emitted only for a committed lease.
                (Settlement::CacheAndDeliver | Settlement::CacheAndRetry(_), _) => {
                    return Err(CredentialStoreError::Read.into());
                }
            }
        }
    }

    fn deliver(&self, lease: AccessLease) -> LeaseResult {
        if let Some(why) = self.inputs.retirements.ending(&lease.key) {
            return Err(LeaseUnavailable::Ended { why });
        }
        Ok(lease)
    }

    pub async fn end_enrollment(
        &self,
        key: &EnrollmentKey,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        self.inputs.retirements.request_definitive(key, why);
        if self.inputs.retirements.pending_attempt(key).is_some() {
            return self.finish_retirement(key).await;
        }
        let deadline = self
            .inputs
            .clock
            .monotonic_now()
            .after(ENDING_CLEANUP_LIMIT);
        let observed = self.inputs.read_for_cleanup(deadline).await?;
        // A direct replay reads the durable row independently of the fence.
        let RetirementEffect::Replay(answer) = read_effect(
            RetirementState::Unfenced,
            RetirementEvent::DirectReplayRead {
                key,
                why,
                observed: &observed,
            },
        )?
        else {
            return Err(CredentialStoreError::Read);
        };
        Ok(answer)
    }

    /// Processes the owner's previously admitted retirement for this exact key.
    pub async fn finish_retirement(
        &self,
        key: &EnrollmentKey,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        while let Some(attempt) = self.inputs.retirements.pending_attempt(key) {
            let started = self.inputs.clock.monotonic_now();
            let result = self
                .inputs
                .finish_retirement_attempt(key, &attempt, started, &self.state)
                .await?;
            if let Some(current) = self.inputs.retirements.pending_attempt(key)
                && !attempt.same_cleanup(&current)
            {
                continue;
            }
            return Ok(result);
        }
        Ok(ConditionalWrite::Superseded)
    }

    fn new_refresh(&self, binding: EnrollmentBinding, next: BindingGeneration) -> RefreshFuture {
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

impl<Store: CredentialStore, Source: RefreshSource> RefreshInputs<Store, Source> {
    async fn read_binding(
        &self,
        requested: Option<&EnrollmentKey>,
    ) -> Result<EnrollmentBinding, LeaseUnavailable> {
        let stored = match requested {
            Some(key) => match self.store.read_for_key(key).await? {
                KeyedEnrollment::Snapshot(stored) => stored,
                KeyedEnrollment::DifferentEnrollment => {
                    return Err(LeaseUnavailable::DifferentEnrollment);
                }
            },
            None => self.store.read().await?,
        };
        match stored {
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
        // An already admitted remote request can settle after retirement.
        let phase = advance_flight(
            FlightPhase::Dispatch,
            RenewalEvent::DispatchChecked {
                fenced: self.retirements.is_fenced(&binding.key()),
            },
        );
        if phase != FlightPhase::Requesting {
            return RefreshCompletion::Recheck;
        }
        let answer = self.source.refresh(&binding.credential).await;
        let phase = advance_flight(
            phase,
            RenewalEvent::RemoteReturned {
                expected: binding.credential.enrollment_id,
                answer: &answer,
            },
        );
        match (phase, answer) {
            (FlightPhase::Writing, RefreshAnswer::Rotated(grant)) => {
                let usable_until = dispatched.after(grant.expires_in);
                self.commit_rotation(binding, next, grant, usable_until, state)
                    .await
            }
            (FlightPhase::Retiring(why), _) => self.finish_binding(binding, why, state).await,
            (FlightPhase::Finished(outcome), _) => self.completed(outcome),
            _ => RefreshCompletion::Recheck,
        }
    }

    fn completed(&self, outcome: FlightOutcome) -> RefreshCompletion {
        match outcome {
            FlightOutcome::Unavailable(retry_after) => RefreshCompletion::Unavailable(
                retry_after.map(|delay| self.clock.monotonic_now().after(delay)),
            ),
            FlightOutcome::Invalid(fault) => RefreshCompletion::Invalid(fault),
            FlightOutcome::OversizedReply => RefreshCompletion::OversizedReply,
            FlightOutcome::OutcomeNotKnown => RefreshCompletion::OutcomeNotKnown,
            // The committed lease is built where the driver still owns its token.
            FlightOutcome::Committed | FlightOutcome::Recheck => RefreshCompletion::Recheck,
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
        let mut phase = FlightPhase::Writing;
        loop {
            phase = match phase {
                FlightPhase::Writing => {
                    if self.clock.monotonic_now() >= write_deadline {
                        phase = advance_flight(
                            phase,
                            RenewalEvent::WriteAdmitted(WriteAdmission::BudgetElapsed),
                        );
                        continue;
                    }
                    // Admit a write under the same lock that records an ending.
                    // SQLite still fences an operation already admitted here.
                    let admission = if let Some(owner) = state.upgrade() {
                        let Ok(mut owner) =
                            timeout_at(self.clock.as_ref(), write_deadline, owner.lock()).await
                        else {
                            continue;
                        };
                        if self.retirements.is_fenced(&key) || !owner.observe(previous.generation) {
                            WriteAdmission::FencedOrStale
                        } else {
                            WriteAdmission::Admitted
                        }
                    } else {
                        WriteAdmission::Admitted
                    };
                    phase = advance_flight(phase, RenewalEvent::WriteAdmitted(admission));
                    if phase != FlightPhase::Writing || self.clock.monotonic_now() >= write_deadline
                    {
                        continue;
                    }
                    // Retain the same pair across write failures. The old token is spent.
                    let result = timeout_at(
                        self.clock.as_ref(),
                        write_deadline,
                        self.store.replace(previous.generation, &replacement),
                    )
                    .await
                    .ok();
                    advance_flight(phase, RenewalEvent::WriteReturned(result))
                }
                FlightPhase::Reconciling => {
                    let read_deadline = self
                        .clock
                        .monotonic_now()
                        .after(RECONCILIATION_RESERVE)
                        .min(deadline);
                    let observed = timeout_at(
                        self.clock.as_ref(),
                        read_deadline,
                        self.store.read_for_key(&key),
                    )
                    .await
                    .ok()
                    .and_then(Result::ok);
                    advance_flight(
                        phase,
                        RenewalEvent::ReconciliationRead {
                            observed: observed.as_ref(),
                            previous: previous.generation,
                            replacement: &replacement,
                        },
                    )
                }
                FlightPhase::BackingOff => {
                    let retry = self
                        .clock
                        .monotonic_now()
                        .after(WRITE_RETRY_INTERVAL)
                        .min(write_deadline);
                    self.clock.sleep_until(retry).await;
                    advance_flight(phase, RenewalEvent::RetryElapsed)
                }
                FlightPhase::Retiring(why) => {
                    self.clock.sleep_until(deadline).await;
                    return self.finish_binding(previous, why, state).await;
                }
                FlightPhase::Finished(FlightOutcome::Committed) => {
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
                FlightPhase::Finished(outcome) => return self.completed(outcome),
                FlightPhase::Dispatch | FlightPhase::Requesting => {
                    return RefreshCompletion::Recheck;
                }
            };
        }
    }

    async fn finish_binding(
        &self,
        binding: EnrollmentBinding,
        why: EnrollmentEnd,
        state: Weak<Mutex<RefreshState>>,
    ) -> RefreshCompletion {
        let key = binding.key();
        let Some(state) = state.upgrade() else {
            return RefreshCompletion::Recheck;
        };
        let attempt = {
            let mut owner = state.lock().await;
            if !owner.observe(binding.generation) {
                return RefreshCompletion::Recheck;
            }
            let Some(attempt) = self
                .retirements
                .record_owner_ending(&key, binding.generation, why)
            else {
                return RefreshCompletion::Recheck;
            };
            attempt
        };

        let started = self.clock.monotonic_now();
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        if self.supervised_retirement.load(Ordering::Acquire) {
            return RefreshCompletion::Ended { why, deadline };
        }
        match self
            .finish_retirement_attempt(&key, &attempt, started, &state)
            .await
        {
            Ok(ConditionalWrite::Superseded) => RefreshCompletion::Recheck,
            Ok(ConditionalWrite::Applied) | Err(_) => RefreshCompletion::Ended { why, deadline },
        }
    }

    async fn finish_retirement_attempt(
        &self,
        key: &EnrollmentKey,
        attempt: &RetirementAttempt,
        started: ClockInstant,
        state: &Mutex<RefreshState>,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        let _cleanup = timeout_at(self.clock.as_ref(), deadline, attempt.cleanup.lock())
            .await
            .map_err(|_| CredentialStoreError::CleanupTimedOut)?;
        if !self
            .retirements
            .pending_attempt(key)
            .is_some_and(|current| current.same_cleanup(attempt))
        {
            return Ok(ConditionalWrite::Superseded);
        }
        let outcome = loop {
            let observed = self.read_for_cleanup(deadline).await?;
            let RetirementEffect::Cleanup(decision) = read_effect(
                RetirementState::pending(attempt.request),
                RetirementEvent::InitialCleanupRead {
                    key,
                    observed: &observed,
                },
            )?
            else {
                return Err(CredentialStoreError::Read);
            };
            match decision {
                CleanupDecision::Settle(outcome) => break outcome,
                CleanupDecision::Remove(generation) => {
                    let observed = timeout_at(self.clock.as_ref(), deadline, state.lock())
                        .await
                        .map_err(|_| CredentialStoreError::CleanupTimedOut)?
                        .observe(generation);
                    if observed
                        && self
                            .end_binding(key, generation, attempt.request, started)
                            .await?
                            == ConditionalWrite::Applied
                    {
                        break RetirementOutcome::Durable;
                    }
                }
            }
        };
        self.retirements.settle(key, attempt, outcome);
        Ok(match outcome {
            RetirementOutcome::Durable => ConditionalWrite::Applied,
            RetirementOutcome::Superseded | RetirementOutcome::StaleOwnerEnding => {
                ConditionalWrite::Superseded
            }
        })
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
        request: RetirementRequest,
        started: ClockInstant,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let deadline = started.after(ENDING_CLEANUP_LIMIT);
        let write_deadline = started.after(ENDING_CLEANUP_LIMIT - RECONCILIATION_RESERVE);
        if self.clock.monotonic_now() >= write_deadline {
            return Err(CredentialStoreError::CleanupTimedOut);
        }
        let pending = RetirementState::pending(request);
        let removed = timeout_at(
            self.clock.as_ref(),
            write_deadline,
            self.store.remove(generation, request.why),
        )
        .await
        .ok();
        let mut effect = read_effect(pending, RetirementEvent::RemoveReturned(removed))?;
        loop {
            let RetirementEffect::EndingWrite(decision) = effect else {
                return Err(CredentialStoreError::Read);
            };
            effect = match decision {
                EndingWriteDecision::Finished(result) => return result,
                EndingWriteDecision::ReadBack => {
                    let observed =
                        timeout_at(self.clock.as_ref(), deadline, self.store.read_metadata())
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .ok_or(CredentialStoreError::CleanupTimedOut)?;
                    read_effect(
                        pending,
                        RetirementEvent::SubmittedWriteReadback {
                            key,
                            generation,
                            observed: &observed,
                        },
                    )?
                }
                EndingWriteDecision::MarkUnusable => {
                    if self.clock.monotonic_now() >= write_deadline {
                        return Err(CredentialStoreError::CleanupTimedOut);
                    }
                    let marked = timeout_at(
                        self.clock.as_ref(),
                        write_deadline,
                        self.store.mark_unusable(generation, request.why),
                    )
                    .await
                    .ok();
                    read_effect(pending, RetirementEvent::MarkUnusableReturned(marked))?
                }
            };
        }
    }
}

#[cfg(test)]
#[path = "refresh_owner_test.rs"]
mod tests;
