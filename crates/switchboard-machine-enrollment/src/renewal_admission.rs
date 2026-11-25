//! Pure admission and lease publication decisions for the renewal owner.
#![deny(clippy::wildcard_enum_match_arm)]

use std::time::Duration;

use router_clock::ClockInstant;

use crate::{
    AccessRequest, BindingGeneration, EnrollmentEnd, LeaseConfigurationFault, LeaseUnavailable,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StaleGrantRetry {
    Initial,
    Available,
    Consumed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LeaseWindow {
    pub(super) generation: BindingGeneration,
    pub(super) usable_until: ClockInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlightAvailability {
    None,
    SameKey,
    OtherKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Admission {
    CachedLease,
    Join {
        retry: StaleGrantRetry,
    },
    Start {
        next: BindingGeneration,
        retry: StaleGrantRetry,
    },
    Refuse(LeaseUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LeaseAdmission {
    pub(super) generation: BindingGeneration,
    pub(super) request: AccessRequest,
    pub(super) now: ClockInstant,
    pub(super) cached: Option<LeaseWindow>,
    pub(super) flight: FlightAvailability,
    pub(super) retry: StaleGrantRetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CompletionFacts {
    Committed(LeaseWindow),
    Invalid(LeaseConfigurationFault),
    Unavailable(Option<ClockInstant>),
    Oversized,
    Recheck,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LeaseSettlement {
    pub(super) observed: BindingGeneration,
    pub(super) origin: BindingGeneration,
    pub(super) request: AccessRequest,
    pub(super) now: ClockInstant,
    pub(super) retry: StaleGrantRetry,
    pub(super) result: CompletionFacts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Settlement {
    CacheAndDeliver,
    CacheAndRetry(StaleGrantRetry),
    Retry(StaleGrantRetry),
    Refuse(LeaseUnavailable),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RenewalFacts {
    newest: BindingGeneration,
    fault: Option<(BindingGeneration, LeaseConfigurationFault)>,
    backoff: Option<(BindingGeneration, ClockInstant)>,
}

pub(super) fn admit_binding(
    ending: Option<EnrollmentEnd>,
    generation: BindingGeneration,
    request: AccessRequest,
) -> Result<(), LeaseUnavailable> {
    if let Some(why) = ending {
        return Err(LeaseUnavailable::Ended { why });
    }
    if let AccessRequest::NewerThan(rejected) = request
        && rejected > generation
    {
        return Err(LeaseConfigurationFault::FutureGeneration.into());
    }
    Ok(())
}

impl RenewalFacts {
    pub(super) fn observe(&mut self, generation: BindingGeneration) -> bool {
        if generation < self.newest {
            return false;
        }
        self.newest = generation;
        true
    }

    pub(super) fn admit(self, facts: LeaseAdmission) -> Admission {
        let LeaseAdmission {
            generation,
            request,
            now,
            cached,
            flight,
            retry,
        } = facts;
        if let Some((faulted, fault)) = self.fault
            && faulted == generation
        {
            return Admission::Refuse(fault.into());
        }
        if let Some(lease) = cached
            && lease.generation == generation
            && request.accepts(generation)
            && now < lease.usable_until
        {
            return Admission::CachedLease;
        }
        if let Some((waiting, until)) = self.backoff
            && waiting == generation
            && now < until
        {
            return Admission::Refuse(unavailable(Some(until.saturating_duration_since(now))));
        }
        let retry = match retry {
            StaleGrantRetry::Initial => StaleGrantRetry::Initial,
            StaleGrantRetry::Available => StaleGrantRetry::Consumed,
            StaleGrantRetry::Consumed => {
                return Admission::Refuse(LeaseUnavailable::StaleAccessGrant);
            }
        };
        match flight {
            FlightAvailability::SameKey => Admission::Join { retry },
            FlightAvailability::OtherKey => {
                Admission::Refuse(LeaseUnavailable::DifferentEnrollment)
            }
            FlightAvailability::None => match generation.binding_successor() {
                Some(next) => Admission::Start { next, retry },
                None => Admission::Refuse(LeaseConfigurationFault::GenerationExhausted.into()),
            },
        }
    }

    pub(super) fn settle(&mut self, facts: LeaseSettlement) -> Settlement {
        let LeaseSettlement {
            observed,
            origin,
            request,
            now,
            mut retry,
            result,
        } = facts;
        // An expired grant can spend the caller's extra attempt, even when a
        // newer binding was observed while waiting for that grant.
        if let CompletionFacts::Committed(lease) = result
            && now >= lease.usable_until
            && retry == StaleGrantRetry::Initial
        {
            retry = StaleGrantRetry::Available;
        }

        match result {
            CompletionFacts::Committed(lease)
                if observed == lease.generation && now < lease.usable_until =>
            {
                if request.accepts(lease.generation) {
                    Settlement::CacheAndDeliver
                } else {
                    Settlement::CacheAndRetry(retry)
                }
            }
            CompletionFacts::Invalid(fault) if observed == origin => {
                self.fault = Some((origin, fault));
                Settlement::Refuse(fault.into())
            }
            CompletionFacts::Oversized if observed == origin => {
                Settlement::Refuse(LeaseUnavailable::OversizedRefreshReply)
            }
            CompletionFacts::Unavailable(until) if observed == origin => {
                self.backoff = until.map(|until| (origin, until));
                Settlement::Refuse(unavailable(
                    until.map(|until| until.saturating_duration_since(now)),
                ))
            }
            CompletionFacts::Committed(_)
            | CompletionFacts::Invalid(_)
            | CompletionFacts::Oversized
            | CompletionFacts::Unavailable(_)
            | CompletionFacts::Recheck => Settlement::Retry(retry),
        }
    }
}

fn unavailable(retry_after: Option<Duration>) -> LeaseUnavailable {
    LeaseUnavailable::SignInUnreachable { retry_after }
}
