use hegel::generators as gs;
use router_clock::ClockInstant;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use crate::{
    AccessRequest, EnrollmentContext, LeaseUnavailable, RefreshCredential, SecretValue,
    SignInDeployment, TokenGrant,
    renewal_admission::{CompletionFacts, FlightAvailability, LeaseWindow, StaleGrantRetry},
};

use super::*;

#[test]
fn refused_flight_transition_logs_the_tags_and_still_rechecks() {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    struct CapturedTrace(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedTrace {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| io::Error::other("trace buffer lock poisoned"))?
                .write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let captured = CapturedTrace::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .with_writer({
            let captured = captured.clone();
            move || captured.clone()
        })
        .finish();

    let (fenced, refused) = tracing::subscriber::with_default(subscriber, || {
        (
            advance_flight(
                FlightPhase::Dispatch,
                RenewalEvent::DispatchChecked { fenced: true },
            ),
            advance_flight(FlightPhase::Dispatch, RenewalEvent::RetryElapsed),
        )
    });

    assert_eq!(fenced, FlightPhase::Finished(FlightOutcome::Recheck));
    assert_eq!(refused, FlightPhase::Finished(FlightOutcome::Recheck));
    let logged = captured.0.lock().unwrap();
    let event: serde_json::Value =
        serde_json::from_slice(&logged).expect("only the refused transition should emit an event");
    assert_eq!(event["level"], "ERROR");
    assert_eq!(event["fields"]["state"], "dispatch");
    assert_eq!(event["fields"]["event"], "retryElapsed");
}

const OLD: &str = "019a0000-0000-7000-8000-000000000002";
const OTHER: &str = "019a0000-0000-7000-8000-000000000004";

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value).unwrap()
}

fn at(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn binding(version: u64, id: &str, token: &str) -> EnrollmentBinding {
    EnrollmentBinding {
        generation: generation(version),
        context: EnrollmentContext {
            machine_id: "019a0000-0000-7000-8000-000000000001".parse().unwrap(),
            organization_id: "019a0000-0000-7000-8000-000000000005".parse().unwrap(),
            deployment: SignInDeployment::new("https://signin.example/".parse().unwrap()).unwrap(),
        },
        credential: RefreshCredential {
            enrollment_id: id.parse().unwrap(),
            refresh_token: SecretValue::new(token.to_owned()).unwrap(),
        },
    }
}

fn grant(id: &str) -> RefreshAnswer {
    RefreshAnswer::Rotated(TokenGrant {
        credential: binding(2, id, "rotated").credential,
        access_token: SecretValue::new("access".to_owned()).unwrap(),
        expires_in: Duration::from_secs(10),
    })
}

fn requested(version: u64, now: u64, retry: StaleGrantRetry) -> LeaseAdmission {
    LeaseAdmission {
        generation: generation(version),
        request: AccessRequest::Usable,
        now: at(now),
        cached: None,
        flight: FlightAvailability::None,
        retry,
    }
}

fn settled(result: CompletionFacts) -> LeaseSettlement {
    LeaseSettlement {
        observed: generation(2),
        origin: generation(2),
        request: AccessRequest::Usable,
        now: at(10),
        retry: StaleGrantRetry::Initial,
        result,
    }
}

#[test]
fn renewal_table_covers_each_edge_and_rejects_missing_and_unwitnessed_edges() {
    use FlightPhase as Phase;
    use RenewalEvent as Event;
    use RenewalState::Flight;
    let owner = RenewalState::Owner(RenewalFacts::default());
    let replacement = binding(2, OLD, "rotated");
    let prior = KeyedEnrollment::Snapshot(StoredEnrollment::Active(binding(1, OLD, "prior")));
    let reply = grant(OLD);
    let unavailable = RefreshAnswer::Unavailable { retry_after: None };
    let refused = RefreshAnswer::Refused {
        reason: InvalidReason::GrantRefused,
    };
    let expected = replacement.credential.enrollment_id;
    let read = |observed| Event::ReconciliationRead {
        observed,
        previous: generation(1),
        replacement: &replacement,
    };
    let cases = [
        (
            owner,
            Event::BindingChecked {
                ending: None,
                generation: generation(1),
                request: AccessRequest::Usable,
            },
        ),
        (
            owner,
            Event::LeaseRequested(requested(1, 1, StaleGrantRetry::Initial)),
        ),
        (
            owner,
            Event::LeaseSettled(settled(CompletionFacts::Recheck)),
        ),
        (
            Flight(Phase::Dispatch),
            Event::DispatchChecked { fenced: false },
        ),
        (
            Flight(Phase::Dispatch),
            Event::DispatchChecked { fenced: true },
        ),
        (
            Flight(Phase::Requesting),
            Event::RemoteReturned {
                expected,
                answer: &reply,
            },
        ),
        (
            Flight(Phase::Requesting),
            Event::RemoteReturned {
                expected,
                answer: &unavailable,
            },
        ),
        (
            Flight(Phase::Requesting),
            Event::RemoteReturned {
                expected,
                answer: &refused,
            },
        ),
        (
            Flight(Phase::Writing),
            Event::WriteAdmitted(WriteAdmission::Admitted),
        ),
        (
            Flight(Phase::Writing),
            Event::WriteAdmitted(WriteAdmission::FencedOrStale),
        ),
        (
            Flight(Phase::Writing),
            Event::WriteAdmitted(WriteAdmission::BudgetElapsed),
        ),
        (
            Flight(Phase::Writing),
            Event::WriteReturned(Some(Ok(ConditionalWrite::Applied))),
        ),
        (Flight(Phase::Writing), Event::WriteReturned(None)),
        (
            Flight(Phase::Writing),
            Event::WriteReturned(Some(Err(CredentialStoreError::Replace))),
        ),
        (Flight(Phase::Reconciling), read(None)),
        (Flight(Phase::Reconciling), read(Some(&prior))),
        (Flight(Phase::BackingOff), Event::RetryElapsed),
    ];
    check_state_machine_transitions::<RenewalMachine<'_>>(cases).unwrap();
    let apply = |state: &RenewalState, event| {
        RenewalMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    let omitted = edge("reconciling", "reconciliationRead", "backingOff", false);
    let mut table = RenewalMachine::transitions().to_vec();
    table.retain(|candidate| *candidate != omitted);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Undeclared(omitted))
    );
    table.push(omitted);
    let unwitnessed = edge("dispatch", "dispatchChecked", "writing", false);
    table.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

fn owner_effect(facts: RenewalFacts, event: RenewalEvent<'_>) -> (RenewalFacts, RenewalEffect) {
    let (RenewalState::Owner(after), effects) =
        RenewalMachine::transition(&RenewalState::Owner(facts), event).unwrap()
    else {
        panic!("owner events stay in owner state");
    };
    let [effect] = effects.try_into().expect("one owner decision");
    (after, effect)
}

#[hegel::test(test_cases = 40)]
fn generated_admissions_keep_cache_and_backoff_ahead_of_the_expired_grant_budget(
    case: hegel::TestCase,
) {
    use StaleGrantRetry::{Available, Consumed, Initial};
    let version = case.draw(gs::integers::<u64>().min_value(2).max_value(40));
    let now = case.draw(gs::integers::<u64>().min_value(1).max_value(40));
    let delay = case.draw(gs::integers::<u64>().min_value(1).max_value(40));
    let generation = generation(version);
    let facts = RenewalFacts::default();
    for (ending, expected) in [
        (None, LeaseConfigurationFault::FutureGeneration.into()),
        (
            Some(EnrollmentEnd::Revoked),
            LeaseUnavailable::Ended {
                why: EnrollmentEnd::Revoked,
            },
        ),
    ] {
        let (_, admission) = owner_effect(
            facts,
            RenewalEvent::BindingChecked {
                ending,
                generation,
                request: AccessRequest::NewerThan(
                    BindingGeneration::from_persisted(version + 1).unwrap(),
                ),
            },
        );
        assert_eq!(admission, RenewalEffect::BindingAdmission(Err(expected)));
    }
    let admit = |facts, request| owner_effect(facts, RenewalEvent::LeaseRequested(request)).1;
    let mut request = requested(version, now, Consumed);
    assert_eq!(
        admit(facts, request),
        RenewalEffect::Admission(Admission::Refuse(LeaseUnavailable::StaleAccessGrant))
    );
    request.cached = Some(LeaseWindow {
        generation,
        usable_until: at(now + 1),
    });
    assert_eq!(
        admit(facts, request),
        RenewalEffect::Admission(Admission::CachedLease)
    );

    let (backoff, _) = owner_effect(
        facts,
        RenewalEvent::LeaseSettled(LeaseSettlement {
            origin: generation,
            observed: generation,
            now: at(now),
            ..settled(CompletionFacts::Unavailable(Some(at(now + delay))))
        }),
    );
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::CachedLease)
    );
    request.cached = None;
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::Refuse(LeaseUnavailable::SignInUnreachable {
            retry_after: Some(Duration::from_secs(delay))
        }))
    );
    request.now = at(now + delay);
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::Refuse(LeaseUnavailable::StaleAccessGrant))
    );

    let expired = CompletionFacts::Committed(LeaseWindow {
        generation,
        usable_until: at(now),
    });
    let (_, first) = owner_effect(
        facts,
        RenewalEvent::LeaseSettled(LeaseSettlement {
            observed: generation,
            now: at(now),
            retry: Initial,
            ..settled(expired)
        }),
    );
    assert_eq!(
        first,
        RenewalEffect::Settlement(Settlement::Retry(Available))
    );
    request.retry = Available;
    request.flight = FlightAvailability::SameKey;
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::Join { retry: Consumed })
    );
    request.flight = FlightAvailability::OtherKey;
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::Refuse(LeaseUnavailable::DifferentEnrollment))
    );
    request.flight = FlightAvailability::None;
    assert_eq!(
        admit(backoff, request),
        RenewalEffect::Admission(Admission::Start {
            next: BindingGeneration::from_persisted(version + 1).unwrap(),
            retry: Consumed
        })
    );
    let (_, second) = owner_effect(
        facts,
        RenewalEvent::LeaseSettled(LeaseSettlement {
            observed: generation,
            now: at(now),
            retry: Consumed,
            ..settled(expired)
        }),
    );
    assert_eq!(
        second,
        RenewalEffect::Settlement(Settlement::Retry(Consumed))
    );
}

#[hegel::test(test_cases = 40)]
fn generated_flight_histories_require_the_exact_rotation_before_leasing(case: hegel::TestCase) {
    use FlightOutcome as Outcome;
    use FlightPhase as Phase;
    use RenewalEvent as Event;

    let version = case.draw(gs::integers::<u64>().min_value(1).max_value(40));
    let retries = case.draw(gs::integers::<u8>().max_value(5));
    let expected = binding(version, OLD, "prior").credential.enrollment_id;
    let replacement = binding(version + 1, OLD, "rotated");
    let prior = KeyedEnrollment::Snapshot(StoredEnrollment::Active(binding(version, OLD, "prior")));
    let exact = KeyedEnrollment::Snapshot(StoredEnrollment::Active(replacement.clone()));
    let other = KeyedEnrollment::Snapshot(StoredEnrollment::Active(binding(
        version + 1,
        OLD,
        "different",
    )));
    let observed = |observed| Event::ReconciliationRead {
        observed,
        previous: generation(version),
        replacement: &replacement,
    };

    assert_eq!(
        advance_flight(Phase::Dispatch, Event::DispatchChecked { fenced: true }),
        Phase::Finished(Outcome::Recheck)
    );
    let requesting = advance_flight(Phase::Dispatch, Event::DispatchChecked { fenced: false });
    let writing = advance_flight(
        requesting,
        Event::RemoteReturned {
            expected,
            answer: &grant(OLD),
        },
    );
    assert_eq!(writing, Phase::Writing);
    for (answer, phase) in [
        (
            grant(OTHER),
            Phase::Finished(Outcome::Invalid(LeaseConfigurationFault::InvalidAnswer(
                InvalidReason::WrongEnrollment,
            ))),
        ),
        (
            RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused,
            },
            Phase::Retiring(EnrollmentEnd::RefreshRefused),
        ),
        (
            RefreshAnswer::Refused {
                reason: InvalidReason::InvalidClient,
            },
            Phase::Retiring(EnrollmentEnd::RefreshClientDisabled),
        ),
        (
            RefreshAnswer::OversizedReply,
            Phase::Finished(Outcome::OversizedReply),
        ),
        (
            RefreshAnswer::Unavailable {
                retry_after: Some(Duration::from_secs(7)),
            },
            Phase::Finished(Outcome::Unavailable(Some(Duration::from_secs(7)))),
        ),
    ] {
        assert_eq!(
            advance_flight(
                requesting,
                Event::RemoteReturned {
                    expected,
                    answer: &answer
                }
            ),
            phase
        );
    }

    let mut phase = writing;
    for _ in 0..retries {
        let result = if case.draw(gs::booleans()) {
            let reading = advance_flight(phase, Event::WriteReturned(None));
            advance_flight(reading, observed(Some(&prior)))
        } else {
            advance_flight(
                phase,
                Event::WriteReturned(Some(Err(CredentialStoreError::Replace))),
            )
        };
        assert_eq!(result, Phase::BackingOff);
        phase = advance_flight(result, Event::RetryElapsed);
        assert_eq!(phase, Phase::Writing);
    }
    assert_eq!(
        advance_flight(phase, Event::WriteAdmitted(WriteAdmission::FencedOrStale)),
        Phase::Finished(Outcome::Recheck)
    );
    assert_eq!(
        advance_flight(phase, Event::WriteAdmitted(WriteAdmission::BudgetElapsed)),
        Phase::Retiring(EnrollmentEnd::LocalCredentialLost)
    );
    assert_eq!(
        advance_flight(
            phase,
            Event::WriteReturned(Some(Ok(ConditionalWrite::Superseded)))
        ),
        Phase::Finished(Outcome::Recheck)
    );
    assert_eq!(
        advance_flight(
            phase,
            Event::WriteReturned(Some(Ok(ConditionalWrite::Applied)))
        ),
        Phase::Finished(Outcome::Committed)
    );
    let reading = advance_flight(
        phase,
        Event::WriteReturned(Some(Err(CredentialStoreError::OutcomeNotKnown))),
    );
    assert_eq!(
        advance_flight(reading, observed(Some(&exact))),
        Phase::Finished(Outcome::Committed)
    );
    assert_eq!(
        advance_flight(reading, observed(Some(&other))),
        Phase::Finished(Outcome::Recheck)
    );
    assert_eq!(
        advance_flight(
            reading,
            observed(Some(&KeyedEnrollment::DifferentEnrollment))
        ),
        Phase::Finished(Outcome::Recheck)
    );
    assert_eq!(
        advance_flight(reading, observed(None)),
        Phase::Finished(Outcome::OutcomeNotKnown)
    );
}
