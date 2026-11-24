use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use crate::SignInDeployment;

use super::*;

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value).unwrap()
}

fn key() -> EnrollmentKey {
    EnrollmentKey {
        machine_id: "019a0000-0000-7000-8000-000000000001".parse().unwrap(),
        organization_id: "019a0000-0000-7000-8000-000000000002".parse().unwrap(),
        enrollment_id: "019a0000-0000-7000-8000-000000000003".parse().unwrap(),
    }
}

fn request() -> RetirementRequest {
    RetirementRequest {
        why: EnrollmentEnd::Revoked,
        through: Some(generation(3)),
    }
}

fn ended(
    key: &EnrollmentKey,
    why: EnrollmentEnd,
    generation: BindingGeneration,
) -> EnrollmentMetadata {
    EnrollmentMetadata::Ended {
        key: key.clone(),
        why,
        generation,
    }
}

fn effect(state: RetirementState, event: RetirementEvent<'_>) -> RetirementEffect {
    let (unchanged, effects) = RetirementMachine::transition(&state, event).unwrap();
    assert_eq!(unchanged, state);
    let [effect] = effects.try_into().expect("one retirement effect");
    effect
}

#[test]
fn initial_cleanup_accepts_the_same_ended_key_with_a_different_reason() {
    let key = key();
    let observed = ended(&key, EnrollmentEnd::Inactive, generation(2));
    assert_eq!(
        effect(
            RetirementState::pending(request()),
            RetirementEvent::InitialCleanupRead {
                key: &key,
                observed: &observed
            }
        ),
        RetirementEffect::Cleanup(CleanupDecision::Settle(RetirementOutcome::Durable))
    );
}

#[test]
fn submitted_write_readback_requires_key_reason_and_generation_floor() {
    let key = key();
    let mut other = key.clone();
    other.enrollment_id = "019a0000-0000-7000-8000-000000000004".parse().unwrap();
    let submitted = generation(3);
    let pending = RetirementState::pending(request());
    for (observed, expected) in [
        (
            ended(&key, EnrollmentEnd::Revoked, generation(2)),
            EndingWriteDecision::MarkUnusable,
        ),
        (
            ended(&key, EnrollmentEnd::Inactive, generation(4)),
            EndingWriteDecision::MarkUnusable,
        ),
        (
            ended(&other, EnrollmentEnd::Revoked, generation(4)),
            EndingWriteDecision::MarkUnusable,
        ),
        (
            ended(&key, EnrollmentEnd::Revoked, generation(3)),
            EndingWriteDecision::Finished(Ok(ConditionalWrite::Applied)),
        ),
        (
            ended(&key, EnrollmentEnd::Revoked, generation(4)),
            EndingWriteDecision::Finished(Ok(ConditionalWrite::Applied)),
        ),
    ] {
        assert_eq!(
            effect(
                pending,
                RetirementEvent::SubmittedWriteReadback {
                    key: &key,
                    generation: submitted,
                    observed: &observed
                }
            ),
            RetirementEffect::EndingWrite(expected)
        );
    }
}

#[test]
fn direct_replay_requires_the_reason_but_accepts_a_lower_generation() {
    let key = key();
    let mut other = key.clone();
    other.enrollment_id = "019a0000-0000-7000-8000-000000000004".parse().unwrap();
    for (observed, expected) in [
        (
            ended(&key, EnrollmentEnd::Revoked, generation(2)),
            ConditionalWrite::Applied,
        ),
        (
            ended(&key, EnrollmentEnd::Inactive, generation(2)),
            ConditionalWrite::Superseded,
        ),
        (
            ended(&other, EnrollmentEnd::Revoked, generation(2)),
            ConditionalWrite::Superseded,
        ),
    ] {
        assert_eq!(
            effect(
                RetirementState::Unfenced,
                RetirementEvent::DirectReplayRead {
                    key: &key,
                    why: request().why,
                    observed: &observed
                }
            ),
            RetirementEffect::Replay(expected)
        );
    }
}

#[test]
fn cleanup_reports_each_write_port_according_to_its_own_reply() {
    let pending = RetirementState::pending(request());
    let definite = Some(Err(CredentialStoreError::Remove));
    for (event, expected) in [
        (
            RetirementEvent::RemoveReturned(definite),
            EndingWriteDecision::MarkUnusable,
        ),
        (
            RetirementEvent::MarkUnusableReturned(definite),
            EndingWriteDecision::Finished(Err(CredentialStoreError::Remove)),
        ),
        (
            RetirementEvent::RemoveReturned(None),
            EndingWriteDecision::ReadBack,
        ),
        (
            RetirementEvent::MarkUnusableReturned(Some(Err(CredentialStoreError::OutcomeNotKnown))),
            EndingWriteDecision::ReadBack,
        ),
        (
            RetirementEvent::MarkUnusableReturned(Some(Ok(ConditionalWrite::Superseded))),
            EndingWriteDecision::Finished(Ok(ConditionalWrite::Superseded)),
        ),
    ] {
        assert_eq!(
            effect(pending, event),
            RetirementEffect::EndingWrite(expected)
        );
    }
}

#[test]
fn retirement_table_reaches_both_stale_owner_destinations_and_checks_both_ways() {
    let key = key();
    let observed = EnrollmentMetadata::Active {
        key: key.clone(),
        deployment: SignInDeployment::new("https://signin.example/".parse().unwrap()).unwrap(),
        generation: generation(3),
    };
    let unfenced = RetirementState::Unfenced;
    let pending = RetirementState::pending(request());
    let cases = [
        (
            unfenced,
            RetirementEvent::DefinitiveRequested(EnrollmentEnd::Revoked),
        ),
        (
            unfenced,
            RetirementEvent::OwnerEndingRequested {
                why: request().why,
                through: generation(3),
            },
        ),
        (
            pending,
            RetirementEvent::DefinitiveRequested(EnrollmentEnd::Inactive),
        ),
        (
            pending,
            RetirementEvent::InitialCleanupRead {
                key: &key,
                observed: &observed,
            },
        ),
        (pending, RetirementEvent::RemoveReturned(None)),
        (pending, RetirementEvent::MarkUnusableReturned(None)),
        (
            pending,
            RetirementEvent::SubmittedWriteReadback {
                key: &key,
                observed: &observed,
                generation: generation(3),
            },
        ),
        (
            unfenced,
            RetirementEvent::DirectReplayRead {
                key: &key,
                why: request().why,
                observed: &observed,
            },
        ),
        (
            pending,
            RetirementEvent::CleanupSettled(RetirementOutcome::Durable),
        ),
        (
            pending,
            RetirementEvent::CleanupSettled(RetirementOutcome::Superseded),
        ),
        (
            pending,
            RetirementEvent::CleanupSettled(RetirementOutcome::StaleOwnerEnding),
        ),
        (
            RetirementState::StaleOwnerEnding(Some(EnrollmentEnd::Inactive)),
            RetirementEvent::ReleaseStaleOwnerEnding,
        ),
        (
            RetirementState::StaleOwnerEnding(None),
            RetirementEvent::ReleaseStaleOwnerEnding,
        ),
    ];
    check_state_machine_transitions::<RetirementMachine<'_>>(cases).unwrap();
    let apply = |state: &RetirementState, event| {
        RetirementMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    let omitted = edge("unfenced", "directReplayRead", "unfenced", false);
    let mut table = RetirementMachine::transitions().to_vec();
    table.retain(|candidate| *candidate != omitted);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Undeclared(omitted))
    );
    table.push(omitted);
    let unwitnessed = edge("pending", "cleanupSettled", "unfenced", false);
    table.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}
