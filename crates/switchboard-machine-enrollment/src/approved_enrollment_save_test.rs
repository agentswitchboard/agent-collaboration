use hegel::generators as gs;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use crate::{SecretValue, SignInDeployment};

use super::*;

const OLD: &str = "019a0000-0000-7000-8000-000000000002";
const APPROVED: &str = "019a0000-0000-7000-8000-000000000003";
const OTHER: &str = "019a0000-0000-7000-8000-000000000004";

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value).unwrap()
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

fn write(expected: u64) -> SaveState {
    SaveState::Write(SaveAttempt {
        expected: generation(expected),
        replacement: generation(expected + 1),
        knowledge: CommitKnowledge::Known,
    })
}

fn step(state: SaveState, event: SaveEvent<'_>) -> SaveState {
    ApprovedEnrollmentSaveMachine::transition(&state, event)
        .unwrap()
        .0
}

fn observed<'row>(
    current: Result<&'row StoredEnrollment, CredentialStoreError>,
    original: &'row EnrollmentKey,
    submitted: &'row EnrollmentBinding,
) -> SaveEvent<'row> {
    SaveEvent::Observed {
        current,
        original_key: Some(original),
        submitted,
    }
}

fn rejected(cause: EnrollmentSaveFailure) -> SaveState {
    SaveState::Failed(ApprovedEnrollmentSaveFailure::NotSaved(cause))
}

fn unknown(cause: EnrollmentSaveFailure) -> SaveState {
    SaveState::Failed(ApprovedEnrollmentSaveFailure::OutcomeUnknown(cause))
}

#[test]
fn declared_save_edges_are_witnessed_and_the_checker_rejects_both_bad_tables() {
    let writing = write(1);
    let pending = step(
        writing,
        SaveEvent::Wrote(Err(CredentialStoreError::OutcomeNotKnown)),
    );
    let submitted = binding(2, APPROVED, "submitted");
    let old = binding(1, OLD, "prior");
    let original = old.key();
    let unchanged = StoredEnrollment::Active(old);
    let exact = StoredEnrollment::Active(submitted.clone());
    let cases = [
        (writing, SaveEvent::Wrote(Ok(ConditionalWrite::Applied))),
        (
            writing,
            SaveEvent::Wrote(Err(CredentialStoreError::OutcomeNotKnown)),
        ),
        (
            writing,
            SaveEvent::Wrote(Err(CredentialStoreError::Replace)),
        ),
        (pending, observed(Ok(&exact), &original, &submitted)),
        (pending, observed(Ok(&unchanged), &original, &submitted)),
        (
            pending,
            observed(Err(CredentialStoreError::Read), &original, &submitted),
        ),
    ];
    check_state_machine_transitions::<ApprovedEnrollmentSaveMachine<'_>>(cases).unwrap();
    let mut table = ApprovedEnrollmentSaveMachine::transitions().to_vec();
    let omitted = edge("reconcile", "observed", "write", false);
    table.retain(|candidate| *candidate != omitted);
    let apply = |state: &SaveState, event| {
        ApprovedEnrollmentSaveMachine::transition(state, event)
            .ok()
            .map(|(next, _)| next)
    };
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Undeclared(omitted))
    );
    table.push(omitted);
    let unwitnessed = edge("write", "observed", "saved", true);
    table.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases, &table, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

#[hegel::test(test_cases = 60)]
fn generated_save_histories_preserve_retries_and_prior_commit_uncertainty(case: hegel::TestCase) {
    let base = case.draw(gs::integers::<u64>().min_value(1).max_value(40));
    let retries = case.draw(gs::integers::<u8>().max_value(8));
    let jump = case.draw(gs::integers::<u64>().min_value(2).max_value(10));
    let original = binding(base, OLD, "prior").key();
    let mut submitted = binding(base + 1, APPROVED, "submitted");
    let written = SaveEvent::Wrote(Ok(ConditionalWrite::Applied));
    let superseded = SaveEvent::Wrote(Ok(ConditionalWrite::Superseded));
    let uncertain = SaveEvent::Wrote(Err(CredentialStoreError::OutcomeNotKnown));
    let refused = SaveEvent::Wrote(Err(CredentialStoreError::Replace));
    let read_failure = observed(Err(CredentialStoreError::Read), &original, &submitted);
    let mut writing = write(base);
    assert_eq!(
        step(writing, written),
        SaveState::Saved(generation(base + 1))
    );
    assert_eq!(
        step(writing, refused),
        rejected(CredentialStoreError::Replace.into())
    );
    assert_eq!(
        step(step(writing, superseded), read_failure),
        rejected(CredentialStoreError::Read.into())
    );

    let unchanged = StoredEnrollment::Active(binding(base, OLD, "prior"));
    let same_key_advanced = StoredEnrollment::Active(binding(base + 1, OLD, "rotated"));
    let other_key = StoredEnrollment::Active(binding(base + 1, OTHER, "different"));
    let exact = StoredEnrollment::Active(submitted.clone());
    let same_generation_not_exact =
        StoredEnrollment::Active(binding(base + 1, APPROVED, "different"));
    let stale = StoredEnrollment::Active(binding(base - 1, OLD, "older"));
    let advanced_same_key = StoredEnrollment::Active(binding(base + jump, OLD, "rotated again"));
    let advanced_other_key = StoredEnrollment::Active(binding(base + jump, OTHER, "different"));
    let conflict = EnrollmentSaveFailure::ReplacementConflict;
    for _ in 0..retries {
        writing = step(
            step(writing, uncertain),
            observed(Ok(&unchanged), &original, &submitted),
        );
        assert_eq!(writing, write(base));
    }
    let pending = step(writing, uncertain);
    assert_eq!(
        step(pending, observed(Ok(&unchanged), &original, &submitted)),
        write(base)
    );
    assert_eq!(
        step(
            step(writing, superseded),
            observed(Ok(&unchanged), &original, &submitted)
        ),
        rejected(conflict)
    );
    assert_eq!(
        step(pending, observed(Ok(&exact), &original, &submitted)),
        SaveState::Saved(generation(base + 1))
    );
    assert_eq!(
        step(pending, read_failure),
        unknown(CredentialStoreError::Read.into())
    );
    assert_eq!(
        step(
            pending,
            observed(Ok(&same_key_advanced), &original, &submitted)
        ),
        write(base + 1)
    );
    assert_eq!(
        step(pending, observed(Ok(&other_key), &original, &submitted)),
        rejected(conflict)
    );
    assert_eq!(
        step(
            pending,
            observed(Ok(&same_generation_not_exact), &original, &submitted)
        ),
        rejected(conflict)
    );
    assert_eq!(
        step(pending, observed(Ok(&stale), &original, &submitted)),
        rejected(conflict)
    );
    assert_eq!(
        step(
            pending,
            observed(Ok(&StoredEnrollment::Empty), &original, &submitted)
        ),
        rejected(conflict)
    );
    assert_eq!(
        step(
            pending,
            observed(Ok(&advanced_other_key), &original, &submitted)
        ),
        unknown(conflict)
    );
    assert_eq!(
        step(
            step(writing, superseded),
            observed(Ok(&same_key_advanced), &original, &submitted)
        ),
        write(base + 1)
    );
    assert_eq!(
        step(
            step(writing, superseded),
            observed(Ok(&other_key), &original, &submitted)
        ),
        rejected(conflict)
    );

    let retry = step(
        pending,
        observed(Ok(&advanced_same_key), &original, &submitted),
    );
    assert!(matches!(retry, SaveState::Write(SaveAttempt {
        expected, replacement, knowledge: CommitKnowledge::PossiblyCommitted,
    }) if expected == generation(base + jump) && replacement == generation(base + jump + 1)));
    submitted.generation = generation(base + jump + 1);
    assert_eq!(
        step(retry, refused),
        unknown(CredentialStoreError::Replace.into())
    );
    assert_eq!(
        step(
            step(retry, superseded),
            observed(Err(CredentialStoreError::Read), &original, &submitted)
        ),
        unknown(CredentialStoreError::Read.into())
    );
    assert_eq!(step(retry, written), SaveState::Saved(submitted.generation));

    let max_active = u64::try_from(i64::MAX).unwrap() - 1;
    let generation_exhausted = StoredEnrollment::Active(binding(max_active, OLD, "last"));
    assert_eq!(
        step(
            step(write(base), superseded),
            observed(Ok(&generation_exhausted), &original, &submitted)
        ),
        rejected(EnrollmentSaveFailure::GenerationExhausted)
    );
}
