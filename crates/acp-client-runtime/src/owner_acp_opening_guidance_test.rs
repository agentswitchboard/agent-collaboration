use super::*;

#[test]
fn opening_refusal_table_covers_all_writer_phase_and_attempt_combinations() {
    use AttemptRelation::*;
    use HeldOpeningKind::{Create, Restore};
    use OpeningPhase as Phase;
    use OpeningRefusal as Refusal;
    use WriterAvailability::*;

    // Columns are Same, Different, NoStoredMatch, Unnamed.
    // A vacant phase cannot have Same. A present phase cannot have NoStoredMatch:
    // no match is projected as vacant, while a competing identity is Different.
    // An unnamed call uses the vacant projection for its writer guard. A close
    // on a live writer takes its separate route/close facts.
    // Configuring and Configured are create-only storage phases.
    let rows = [
        (
            Phase::None,
            [
                Refusal::Missing,
                Refusal::AttemptMismatch,
                Refusal::Missing,
                Refusal::Missing,
            ],
        ),
        (
            Phase::Opening,
            [
                Refusal::InProgress,
                Refusal::AttemptMismatch,
                Refusal::InProgress,
                Refusal::InProgress,
            ],
        ),
        (
            Phase::Held(Create),
            [
                Refusal::AlreadyHeld(Create),
                Refusal::AttemptMismatch,
                Refusal::AlreadyHeld(Create),
                Refusal::AlreadyHeld(Create),
            ],
        ),
        (
            Phase::Held(Restore),
            [
                Refusal::AlreadyHeld(Restore),
                Refusal::AttemptMismatch,
                Refusal::AlreadyHeld(Restore),
                Refusal::AlreadyHeld(Restore),
            ],
        ),
        (
            Phase::Configuring,
            [
                Refusal::InProgress,
                Refusal::AttemptMismatch,
                Refusal::InProgress,
                Refusal::InProgress,
            ],
        ),
        (
            Phase::Configured,
            [
                Refusal::NotOffered,
                Refusal::AttemptMismatch,
                Refusal::NotOffered,
                Refusal::NotOffered,
            ],
        ),
        (
            Phase::Consumed,
            [
                Refusal::NotOffered,
                Refusal::AttemptMismatch,
                Refusal::NotOffered,
                Refusal::NotOffered,
            ],
        ),
    ];
    for (writer, override_refusal) in [
        (Live, None),
        (Stopped, Some(Refusal::WriterStopped)),
        (ConnectionRetired, Some(Refusal::ConnectionRetired)),
    ] {
        for (phase, live) in rows {
            for (caller, expected) in [Same, Different, NoStoredMatch, Unnamed]
                .into_iter()
                .zip(live)
            {
                assert_eq!(
                    classify_refusal(writer, phase, caller),
                    override_refusal.unwrap_or(expected),
                    "opening guidance for {writer:?}, {phase:?}, {caller:?}"
                );
            }
        }
    }
}
