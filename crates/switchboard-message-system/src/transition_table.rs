use state_machine_core::Transition;

macro_rules! row {
    ($from:literal, $event:literal, $to:literal) => {
        Transition {
            from: $from,
            event: $event,
            to: $to,
            terminal: false,
        }
    };
    ($from:literal, $event:literal, $to:literal, terminal) => {
        Transition {
            from: $from,
            event: $event,
            to: $to,
            terminal: true,
        }
    };
}

/// All successful transitions, including coalesced changes and repeated close.
pub(crate) const TRANSITIONS: &[Transition] = &[
    row!("Observing", "EnrollmentMayHaveChanged", "Observing"),
    row!("Observing", "MetadataObserved", "Observing"),
    row!("Observing", "MetadataObserved", "Opening"),
    row!("Observing", "MetadataObserved", "Faulted", terminal),
    row!("Observing", "Close", "Quiescing"),
    row!("Observing", "EnrollmentStoreExited", "Quiescing"),
    row!("Opening", "EnrollmentMayHaveChanged", "Opening"),
    row!("Opening", "MetadataObserved", "Opening"),
    row!("Opening", "MetadataObserved", "Quiescing"),
    row!("Opening", "BuildFinished", "Running"),
    row!("Opening", "BuildFailed", "Quiescing"),
    row!("Opening", "PieceExited", "Quiescing"),
    row!("Opening", "Close", "Quiescing"),
    row!("Opening", "EnrollmentStoreExited", "Quiescing"),
    row!("Running", "EnrollmentMayHaveChanged", "Running"),
    row!("Running", "MetadataObserved", "Running"),
    row!("Running", "MetadataObserved", "Quiescing"),
    row!("Running", "PieceExited", "Quiescing"),
    row!("Running", "Close", "Quiescing"),
    row!("Running", "EnrollmentStoreExited", "Quiescing"),
    row!("Quiescing", "EnrollmentMayHaveChanged", "Quiescing"),
    row!("Quiescing", "MetadataObserved", "Quiescing"),
    row!("Quiescing", "StopFinished", "Quiescing"),
    row!("Quiescing", "StopFinished", "Observing"),
    row!("Quiescing", "StopFinished", "Faulted", terminal),
    row!("Quiescing", "StopFinished", "Closed", terminal),
    row!(
        "Quiescing",
        "StopFinished",
        "ClosedWithOutstanding",
        terminal
    ),
    row!("Quiescing", "Close", "Quiescing"),
    row!("Quiescing", "EnrollmentStoreExited", "Quiescing"),
    row!("Faulted", "Close", "Quiescing"),
    row!("Faulted", "EnrollmentStoreExited", "Quiescing"),
    row!(
        "ClosedWithOutstanding",
        "StopFinished",
        "ClosedWithOutstanding",
        terminal
    ),
    row!("ClosedWithOutstanding", "StopFinished", "Closed", terminal),
    row!(
        "ClosedWithOutstanding",
        "Close",
        "ClosedWithOutstanding",
        terminal
    ),
    row!("Closed", "Close", "Closed", terminal),
];
