#![expect(
    clippy::unwrap_used,
    reason = "test fixtures and transitions must succeed"
)]

mod histories;
mod history_model;
mod transitions;

use std::{num::NonZeroU64, time::Duration};

use state_machine_core::StateMachine;
use switchboard_message_system::{
    ActiveEnrollment, Effect, EnrollmentId, EnrollmentPair, Event, Generation, Metadata, ReadId,
    StopBudgets, StopReport, StopResult, StopTarget, Supervisor, UnitPiece,
};

const FIRST: Generation = Generation(NonZeroU64::MIN);
const PIECES: [UnitPiece; 4] = [
    UnitPiece::Driver,
    UnitPiece::Custody,
    UnitPiece::Settings,
    UnitPiece::LeaseAdapter,
];
const BUDGETS: StopBudgets = StopBudgets {
    driver: Duration::from_secs(1),
    custody: Duration::from_secs(2),
    settings: Duration::from_secs(3),
    lease_adapter: Duration::from_secs(5),
    enrollment_store: Duration::from_secs(4),
};

fn pair(organization: u8, deployment: u8) -> EnrollmentPair {
    EnrollmentPair::new(
        format!("00000000-0000-7000-8000-{organization:012x}")
            .parse()
            .unwrap(),
        format!("https://signin{deployment}.example")
            .parse()
            .unwrap(),
    )
    .unwrap()
}

fn enrollment(id: u8) -> EnrollmentId {
    format!("00000000-0000-7001-8000-{id:012x}")
        .parse()
        .unwrap()
}

fn active(organization: u8, deployment: u8, id: u8) -> Metadata {
    Metadata::Active(ActiveEnrollment {
        pair: pair(organization, deployment),
        enrollment: enrollment(id),
    })
}

fn target(generation: Generation, piece: UnitPiece) -> StopTarget {
    StopTarget::Unit { generation, piece }
}

fn report(target: StopTarget, result: StopResult) -> StopReport {
    match target {
        StopTarget::EnrollmentStore => StopReport::EnrollmentStore(result),
        StopTarget::Unit { generation, piece } => match piece {
            UnitPiece::Driver => StopReport::Driver { generation, result },
            UnitPiece::Custody => StopReport::Custody { generation, result },
            UnitPiece::Settings => StopReport::Settings { generation, result },
            UnitPiece::LeaseAdapter => StopReport::LeaseAdapter {
                generation,
                result,
                unsettled_retirements: vec![],
            },
        },
    }
}

fn send(supervisor: &mut Supervisor, event: Event) -> Vec<Effect> {
    let (next, effects) = Supervisor::transition(supervisor, event).unwrap();
    *supervisor = next;
    effects
}

fn read(supervisor: &mut Supervisor, token: ReadId, metadata: Metadata) -> Vec<Effect> {
    send(
        supervisor,
        Event::MetadataObserved {
            read: token,
            metadata,
        },
    )
}

fn observe(supervisor: &mut Supervisor, metadata: Metadata) -> Vec<Effect> {
    let latest = supervisor.latest_read();
    read(supervisor, latest, metadata)
}

fn stop(supervisor: &mut Supervisor, target: StopTarget, result: StopResult) -> Vec<Effect> {
    send(supervisor, Event::StopFinished(report(target, result)))
}
