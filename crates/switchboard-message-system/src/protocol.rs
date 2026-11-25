use std::{collections::BTreeSet, num::NonZeroU64, time::Duration};

use state_machine_core::EventTag;
use switchboard_machine_enrollment::{EnrollmentEnd, EnrollmentKey};
use url::Url;
use uuid::{Uuid, Variant};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrganizationId(Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EnrollmentId(Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PairError {
    #[error("an identifier must be a canonical RFC UUIDv7")]
    InvalidId,
    #[error("the sign-in deployment must be an HTTPS origin")]
    InvalidDeployment,
}

fn identifier(text: &str) -> Result<Uuid, PairError> {
    let id = Uuid::try_parse(text).map_err(|_| PairError::InvalidId)?;
    if id.get_version_num() != 7
        || id.get_variant() != Variant::RFC4122
        || id.hyphenated().to_string() != text
    {
        return Err(PairError::InvalidId);
    }
    Ok(id)
}

impl std::str::FromStr for OrganizationId {
    type Err = PairError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        identifier(text).map(Self)
    }
}

impl std::str::FromStr for EnrollmentId {
    type Err = PairError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        identifier(text).map(Self)
    }
}

impl std::fmt::Display for EnrollmentId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::fmt::Display for OrganizationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The organization and canonical sign-in origin, with neither credentials
/// nor link discovery attached. The store/lease types live in an I/O crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentPair {
    organization: OrganizationId,
    deployment: Box<str>,
}

impl EnrollmentPair {
    pub fn new(organization: OrganizationId, deployment: Url) -> Result<Self, PairError> {
        if deployment.scheme() != "https"
            || !deployment.has_host()
            || !deployment.username().is_empty()
            || deployment.password().is_some()
            || deployment.path() != "/"
            || deployment.query().is_some()
            || deployment.fragment().is_some()
        {
            return Err(PairError::InvalidDeployment);
        }
        Ok(Self {
            organization,
            deployment: String::from(deployment).into_boxed_str(),
        })
    }

    pub const fn organization(&self) -> OrganizationId {
        self.organization
    }

    pub fn deployment(&self) -> &str {
        &self.deployment
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Generation(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReadId(pub NonZeroU64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveEnrollment {
    pub pair: EnrollmentPair,
    pub enrollment: EnrollmentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Metadata {
    Empty,
    Ended {
        key: EnrollmentKey,
        why: EnrollmentEnd,
    },
    Active(ActiveEnrollment),
    ReadFailed,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum UnitPiece {
    /// The driver owns the lane's receiving ends for the unit's life.
    Driver,
    Custody,
    Settings,
    LeaseAdapter,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StopTarget {
    Unit {
        generation: Generation,
        piece: UnitPiece,
    },
    EnrollmentStore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopResult {
    Joined,
    TimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseStopResult {
    TimedOut,
    /// The executor samples the owner's shared retirement fence at the join.
    /// Its pending entries are authoritative even if the worker returned no
    /// result.
    Joined {
        unsettled_retirements: Vec<EnrollmentId>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StopReport {
    Driver {
        generation: Generation,
        result: StopResult,
    },
    Custody {
        generation: Generation,
        result: StopResult,
    },
    Settings {
        generation: Generation,
        result: StopResult,
    },
    LeaseAdapter {
        generation: Generation,
        result: LeaseStopResult,
    },
    /// TimedOut means the enrollment-store operation remains live.
    EnrollmentStore(StopResult),
}

impl StopReport {
    pub const fn target(&self) -> StopTarget {
        let (generation, piece) = match *self {
            Self::Driver { generation, .. } => (generation, UnitPiece::Driver),
            Self::Custody { generation, .. } => (generation, UnitPiece::Custody),
            Self::Settings { generation, .. } => (generation, UnitPiece::Settings),
            Self::LeaseAdapter { generation, .. } => (generation, UnitPiece::LeaseAdapter),
            Self::EnrollmentStore(_) => return StopTarget::EnrollmentStore,
        };
        StopTarget::Unit { generation, piece }
    }

    pub const fn result(&self) -> StopResult {
        match *self {
            Self::Driver { result, .. }
            | Self::Custody { result, .. }
            | Self::Settings { result, .. }
            | Self::EnrollmentStore(result) => result,
            Self::LeaseAdapter {
                result: LeaseStopResult::TimedOut,
                ..
            } => StopResult::TimedOut,
            Self::LeaseAdapter {
                result: LeaseStopResult::Joined { .. },
                ..
            } => StopResult::Joined,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StopBudgets {
    pub driver: Duration,
    pub custody: Duration,
    pub settings: Duration,
    pub lease_adapter: Duration,
    pub enrollment_store: Duration,
}

impl StopBudgets {
    pub(crate) const fn for_target(self, target: StopTarget) -> Duration {
        match target {
            StopTarget::Unit { piece, .. } => match piece {
                UnitPiece::Driver => self.driver,
                UnitPiece::Custody => self.custody,
                UnitPiece::Settings => self.settings,
                UnitPiece::LeaseAdapter => self.lease_adapter,
            },
            StopTarget::EnrollmentStore => self.enrollment_store,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontRefusal {
    /// Composition could not start a supervisor for this front.
    CouldNotStart,
    AwaitingFirstRead,
    NotEnrolled,
    EnrollmentEnded,
    MetadataUnreadable,
    Starting,
    Restarting,
    EnrollmentMismatch,
    ShuttingDown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Front {
    Forwarding(Generation),
    Refusing(FrontRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// The executor consumes a queued notification once observation has
    /// stopped. Its `NotAllowed` result is inapplicability, not a fault.
    EnrollmentMayHaveChanged,
    MetadataObserved {
        read: ReadId,
        metadata: Metadata,
    },
    BuildFinished {
        generation: Generation,
    },
    BuildFailed {
        generation: Generation,
    },
    PieceExited {
        generation: Generation,
        piece: UnitPiece,
    },
    EnrollmentStoreExited,
    StopFinished(StopReport),
    Close,
}

impl EventTag for Event {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::EnrollmentMayHaveChanged => "EnrollmentMayHaveChanged",
            Self::MetadataObserved { .. } => "MetadataObserved",
            Self::BuildFinished { .. } => "BuildFinished",
            Self::BuildFailed { .. } => "BuildFailed",
            Self::PieceExited { .. } => "PieceExited",
            Self::EnrollmentStoreExited => "EnrollmentStoreExited",
            Self::StopFinished(_) => "StopFinished",
            Self::Close => "Close",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Fault {
    DifferentPair {
        pinned: EnrollmentPair,
        observed: EnrollmentPair,
    },
    BuildFailed {
        generation: Generation,
    },
    PieceExited {
        generation: Generation,
        piece: UnitPiece,
    },
    EnrollmentStoreExited,
    StopTimedOut {
        target: StopTarget,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    StartUnit {
        generation: Generation,
        pair: EnrollmentPair,
        /// The new settings source and lease owner must refuse these IDs
        /// before they admit an enrollment for this unit.
        retired_enrollments: BTreeSet<EnrollmentId>,
    },
    /// Custody's step joins the offer taker. Restart can lose deferred
    /// offers awaiting Central replay, checked answers/nondeparture not yet
    /// recorded, and tool requests in flight.
    StopAndJoin {
        target: StopTarget,
        budget: Duration,
    },
    PublishFront(Front),
    /// Replaces the pending metadata timer. Zero requests a read now. Only
    /// the reply to the most recently issued token can be applied.
    ScheduleMetadataRead {
        read: ReadId,
        after: Duration,
    },
    ReportFault(Fault),
    ReleaseInputs,
}
