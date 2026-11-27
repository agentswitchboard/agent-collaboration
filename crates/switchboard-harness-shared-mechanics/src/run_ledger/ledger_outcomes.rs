//! Typed answers returned by the harness run ledger.

use switchboard_harness_contracts::{DispatchResult, TransportRun, TurnId};

use super::turn_membership_machine::TurnMembershipRefusal;

#[derive(Debug, Eq, PartialEq)]
pub struct RetiredRun {
    pub run: TransportRun,
    pub session: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunOpening {
    Opened,
    AlreadyOpen,
}

#[derive(Debug, Eq, PartialEq)]
pub enum MembershipOutcome {
    Applied,
    Ended {
        run: TransportRun,
        turn: TurnId,
        session: String,
    },
    Retired(RetiredRun),
    Ignored(TurnMembershipRefusal),
    NoSuchRun,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteRefusal {
    RouteUnavailable,
    Closing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferRefusal {
    RouteUnavailable,
    Closing,
    WorkDoesNotFitAuthorization,
    OtherAuthorizationOffered,
    AuthorizationSequenceOnRecord,
}

/// Work the ledger left in the caller's custody.
#[derive(Debug, Eq, PartialEq)]
pub struct RefusedOffer<Frame> {
    pub refusal: OfferRefusal,
    pub frame: Frame,
}

impl From<RouteRefusal> for OfferRefusal {
    fn from(refusal: RouteRefusal) -> Self {
        match refusal {
            RouteRefusal::RouteUnavailable => Self::RouteUnavailable,
            RouteRefusal::Closing => Self::Closing,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoSuchOffer;

#[derive(Debug, Eq, PartialEq)]
pub enum HandOverCommit<Frame> {
    Send { run: TransportRun, frame: Frame },
    Unsent(DispatchResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadmitOutcome {
    Reopened,
    RouteLost,
    MarkerAbsent,
    DifferentMarker,
}
