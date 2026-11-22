//! Correlation identities shared by the controller and a harness.

use uuid::Uuid;

/// One run of a session, from the create, load or resume the connection
/// admitted to its Closed (D1 R14). Every per-session publication carries the
/// generation it was produced under; once its run has ended, nothing more is
/// published for it.
/// Generations of one session only grow: each admitted run takes the last
/// one plus one, so a later run orders after an earlier one.
/// The session lifecycle machine assigns this generation to an admitted session run so later publications can be ordered against prior runs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionGeneration(u64);

impl SessionGeneration {
    /// What a sink that doesn't track generations hands out.
    pub const UNTRACKED: Self = Self(0);

    /// A tracking sink's numbering.
    #[must_use]
    pub const fn from_number(number: u64) -> Self {
        Self(number)
    }

    #[must_use]
    pub const fn number(self) -> u64 {
        self.0
    }
}

/// One turn's identity (a UUIDv7 minted at the handover, design note §13.1).
/// The Router assigns this correlation ID when it establishes a local identity for a turn.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TurnId(pub uuid::Uuid);

/// One load or resume attempt's identity, so a late reply from an earlier
/// attempt is recognised (a UUIDv7 minted by the caller).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LoadAttempt(pub uuid::Uuid);

/// One close attempt's identity.
/// The Router's close caller supplies this ID for one close attempt so its reply cannot settle another close.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CloseAttempt(pub uuid::Uuid);

/// The controller records whether it is restoring the session through the harness's load or resume operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreVia {
    Load,
    Resume,
}

/// The Router pairs a typed attempt with the session generation it belongs to before carrying it across the harness boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunAttempt<Attempt> {
    pub generation: SessionGeneration,
    pub attempt: Attempt,
}

/// The Host registry assigns this identity when it installs a session owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct OwnerIncarnation(pub Uuid);

/// The Host assigns this identity to the provider connection a session owner belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConnectionEpoch(pub Uuid);

/// The Host registry combines an owner incarnation with its connection epoch to address that exact owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerAddress {
    pub incarnation: OwnerIncarnation,
    pub connection_epoch: ConnectionEpoch,
}

/// The controller selects the scope of an authorized dispatch as a new turn, an exact existing turn, or the session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AuthorizationTarget {
    NewTurn,
    ExistingTurn { turn: TurnId },
    Session,
}

/// The session controller sends this change to the harness to update which turns may own callbacks in a run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnMembership {
    Provisional {
        run: SessionGeneration,
        turn: TurnId,
    },
    ObservedEligible {
        run: SessionGeneration,
        turn: TurnId,
    },
    RemoveOnRevoke {
        run: SessionGeneration,
        turn: TurnId,
    },
    End {
        run: SessionGeneration,
        turn: TurnId,
    },
    Retire(SessionGeneration),
}

/// The harness records the method and request ID to identify a reply still owed to the provider.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PendingTurnResponse {
    pub method: &'static str,
    pub request_id: String,
}

/// The ordered harness transport reports whether it offered, took, or revoked the authorized dispatch slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotEvidence {
    Offered,
    Taken,
    Revoked,
}

/// The Router and harness identify an ordered transport route by its owner incarnation and session generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TransportRun {
    pub incarnation: OwnerIncarnation,
    pub generation: SessionGeneration,
}
