//! Correlation identities shared by the controller and a harness.

use uuid::Uuid;

/// One run of a session, from the create, load or resume the connection
/// admitted to its Closed (D1 R14). Every per-session publication carries the
/// generation it was produced under; once its run has ended, nothing more is
/// published for it.
/// Generations of one session only grow: each admitted run takes the last
/// one plus one, so a later run orders after an earlier one.
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TurnId(pub uuid::Uuid);

/// One load or resume attempt's identity, so a late reply from an earlier
/// attempt is recognised (a UUIDv7 minted by the caller).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LoadAttempt(pub uuid::Uuid);

/// One close attempt's identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CloseAttempt(pub uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreVia {
    Load,
    Resume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunAttempt<Attempt> {
    pub generation: SessionGeneration,
    pub attempt: Attempt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct OwnerIncarnation(pub Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConnectionEpoch(pub Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerAddress {
    pub incarnation: OwnerIncarnation,
    pub connection_epoch: ConnectionEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AuthorizationTarget {
    NewTurn,
    ExistingTurn { turn: TurnId },
    Session,
}

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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PendingTurnResponse {
    pub method: &'static str,
    pub request_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotEvidence {
    Offered,
    Taken,
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TransportRun {
    pub incarnation: OwnerIncarnation,
    pub generation: SessionGeneration,
}
